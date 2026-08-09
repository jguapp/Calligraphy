// Package api is Caligraphy's public face: the HTTP surface Booklet (or any
// client holding a token) talks to. net/http and the 1.22 ServeMux --
// there is nothing here a router framework would add except a dependency
// to explain.
//
// The one design rule: this package composes calls into store and queue;
// it owns no job semantics of its own. If a behavior can't be explained
// as "validate, then call the same store/queue methods everything else
// uses," it doesn't belong here.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"sort"
	"time"

	"github.com/jguapp/caligraphy/internal/control"
	"github.com/jguapp/caligraphy/internal/job"
	"github.com/jguapp/caligraphy/internal/metrics"
	"github.com/jguapp/caligraphy/internal/queue"
	"github.com/jguapp/caligraphy/internal/store"
)

// ControlHub is the live-worker command surface, defined here (the
// consumer) and implemented by control.Hub. Nil means no control plane --
// every admin endpoint answers 503 rather than pretending.
type ControlHub interface {
	Workers() []control.WorkerView
	Drain(workerID string) error
	Pause(workerID string) error
	Resume(workerID string) error
	SetConcurrency(workerID string, target int) error
}

type Server struct {
	Store  *store.Store
	Queue  *queue.Queue
	Log    *slog.Logger
	Meters *metrics.Metrics
	Hub    ControlHub

	// KnownTypes validates submissions against the same registry the
	// workers run, so "job accepted" implies "somebody can run it".
	KnownTypes map[string]bool
	// Tokens: SHA-256 digests of accepted bearer tokens. Comparing
	// digests makes every comparison constant-time and same-length.
	tokens [][32]byte
	// MaxPayloadBytes bounds submitted payloads (config, <= job's cap).
	MaxPayloadBytes int

	Version string
}

type Config struct {
	Tokens          []string
	KnownTypes      []string
	MaxPayloadBytes int
	Version         string
}

func NewServer(st *store.Store, q *queue.Queue, m *metrics.Metrics, cfg Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		Store: st, Queue: q, Log: log, Meters: m,
		KnownTypes:      map[string]bool{},
		MaxPayloadBytes: cfg.MaxPayloadBytes,
		Version:         cfg.Version,
	}
	for _, t := range cfg.KnownTypes {
		s.KnownTypes[t] = true
	}
	for _, t := range cfg.Tokens {
		s.tokens = append(s.tokens, sha256.Sum256([]byte(t)))
	}
	if len(s.tokens) == 0 {
		// Loud, once, at startup -- not buried per-request. Dev-only mode.
		log.Warn("api: CALIGRAPHY_API_TOKENS is empty; authentication is DISABLED")
	}
	return s
}

// Handler builds the routed, middleware-wrapped handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Open endpoints: probes and metrics. A probe cannot log in, and
	// metrics are scraped by Prometheus, not humans.
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	if s.Meters != nil {
		mux.Handle("GET /metrics", s.Meters.Handler())
	}

	// The authenticated API.
	authed := func(h http.HandlerFunc) http.Handler { return s.auth(h) }
	mux.Handle("POST /api/v1/jobs", authed(s.handleSubmit))
	mux.Handle("GET /api/v1/jobs/{id}", authed(s.handleGetJob))
	mux.Handle("GET /api/v1/jobs/{id}/result", authed(s.handleGetResult))
	mux.Handle("GET /api/v1/jobs/{id}/attempts", authed(s.handleGetAttempts))
	mux.Handle("DELETE /api/v1/jobs/{id}", authed(s.handleCancel))
	mux.Handle("GET /api/v1/queues/depths", authed(s.handleDepths))
	mux.Handle("GET /api/v1/stats/summary", authed(s.handleSummary))
	mux.Handle("GET /api/v1/dlq", authed(s.handleListDLQ))
	mux.Handle("POST /api/v1/dlq/{id}/requeue", authed(s.handleRequeueDLQ))
	mux.Handle("GET /api/v1/workers", authed(s.handleWorkers))
	mux.Handle("GET /api/v1/meta", authed(s.handleMeta))

	// The control plane's HTTP face: live-connected workers and the four
	// commands. caligraphyctl talks to these, which keeps it a plain HTTP
	// client -- the gRPC stream stays a private worker<->API affair.
	mux.Handle("GET /api/v1/control/workers", authed(s.handleControlWorkers))
	mux.Handle("POST /api/v1/control/workers/{id}/drain", authed(s.controlCmd(func(h ControlHub, id string) error { return h.Drain(id) })))
	mux.Handle("POST /api/v1/control/workers/{id}/pause", authed(s.controlCmd(func(h ControlHub, id string) error { return h.Pause(id) })))
	mux.Handle("POST /api/v1/control/workers/{id}/resume", authed(s.controlCmd(func(h ControlHub, id string) error { return h.Resume(id) })))
	mux.Handle("POST /api/v1/control/workers/{id}/concurrency", authed(s.handleSetConcurrency))

	return s.recoverMW(s.logMW(mux))
}

// ------------------------------------------------------------- middleware

func (s *Server) recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.Log.Error("api: handler panic", "path", r.URL.Path, "panic", rec)
				writeErr(w, http.StatusInternalServerError, "internal_error", "Something went wrong.")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) logMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r) // probes at 10s intervals are log spam, not signal
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		s.Log.Info("api", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "ms", time.Since(start).Milliseconds())
	})
}

func (s *Server) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.tokens) == 0 {
			next(w, r) // dev mode, warned at startup
			return
		}
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "Missing bearer token.")
			return
		}
		presented := sha256.Sum256([]byte(h[len(prefix):]))
		for _, want := range s.tokens {
			if subtle.ConstantTimeCompare(presented[:], want[:]) == 1 {
				next(w, r)
				return
			}
		}
		writeErr(w, http.StatusUnauthorized, "unauthorized", "Unknown token.")
	})
}

// ------------------------------------------------------------- submission

type submitRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Options struct {
		Queue          string       `json:"queue"`
		Priority       job.Priority `json:"priority"`
		MaxAttempts    int          `json:"maxAttempts"`
		IdempotencyKey string       `json:"idempotencyKey"`
		// Exactly one of these two, or neither.
		DelaySeconds int       `json:"delaySeconds"`
		ScheduledAt  time.Time `json:"scheduledAt"`
	} `json:"options"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	limit := int64(s.MaxPayloadBytes) + 4096 // payload plus envelope slack
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit)).Decode(&req); err != nil {
		// MaxBytesReader trips mid-decode, so an oversized body surfaces
		// here as a decode error -- distinguish it, because "your payload
		// is too big" and "your JSON is broken" call for different fixes.
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeErr(w, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("Request body exceeds %d bytes.", tooBig.Limit))
			return
		}
		writeErr(w, http.StatusBadRequest, "invalid_json", "Body must be valid JSON: "+err.Error())
		return
	}
	if !s.KnownTypes[req.Type] {
		writeErr(w, http.StatusBadRequest, "unknown_type",
			fmt.Sprintf("No handler is registered for type %q. Known types: %v", req.Type, sortedKeys(s.KnownTypes)))
		return
	}
	if len(req.Payload) > s.MaxPayloadBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "payload_too_large",
			fmt.Sprintf("Payload is %d bytes; the limit is %d.", len(req.Payload), s.MaxPayloadBytes))
		return
	}

	now := time.Now().UTC()
	opts := job.Options{
		Queue:          req.Options.Queue,
		Priority:       req.Options.Priority,
		MaxAttempts:    req.Options.MaxAttempts,
		IdempotencyKey: req.Options.IdempotencyKey,
	}
	switch {
	case req.Options.DelaySeconds > 0 && !req.Options.ScheduledAt.IsZero():
		writeErr(w, http.StatusBadRequest, "invalid_schedule", "Set delaySeconds or scheduledAt, not both.")
		return
	case req.Options.DelaySeconds > 0:
		opts.ScheduledAt = now.Add(time.Duration(req.Options.DelaySeconds) * time.Second)
	case !req.Options.ScheduledAt.IsZero():
		opts.ScheduledAt = req.Options.ScheduledAt.UTC()
	}

	j, err := job.New(req.Type, req.Payload, opts, now)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_job", err.Error())
		return
	}

	created, err := s.Store.CreateJob(r.Context(), j)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", "Could not persist the job.")
		return
	}
	if !created.Created {
		// Idempotent replay: same (type, key) -> the original job, 200.
		writeJSON(w, http.StatusOK, created.Job)
		return
	}

	// The handoff. If Redis is down RIGHT NOW the job is still accepted:
	// it is durable in Postgres and the orphan sweep will enqueue it when
	// Redis returns. That's not a best-effort excuse -- it's the designed
	// path, and it's why submission's contract is "accepted", not "queued".
	streamID, err := s.Queue.Enqueue(r.Context(), job.NewEnvelope(j, now), j.ScheduledAt)
	if err != nil {
		s.Log.Warn("api: enqueue failed; orphan sweep will repair", "job", j.ID, "err", err)
	} else if err := s.Store.SetEnqueued(r.Context(), j.ID, streamID); err != nil {
		s.Log.Warn("api: enqueue receipt failed; sweep may redeliver once", "job", j.ID, "err", err)
	}

	if s.Meters != nil {
		s.Meters.JobsSubmitted.WithLabelValues(j.Type, j.Queue).Inc()
	}
	writeJSON(w, http.StatusCreated, j)
}

// ------------------------------------------------------------------ reads

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	j, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// handleGetResult is the endpoint Booklet polls. Status semantics:
// 200 with result when COMPLETED; 200 with error detail for the two
// failure terminals (the poll succeeded -- the JOB failed; 5xx would lie
// to retry logic); 202 while there's still a future to wait for.
func (s *Server) handleGetResult(w http.ResponseWriter, r *http.Request) {
	j, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	type resultResponse struct {
		ID     string          `json:"id"`
		Status job.Status      `json:"status"`
		Result json.RawMessage `json:"result,omitempty"`
		Error  string          `json:"error,omitempty"`
	}
	resp := resultResponse{ID: j.ID, Status: j.Status, Result: j.Result, Error: j.Error}
	switch j.Status {
	case job.StatusCompleted, job.StatusFailed, job.StatusDeadLetter, job.StatusCancelled:
		writeJSON(w, http.StatusOK, resp)
	default:
		writeJSON(w, http.StatusAccepted, resp)
	}
}

func (s *Server) handleGetAttempts(w http.ResponseWriter, r *http.Request) {
	j, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	atts, err := s.Store.ListAttempts(r.Context(), j.ID)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobId": j.ID, "attempts": atts})
}

// ----------------------------------------------------------------- cancel

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	j, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	switch {
	case j.Status.Terminal():
		writeErr(w, http.StatusConflict, "already_terminal",
			fmt.Sprintf("Job is %s; there is nothing to cancel.", j.Status))
	case j.Status == job.StatusRunning:
		// Cooperative: raise the flag; the worker's next heartbeat sees it
		// and cancels the handler's context. May land after completion --
		// that race is honest and the 202 says so.
		if err := s.Queue.RequestCancel(r.Context(), j.ID, 24*time.Hour); err != nil {
			writeErr(w, http.StatusServiceUnavailable, "queue_unavailable", err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"id": j.ID, "cancellation": "requested",
			"note": "job is RUNNING; cancellation is cooperative and may lose the race with completion",
		})
	default: // PENDING or RETRYING: cancel in the store, flag for stray deliveries
		cancelled, err := s.Store.CancelJob(r.Context(), j.ID)
		if err != nil {
			writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
			return
		}
		if !cancelled {
			writeErr(w, http.StatusConflict, "race_lost", "The job changed state mid-cancel; fetch it again.")
			return
		}
		s.Queue.RequestCancel(r.Context(), j.ID, 24*time.Hour)
		writeJSON(w, http.StatusOK, map[string]any{"id": j.ID, "cancellation": "done"})
	}
}

// ------------------------------------------------------------------- ops

func (s *Server) handleDepths(w http.ResponseWriter, r *http.Request) {
	d, err := s.Queue.Depths(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "queue_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	since := time.Now().UTC().Add(-time.Hour)
	if q := r.URL.Query().Get("since"); q != "" {
		parsed, err := time.Parse(time.RFC3339, q)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_since", "since must be RFC3339.")
			return
		}
		since = parsed
	}
	sum, err := s.Store.Summary(r.Context(), since)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	limit := 50
	jobs, err := s.Store.ListDeadLetters(r.Context(), limit, 0)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleRequeueDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	req, ok, err := s.Store.RequeueDeadLetter(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	if !ok {
		writeErr(w, http.StatusConflict, "not_dead_lettered", "Job is not in the dead-letter queue.")
		return
	}
	env := job.Envelope{
		ID: req.ID, Type: req.Type, Queue: req.Queue, Priority: req.Priority,
		Attempt: 1, Payload: req.Payload, EnqueuedAt: time.Now().UTC(),
	}
	streamID, err := s.Queue.Enqueue(r.Context(), env, time.Time{})
	if err != nil {
		// Same contract as submit: the DB flip stands; the sweep delivers.
		s.Log.Warn("api: dlq requeue enqueue failed; orphan sweep will repair", "job", id, "err", err)
	} else {
		s.Store.SetEnqueued(r.Context(), id, streamID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "requeued": true})
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	ws, err := s.Store.ListWorkers(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": ws})
}

func (s *Server) handleControlWorkers(w http.ResponseWriter, r *http.Request) {
	if s.Hub == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_control_plane", "This API has no control plane attached.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": s.Hub.Workers()})
}

func (s *Server) controlCmd(f func(ControlHub, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Hub == nil {
			writeErr(w, http.StatusServiceUnavailable, "no_control_plane", "This API has no control plane attached.")
			return
		}
		id := r.PathValue("id")
		if err := f(s.Hub, id); err != nil {
			writeErr(w, http.StatusNotFound, "command_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"worker": id, "sent": true})
	}
}

func (s *Server) handleSetConcurrency(w http.ResponseWriter, r *http.Request) {
	if s.Hub == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_control_plane", "This API has no control plane attached.")
		return
	}
	var body struct {
		Target int `json:"target"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := s.Hub.SetConcurrency(r.PathValue("id"), body.Target); err != nil {
		writeErr(w, http.StatusNotFound, "command_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"worker": r.PathValue("id"), "target": body.Target})
}

// handleMeta exists for benchmark provenance: results are meaningless
// without the environment that produced them, so the bench tool asks.
func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"version":   s.Version,
		"goVersion": runtime.Version(),
		"numCPU":    runtime.NumCPU(),
		"goos":      runtime.GOOS,
		"goarch":    runtime.GOARCH,
	})
}

// ----------------------------------------------------------------- probes

// handleHealthz is liveness: process-local, dependency-free, on purpose.
// Wiring the database in here would make a Postgres blip restart the whole
// API fleet -- turning a recoverable dependency outage into an outage of
// everything (the same reasoning Booklet's /api/health documents).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is the different question: can this instance serve a real
// request right now? Both dependencies checked, bounded.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "not_ready", "database unreachable")
		return
	}
	if err := s.Queue.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "not_ready", "redis unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// ---------------------------------------------------------------- helpers

func (s *Server) loadJob(w http.ResponseWriter, r *http.Request) (*job.Job, bool) {
	j, err := s.Store.GetJob(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "No such job.")
		return nil, false
	}
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
		return nil, false
	}
	return j, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck // client gone is client gone
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": code, "message": msg})
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
