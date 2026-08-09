// Package store is Forge's Postgres layer: the durable source of truth for
// job state.
//
// The division of labor with the queue package is strict and worth stating
// once: Redis moves work, Postgres records it. Nothing in the dispatch path
// ever *reads* Postgres to find out what to run (the queue envelope is
// self-contained); Postgres is written to at the moments that matter --
// submit, claim, terminal state -- and read by humans, the API, and the
// recovery sweeps.
//
// Two ideas repeat through every write here:
//
//   - Conditional UPDATEs as arbitration. Claims match on status, terminal
//     writes match on (status, lease_epoch). "Zero rows affected" is not an
//     error -- it's the database saying someone else got there first, and
//     callers treat it as the signal it is.
//
//   - The lease epoch as a fencing token. Every ownership change bumps it;
//     every terminal write carries it. A worker that stalled past its lease
//     and lost the job to the reaper holds a stale epoch, so its late write
//     matches zero rows instead of corrupting the next owner's state.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jguapp/forge/internal/job"
)

// ErrNotFound is returned when a job id doesn't exist.
var ErrNotFound = errors.New("store: job not found")

type Store struct {
	pool *pgxpool.Pool
}

// Open connects, sizes the pool, and pings. maxConns is an explicit
// benchmark ablation lever: the baseline configuration runs with 1 to
// measure what connection pooling is actually worth.
func Open(ctx context.Context, dsn string, maxConns int) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parsing DSN: %w", err)
	}
	cfg.MaxConns = int32(maxConns)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: connecting: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// TruncateForTest wipes every table. Integration suites in other packages
// (worker, recovery, api) need a clean slate and shouldn't each hold raw
// SQL that has to track the schema.
func (s *Store) TruncateForTest(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE jobs, job_attempts, job_events, workers`)
	return err
}

// ---------------------------------------------------------------- submission

// CreateResult reports whether the submission created a new job or matched
// an existing one via its idempotency key.
type CreateResult struct {
	Job     *job.Job
	Created bool
}

// CreateJob persists a new job, honoring idempotency: if (type, key)
// already exists, the existing job is returned unchanged and Created is
// false. This is the Stripe model -- resubmission is safe and returns the
// original, so a submitter that timed out and retried cannot create
// duplicate work.
func (s *Store) CreateJob(ctx context.Context, j *job.Job) (CreateResult, error) {
	payload := "null"
	if len(j.Payload) > 0 {
		payload = string(j.Payload)
	}
	var id string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (id, type, queue, status, priority, payload,
		                  idempotency_key, max_attempts, created_at, scheduled_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7,''), $8, $9, $10, now())
		ON CONFLICT (type, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id`,
		j.ID, j.Type, j.Queue, j.Status, j.Priority, payload,
		j.IdempotencyKey, j.MaxAttempts, j.CreatedAt, j.ScheduledAt,
	).Scan(&id)

	switch {
	case err == nil:
		s.addEvent(ctx, j.ID, "submitted", map[string]any{"queue": j.Queue, "priority": j.Priority})
		return CreateResult{Job: j, Created: true}, nil
	case errors.Is(err, pgx.ErrNoRows) && j.IdempotencyKey != "":
		existing, gerr := s.getJobBy(ctx, `type = $1 AND idempotency_key = $2`, j.Type, j.IdempotencyKey)
		if gerr != nil {
			return CreateResult{}, fmt.Errorf("store: idempotent lookup after conflict: %w", gerr)
		}
		return CreateResult{Job: existing, Created: false}, nil
	default:
		return CreateResult{}, fmt.Errorf("store: creating job: %w", err)
	}
}

// SetEnqueued records the Redis handoff. Until this lands, the job is a
// candidate for the orphan sweep -- which is exactly the recovery path for
// "INSERT succeeded, XADD didn't".
func (s *Store) SetEnqueued(ctx context.Context, id, streamID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET enqueued_stream_id = $2, updated_at = now()
		WHERE id = $1 AND enqueued_stream_id IS NULL`, id, streamID)
	if err != nil {
		return fmt.Errorf("store: marking enqueued: %w", err)
	}
	return nil
}

// ------------------------------------------------------------------ dispatch

// Claim is what a successful claim hands the runner: the fencing epoch it
// must present on every subsequent write, and the authoritative attempt
// coordinates.
type Claim struct {
	Epoch       int64
	Attempt     int
	MaxAttempts int
	StartedAt   time.Time
}

// ClaimJob is the arbitration point for at-least-once delivery. Any number
// of stream entries may exist for one job (redeliveries, sweep repairs);
// whichever worker's claim UPDATE matches first wins, and every other
// delivery sees ok=false and simply acks the entry away.
//
// attempt_count increments here -- at claim, not at completion -- so a
// worker that dies mid-attempt has still consumed an attempt. The
// alternative (increment on failure report) undercounts crashes, which are
// precisely the failures retry budgets exist for.
func (s *Store) ClaimJob(ctx context.Context, id, workerID string, leaseTTL time.Duration) (Claim, bool, error) {
	var c Claim
	err := s.pool.QueryRow(ctx, `
		UPDATE jobs SET
			status = 'RUNNING',
			worker_id = $2,
			lease_epoch = lease_epoch + 1,
			lease_expires_at = now() + make_interval(secs => $3),
			started_at = COALESCE(started_at, now()),
			attempt_count = attempt_count + 1,
			updated_at = now()
		WHERE id = $1 AND status IN ('PENDING','RETRYING')
		RETURNING lease_epoch, attempt_count, max_attempts, now()`,
		id, workerID, leaseTTL.Seconds(),
	).Scan(&c.Epoch, &c.Attempt, &c.MaxAttempts, &c.StartedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, fmt.Errorf("store: claiming job: %w", err)
	}
	return c, true, nil
}

// ExtendLease renews a running job's lease. ok=false means the caller no
// longer owns the job (reaped, cancelled) and should stop working on it.
func (s *Store) ExtendLease(ctx context.Context, id string, epoch int64, ttl time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1 AND lease_epoch = $2 AND status = 'RUNNING'`,
		id, epoch, ttl.Seconds())
	if err != nil {
		return false, fmt.Errorf("store: extending lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ------------------------------------------------------- terminal (fenced)

// Each of these matches on (id, epoch, status='RUNNING'). ok=false is the
// fence doing its job: the write was stale and nothing happened.

func (s *Store) CompleteJob(ctx context.Context, id string, epoch int64, result json.RawMessage) (bool, error) {
	res := "null"
	if len(result) > 0 {
		res = string(result)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'COMPLETED', result = $3, error = NULL,
			completed_at = now(), lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_epoch = $2 AND status = 'RUNNING'`,
		id, epoch, res)
	if err != nil {
		return false, fmt.Errorf("store: completing job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) FailJob(ctx context.Context, id string, epoch int64, errMsg string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'FAILED', error = $3,
			completed_at = now(), lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_epoch = $2 AND status = 'RUNNING'`,
		id, epoch, errMsg)
	if err != nil {
		return false, fmt.Errorf("store: failing job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) DeadLetterJob(ctx context.Context, id string, epoch int64, errMsg string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'DEAD_LETTER', error = $3,
			completed_at = now(), lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_epoch = $2 AND status = 'RUNNING'`,
		id, epoch, errMsg)
	if err != nil {
		return false, fmt.Errorf("store: dead-lettering job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		s.addEvent(ctx, id, "dead_lettered", map[string]any{"error": errMsg})
		return true, nil
	}
	return false, nil
}

func (s *Store) CancelRunning(ctx context.Context, id string, epoch int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'CANCELLED',
			completed_at = now(), lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_epoch = $2 AND status = 'RUNNING'`,
		id, epoch)
	if err != nil {
		return false, fmt.Errorf("store: cancelling running job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		s.addEvent(ctx, id, "cancelled", nil)
		return true, nil
	}
	return false, nil
}

// ScheduleRetry parks a failed job as RETRYING until nextAt. The actual
// re-delivery is the promoter's job (ZSET -> stream); if that handoff is
// lost, the stuck-retry sweep repairs it from this row.
func (s *Store) ScheduleRetry(ctx context.Context, id string, epoch int64, errMsg string, nextAt time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'RETRYING', error = $3, scheduled_at = $4,
			lease_expires_at = NULL, worker_id = NULL, updated_at = now()
		WHERE id = $1 AND lease_epoch = $2 AND status = 'RUNNING'`,
		id, epoch, errMsg, nextAt)
	if err != nil {
		return false, fmt.Errorf("store: scheduling retry: %w", err)
	}
	if tag.RowsAffected() == 1 {
		s.addEvent(ctx, id, "retry_scheduled", map[string]any{"nextAt": nextAt, "error": errMsg})
		return true, nil
	}
	return false, nil
}

// --------------------------------------------------------------- recording

// Attempt is one execution's record. Kept forever: "what happened to job X"
// should be answerable from SQL, not reconstructed from logs.
type Attempt struct {
	JobID      string      `json:"jobId"`
	Attempt    int         `json:"attempt"`
	WorkerID   string      `json:"workerId"`
	StartedAt  time.Time   `json:"startedAt"`
	FinishedAt time.Time   `json:"finishedAt"`
	Outcome    job.Outcome `json:"outcome"`
	Error      string      `json:"error,omitempty"`
}

func (s *Store) RecordAttempt(ctx context.Context, a Attempt) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
		VALUES ($1, $2, NULLIF($3,''), $4, $5, $6, NULLIF($7,''))
		ON CONFLICT (job_id, attempt) DO NOTHING`,
		a.JobID, a.Attempt, a.WorkerID, a.StartedAt, a.FinishedAt, a.Outcome, a.Error)
	if err != nil {
		return fmt.Errorf("store: recording attempt: %w", err)
	}
	return nil
}

func (s *Store) ListAttempts(ctx context.Context, jobID string) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, attempt, COALESCE(worker_id,''), started_at,
		       COALESCE(finished_at, started_at), outcome, COALESCE(error,'')
		FROM job_attempts WHERE job_id = $1 ORDER BY attempt`, jobID)
	if err != nil {
		return nil, fmt.Errorf("store: listing attempts: %w", err)
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.JobID, &a.Attempt, &a.WorkerID, &a.StartedAt,
			&a.FinishedAt, &a.Outcome, &a.Error); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// addEvent is best-effort by design: an event is history, and history must
// never make the present fail. Errors are dropped (not even logged here --
// the caller's own operation succeeded, and a Postgres that's failing event
// inserts is already failing louder writes elsewhere).
func (s *Store) addEvent(ctx context.Context, jobID, event string, detail map[string]any) {
	var d any
	if detail != nil {
		if b, err := json.Marshal(detail); err == nil {
			d = string(b)
		}
	}
	_, _ = s.pool.Exec(ctx,
		`INSERT INTO job_events (job_id, event, detail) VALUES ($1, $2, $3)`,
		jobID, event, d)
}

// AddEvent is the exported form for other packages (recovery, api).
func (s *Store) AddEvent(ctx context.Context, jobID, event string, detail map[string]any) {
	s.addEvent(ctx, jobID, event, detail)
}

// ------------------------------------------------------------------- reads

const jobColumns = `id, type, queue, status, priority, payload, result,
	COALESCE(error,''), COALESCE(idempotency_key,''), attempt_count, max_attempts,
	lease_epoch, lease_expires_at, COALESCE(worker_id,''),
	created_at, scheduled_at, started_at, completed_at`

func scanJob(row pgx.Row) (*job.Job, error) {
	var j job.Job
	var payload, result []byte
	err := row.Scan(&j.ID, &j.Type, &j.Queue, &j.Status, &j.Priority, &payload, &result,
		&j.Error, &j.IdempotencyKey, &j.AttemptCount, &j.MaxAttempts,
		&j.LeaseEpoch, &j.LeaseExpiresAt, &j.WorkerID,
		&j.CreatedAt, &j.ScheduledAt, &j.StartedAt, &j.CompletedAt)
	if err != nil {
		return nil, err
	}
	if string(payload) != "null" {
		j.Payload = payload
	}
	if len(result) > 0 && string(result) != "null" {
		j.Result = result
	}
	return &j, nil
}

func (s *Store) getJobBy(ctx context.Context, where string, args ...any) (*job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE `+where, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

func (s *Store) GetJob(ctx context.Context, id string) (*job.Job, error) {
	return s.getJobBy(ctx, `id = $1`, id)
}

// CancelJob cancels a job that hasn't started (PENDING or RETRYING). For
// RUNNING jobs cancellation is cooperative and goes through the queue's
// cancel flag instead; the API composes the two.
func (s *Store) CancelJob(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'CANCELLED', completed_at = now(),
			lease_epoch = lease_epoch + 1, updated_at = now()
		WHERE id = $1 AND status IN ('PENDING','RETRYING')`, id)
	if err != nil {
		return false, fmt.Errorf("store: cancelling job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		s.addEvent(ctx, id, "cancelled", nil)
		return true, nil
	}
	return false, nil
}

// ---------------------------------------------------------------- recovery

// ReapResult reports what the reaper's conditional UPDATE decided.
type ReapResult struct {
	Reaped      bool
	NewStatus   job.Status
	Attempt     int
	MaxAttempts int
	PrevWorker  string
}

// ReapJob handles a RUNNING job whose lease is expired: bump the epoch
// (fencing any zombie still holding the old one), then either schedule a
// retry or dead-letter it, atomically, in one statement. graceSecs > 0 is
// used by the DB-side sweep to demand the lease be *long* expired; the
// Redis-side reaper passes 0 because the PEL idle time already proved
// abandonment.
//
// Reaped=false means the job wasn't actually an expired RUNNING row by the
// time we got here -- the worker finished (or a competing reaper won). Not
// an error; the caller just acks and moves on.
func (s *Store) ReapJob(ctx context.Context, id string, retryDelay time.Duration, graceSecs float64) (ReapResult, error) {
	var r ReapResult
	err := s.pool.QueryRow(ctx, `
		WITH prev AS (SELECT worker_id FROM jobs WHERE id = $1)
		UPDATE jobs j SET
			lease_epoch = lease_epoch + 1,
			status = CASE WHEN j.attempt_count >= j.max_attempts THEN 'DEAD_LETTER' ELSE 'RETRYING' END,
			error = 'lease expired: worker presumed dead',
			scheduled_at = CASE WHEN j.attempt_count >= j.max_attempts
				THEN j.scheduled_at ELSE now() + make_interval(secs => $2) END,
			completed_at = CASE WHEN j.attempt_count >= j.max_attempts THEN now() ELSE j.completed_at END,
			lease_expires_at = NULL,
			worker_id = NULL,
			updated_at = now()
		FROM prev
		WHERE j.id = $1 AND j.status = 'RUNNING'
			AND j.lease_expires_at < now() - make_interval(secs => $3)
		RETURNING j.status, j.attempt_count, j.max_attempts, COALESCE(prev.worker_id,'')`,
		id, retryDelay.Seconds(), graceSecs,
	).Scan(&r.NewStatus, &r.Attempt, &r.MaxAttempts, &r.PrevWorker)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReapResult{Reaped: false}, nil
	}
	if err != nil {
		return ReapResult{}, fmt.Errorf("store: reaping job: %w", err)
	}
	r.Reaped = true
	s.addEvent(ctx, id, "lease_expired", map[string]any{"prevWorker": r.PrevWorker, "newStatus": r.NewStatus})
	return r, nil
}

// ListExpiredRunning finds RUNNING rows whose lease expired more than
// grace ago -- the DB-side backstop for jobs that vanished from the Redis
// PEL (acked but never recorded, e.g. a crash between flush and record).
func (s *Store) ListExpiredRunning(ctx context.Context, grace time.Duration, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM jobs
		WHERE status = 'RUNNING' AND lease_expires_at < now() - make_interval(secs => $1)
		LIMIT $2`, grace.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: listing expired running: %w", err)
	}
	defer rows.Close()
	return scanIDs(rows)
}

// Requeueable is the minimal shape needed to rebuild an Envelope for a job
// the sweeps are putting back on the wire.
type Requeueable struct {
	ID       string
	Type     string
	Queue    string
	Priority job.Priority
	Payload  json.RawMessage
	Attempt  int // attempts already consumed; next delivery is Attempt+1
}

const requeueColumns = `id, type, queue, priority, payload, attempt_count`

func scanRequeueables(rows pgx.Rows) ([]Requeueable, error) {
	defer rows.Close()
	var out []Requeueable
	for rows.Next() {
		var r Requeueable
		var payload []byte
		if err := rows.Scan(&r.ID, &r.Type, &r.Queue, &r.Priority, &payload, &r.Attempt); err != nil {
			return nil, err
		}
		if string(payload) != "null" {
			r.Payload = payload
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListUnenqueued finds PENDING jobs that are due but were never confirmably
// handed to Redis -- the "INSERT succeeded, XADD didn't" repair path.
func (s *Store) ListUnenqueued(ctx context.Context, olderThan time.Duration, limit int) ([]Requeueable, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+requeueColumns+` FROM jobs
		WHERE status = 'PENDING' AND enqueued_stream_id IS NULL
			AND scheduled_at <= now()
			AND created_at < now() - make_interval(secs => $1)
		LIMIT $2`, olderThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: listing unenqueued: %w", err)
	}
	return scanRequeueables(rows)
}

// ListStuckRetrying finds RETRYING jobs whose promotion is overdue -- the
// "DB says retry scheduled, ZADD never landed (or promoter output was
// lost)" repair path.
func (s *Store) ListStuckRetrying(ctx context.Context, olderThan time.Duration, limit int) ([]Requeueable, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+requeueColumns+` FROM jobs
		WHERE status = 'RETRYING' AND scheduled_at < now() - make_interval(secs => $1)
		LIMIT $2`, olderThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: listing stuck retrying: %w", err)
	}
	return scanRequeueables(rows)
}

// MarkPromoted flips RETRYING -> PENDING for jobs the promoter just moved
// back onto a stream. Batch, because the promoter works in batches.
func (s *Store) MarkPromoted(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'PENDING', updated_at = now()
		WHERE id = ANY($1) AND status = 'RETRYING'`, ids)
	if err != nil {
		return 0, fmt.Errorf("store: marking promoted: %w", err)
	}
	return tag.RowsAffected(), nil
}

// --------------------------------------------------------------------- DLQ

func (s *Store) ListDeadLetters(ctx context.Context, limit, offset int) ([]*job.Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColumns+` FROM jobs WHERE status = 'DEAD_LETTER'
		ORDER BY completed_at DESC NULLS LAST LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("store: listing dead letters: %w", err)
	}
	defer rows.Close()
	var out []*job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// RequeueDeadLetter resets a DLQ'd job for a fresh run: attempts back to
// zero, error cleared, epoch bumped (fencing anything stale), and the
// Redis handoff marker cleared so the orphan sweep would repair even a
// requeue whose XADD fails.
func (s *Store) RequeueDeadLetter(ctx context.Context, id string) (Requeueable, bool, error) {
	var r Requeueable
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		UPDATE jobs SET status = 'PENDING', error = NULL, result = NULL,
			attempt_count = 0, lease_epoch = lease_epoch + 1,
			scheduled_at = now(), completed_at = NULL, started_at = NULL,
			enqueued_stream_id = NULL, updated_at = now()
		WHERE id = $1 AND status = 'DEAD_LETTER'
		RETURNING `+requeueColumns,
		id,
	).Scan(&r.ID, &r.Type, &r.Queue, &r.Priority, &payload, &r.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Requeueable{}, false, nil
	}
	if err != nil {
		return Requeueable{}, false, fmt.Errorf("store: requeueing dead letter: %w", err)
	}
	if string(payload) != "null" {
		r.Payload = payload
	}
	s.addEvent(ctx, id, "requeued", nil)
	return r, true, nil
}

// ----------------------------------------------------------------- workers

type WorkerInfo struct {
	ID                string    `json:"id"`
	Hostname          string    `json:"hostname"`
	PID               int       `json:"pid"`
	Concurrency       int       `json:"concurrency"`
	TargetConcurrency int       `json:"targetConcurrency"`
	ActiveJobs        int       `json:"activeJobs"`
	Processed         int64     `json:"processed"`
	State             string    `json:"state"`
	StartedAt         time.Time `json:"startedAt"`
	LastHeartbeatAt   time.Time `json:"lastHeartbeatAt"`
}

func (s *Store) UpsertWorker(ctx context.Context, w WorkerInfo) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (id, hostname, pid, concurrency, target_concurrency,
			active_jobs, processed, state, started_at, last_heartbeat_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
		ON CONFLICT (id) DO UPDATE SET
			concurrency = EXCLUDED.concurrency,
			target_concurrency = EXCLUDED.target_concurrency,
			active_jobs = EXCLUDED.active_jobs,
			processed = EXCLUDED.processed,
			state = EXCLUDED.state,
			last_heartbeat_at = now()`,
		w.ID, w.Hostname, w.PID, w.Concurrency, w.TargetConcurrency,
		w.ActiveJobs, w.Processed, w.State)
	if err != nil {
		return fmt.Errorf("store: upserting worker: %w", err)
	}
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, COALESCE(hostname,''), COALESCE(pid,0), concurrency, target_concurrency,
			active_jobs, processed, state, started_at, last_heartbeat_at
		FROM workers ORDER BY started_at`)
	if err != nil {
		return nil, fmt.Errorf("store: listing workers: %w", err)
	}
	defer rows.Close()
	var out []WorkerInfo
	for rows.Next() {
		var w WorkerInfo
		if err := rows.Scan(&w.ID, &w.Hostname, &w.PID, &w.Concurrency, &w.TargetConcurrency,
			&w.ActiveJobs, &w.Processed, &w.State, &w.StartedAt, &w.LastHeartbeatAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// MarkDeadWorkers flips workers whose heartbeat is older than cutoff to
// 'gone'. Bookkeeping only -- job recovery never depends on this table,
// only on leases -- but "which workers are alive" should have one answer.
func (s *Store) MarkDeadWorkers(ctx context.Context, cutoff time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE workers SET state = 'gone'
		WHERE last_heartbeat_at < now() - make_interval(secs => $1) AND state <> 'gone'`,
		cutoff.Seconds())
	if err != nil {
		return 0, fmt.Errorf("store: marking dead workers: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ------------------------------------------------------------------- stats

type LatencyPercentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// Summary is the benchmark's (and dashboard's) one-call view of a run.
type Summary struct {
	Since         time.Time            `json:"since"`
	Counts        map[job.Status]int64 `json:"counts"`
	Total         int64                `json:"total"`
	RetriedJobs   int64                `json:"retriedJobs"`   // jobs that needed >1 attempt
	TotalAttempts int64                `json:"totalAttempts"` // sum of attempt_count
	E2ESeconds    LatencyPercentiles   `json:"e2eSeconds"`    // created -> completed (includes queue wait)
	ExecSeconds   LatencyPercentiles   `json:"execSeconds"`   // started -> completed
}

// Summary aggregates jobs created since `since`. Percentiles are exact
// (percentile_cont over the real distribution), computed in SQL -- the
// database already has the data sorted-ish and indexed; shipping tens of
// thousands of rows to Go to sort them again would be motion without work.
func (s *Store) Summary(ctx context.Context, since time.Time) (Summary, error) {
	out := Summary{Since: since, Counts: map[job.Status]int64{}}

	rows, err := s.pool.Query(ctx, `
		SELECT status, count(*) FROM jobs WHERE created_at >= $1 GROUP BY status`, since)
	if err != nil {
		return out, fmt.Errorf("store: summary counts: %w", err)
	}
	for rows.Next() {
		var st job.Status
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return out, err
		}
		out.Counts[st] = n
		out.Total += n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}

	err = s.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE attempt_count > 1), COALESCE(sum(attempt_count),0)
		FROM jobs WHERE created_at >= $1`, since,
	).Scan(&out.RetriedJobs, &out.TotalAttempts)
	if err != nil {
		return out, fmt.Errorf("store: summary attempts: %w", err)
	}

	var e2e, exec []float64
	err = s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP
				(ORDER BY extract(epoch FROM (completed_at - created_at))), ARRAY[0,0,0]),
			COALESCE(percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP
				(ORDER BY extract(epoch FROM (completed_at - started_at))), ARRAY[0,0,0])
		FROM jobs
		WHERE created_at >= $1 AND status = 'COMPLETED' AND completed_at IS NOT NULL`, since,
	).Scan(&e2e, &exec)
	if err != nil {
		return out, fmt.Errorf("store: summary percentiles: %w", err)
	}
	if len(e2e) == 3 {
		out.E2ESeconds = LatencyPercentiles{P50: e2e[0], P95: e2e[1], P99: e2e[2]}
	}
	if len(exec) == 3 {
		out.ExecSeconds = LatencyPercentiles{P50: exec[0], P95: exec[1], P99: exec[2]}
	}
	return out, nil
}

func scanIDs(rows pgx.Rows) ([]string, error) {
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
