// Package job defines Calligraphy's core domain: the job record, the lifecycle
// state machine, and the error taxonomy handlers use to steer retry
// behavior.
//
// It depends on nothing but the standard library and a UUID generator.
// That's deliberate: store, queue, worker, and api all import this package,
// so anything added here becomes a dependency of the entire system -- and
// keeping it dependency-free is what lets the state machine be tested
// without a database or a queue anywhere in sight.
package job

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Status is a job's position in its lifecycle. Stored as text in Postgres
// and serialized verbatim over the API, so these strings are a public
// contract -- renaming one is a breaking change, not a refactor.
type Status string

const (
	// StatusPending: accepted and durable; waiting to be claimed by a
	// worker (or waiting for its scheduled_at to arrive).
	StatusPending Status = "PENDING"
	// StatusRunning: claimed by a worker that holds a live lease.
	StatusRunning Status = "RUNNING"
	// StatusCompleted: the handler returned successfully; the result is
	// persisted. Terminal.
	StatusCompleted Status = "COMPLETED"
	// StatusFailed: the handler declared the failure permanent -- retrying
	// would produce the same failure again. Terminal.
	StatusFailed Status = "FAILED"
	// StatusRetrying: failed transiently; parked in the delayed set until
	// backoff elapses, then promoted back to PENDING.
	StatusRetrying Status = "RETRYING"
	// StatusDeadLetter: retries exhausted; parked for human inspection and
	// possible requeue. Terminal until an operator intervenes.
	StatusDeadLetter Status = "DEAD_LETTER"
	// StatusCancelled: cancelled before or during execution. Terminal.
	StatusCancelled Status = "CANCELLED"
)

// Statuses lists every valid status, for validation and for iterating in
// tests. Order is not significant.
var Statuses = []Status{
	StatusPending, StatusRunning, StatusCompleted, StatusFailed,
	StatusRetrying, StatusDeadLetter, StatusCancelled,
}

// Valid reports whether s is one of the defined statuses.
func (s Status) Valid() bool {
	for _, v := range Statuses {
		if s == v {
			return true
		}
	}
	return false
}

// Terminal reports whether a job in this status is finished. A terminal job
// holds no lease, occupies no queue slot, and (DEAD_LETTER's operator
// requeue aside) never changes status again.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusDeadLetter, StatusCancelled:
		return true
	}
	return false
}

// Priority selects which stream a job is enqueued to. Workers drain higher
// priorities first (see queue.Fetch); within one priority, order is FIFO.
type Priority string

const (
	PriorityHigh    Priority = "high"
	PriorityDefault Priority = "default"
	PriorityLow     Priority = "low"
)

// PriorityOrder is the dispatch order workers use: high is always offered
// work before default, default before low.
var PriorityOrder = []Priority{PriorityHigh, PriorityDefault, PriorityLow}

// Valid reports whether p is one of the defined priorities.
func (p Priority) Valid() bool {
	return p == PriorityHigh || p == PriorityDefault || p == PriorityLow
}

// Limits enforced at admission. These are hard caps baked into the domain;
// the API is free to configure something tighter, never looser.
const (
	// MaxPayloadBytes bounds a single job's payload. The queue carries the
	// payload inside each Redis stream entry (so dispatch never needs a
	// database read), which means an unbounded payload is unbounded Redis
	// memory multiplied by queue depth.
	MaxPayloadBytes = 1 << 20 // 1 MiB

	// MaxAttemptsCeiling bounds how many attempts a submitter may ask for.
	// Past ~25 attempts with capped exponential backoff, a job that still
	// fails is not transiently failing -- it belongs in the DLQ where a
	// human can see it, not in a permanent silent retry loop.
	MaxAttemptsCeiling = 25

	// DefaultMaxAttempts is used when the submitter doesn't say. Five
	// attempts under full-jitter backoff spreads roughly a minute of
	// retrying -- enough to ride out a dependency blip, short enough that
	// a genuinely broken job surfaces in the DLQ quickly.
	DefaultMaxAttempts = 5

	// DefaultQueue is the queue jobs land on unless the submitter names one.
	DefaultQueue = "default"
)

// nameRe validates job types and queue names. Deliberately strict: these
// strings become Redis key fragments and Prometheus label values, both of
// which get painful with arbitrary bytes in them.
var nameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$`)

// ValidName reports whether s is usable as a job type or queue name.
func ValidName(s string) bool { return nameRe.MatchString(s) }

// Job is the persistent record of one unit of work. The database row is
// the source of truth; the queue only ever carries an Envelope (below).
type Job struct {
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	Queue    string   `json:"queue"`
	Status   Status   `json:"status"`
	Priority Priority `json:"priority"`

	Payload json.RawMessage `json:"payload,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   string          `json:"error,omitempty"`

	// IdempotencyKey deduplicates submissions: two submissions of the same
	// (type, key) return the same job. Empty means no deduplication.
	IdempotencyKey string `json:"idempotencyKey,omitempty"`

	// AttemptCount is how many executions have been *started* (not how
	// many have finished). Incremented at claim time by the store, which
	// makes it correct even when a worker dies mid-attempt.
	AttemptCount int `json:"attemptCount"`
	MaxAttempts  int `json:"maxAttempts"`

	// LeaseEpoch is a fencing token. It increments every time ownership of
	// the job changes hands (claim, reap), and every terminal write carries
	// the epoch its writer holds -- so a worker that stalled past its lease
	// and lost the job to the reaper cannot overwrite state written by the
	// job's next owner. See docs: "fencing" in the README failure model.
	LeaseEpoch     int64      `json:"-"`
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt,omitempty"`
	WorkerID       string     `json:"workerId,omitempty"`

	CreatedAt   time.Time  `json:"createdAt"`
	ScheduledAt time.Time  `json:"scheduledAt"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// Options are the submitter-controlled knobs on a new job. The zero value
// is valid and means: default queue, default priority, default attempts,
// no deduplication, ready immediately.
type Options struct {
	Queue          string
	Priority       Priority
	MaxAttempts    int
	IdempotencyKey string
	// ScheduledAt delays the job's first execution. Zero means "now".
	ScheduledAt time.Time
}

// New validates and constructs a Job in StatusPending. It does not persist
// anything -- the store owns that -- but everything invalid is rejected
// here, so nothing below this layer needs to re-validate.
func New(jobType string, payload json.RawMessage, opts Options, now time.Time) (*Job, error) {
	if !ValidName(jobType) {
		return nil, fmt.Errorf("job: invalid type %q (want %s)", jobType, nameRe)
	}
	if len(payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("job: payload is %d bytes, limit %d", len(payload), MaxPayloadBytes)
	}
	if len(payload) > 0 && !json.Valid(payload) {
		return nil, fmt.Errorf("job: payload is not valid JSON")
	}

	q := opts.Queue
	if q == "" {
		q = DefaultQueue
	}
	if !ValidName(q) {
		return nil, fmt.Errorf("job: invalid queue %q", q)
	}

	p := opts.Priority
	if p == "" {
		p = PriorityDefault
	}
	if !p.Valid() {
		return nil, fmt.Errorf("job: invalid priority %q", p)
	}

	ma := opts.MaxAttempts
	if ma == 0 {
		ma = DefaultMaxAttempts
	}
	if ma < 1 || ma > MaxAttemptsCeiling {
		return nil, fmt.Errorf("job: maxAttempts %d out of range [1,%d]", ma, MaxAttemptsCeiling)
	}

	if len(opts.IdempotencyKey) > 256 {
		return nil, fmt.Errorf("job: idempotency key longer than 256 bytes")
	}

	sched := opts.ScheduledAt
	if sched.IsZero() {
		sched = now
	}

	return &Job{
		ID:             uuid.NewString(),
		Type:           jobType,
		Queue:          q,
		Status:         StatusPending,
		Priority:       p,
		Payload:        payload,
		IdempotencyKey: opts.IdempotencyKey,
		MaxAttempts:    ma,
		CreatedAt:      now,
		ScheduledAt:    sched,
	}, nil
}

// Envelope is the message that actually travels through Redis. It is
// self-contained -- payload included -- so dispatching a job costs zero
// database reads; the database is written to (claim, result) but never
// consulted to find out what the work *is*.
//
// The envelope is not authoritative. If it disagrees with the database row
// (a stale redelivery after a retry, say), the database wins: the claim
// UPDATE in the store is conditional on status, so a stale envelope simply
// fails to claim and gets acked away.
type Envelope struct {
	ID       string   `json:"id"`
	Type     string   `json:"type"`
	Queue    string   `json:"queue"`
	Priority Priority `json:"priority"`
	// Attempt is the 1-based attempt number this delivery is *expected* to
	// be. Informational (logging, DLQ records); the store's attempt_count
	// is the authoritative counter.
	Attempt    int             `json:"attempt"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	EnqueuedAt time.Time       `json:"enqueuedAt"`
}

// NewEnvelope builds the first-delivery envelope for a job.
func NewEnvelope(j *Job, now time.Time) Envelope {
	return Envelope{
		ID:         j.ID,
		Type:       j.Type,
		Queue:      j.Queue,
		Priority:   j.Priority,
		Attempt:    1,
		Payload:    j.Payload,
		EnqueuedAt: now,
	}
}

// Encode serializes the envelope for a stream entry / delayed-set member.
func (e Envelope) Encode() (string, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("job: encoding envelope: %w", err)
	}
	return string(b), nil
}

// DecodeEnvelope parses a stream entry / delayed-set member back into an
// Envelope. A failure here means a corrupt ("poison") entry; the queue
// layer acks those away rather than redelivering them forever.
func DecodeEnvelope(s string) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal([]byte(s), &e); err != nil {
		return Envelope{}, fmt.Errorf("job: decoding envelope: %w", err)
	}
	if e.ID == "" || e.Type == "" {
		return Envelope{}, fmt.Errorf("job: envelope missing id or type")
	}
	return e, nil
}
