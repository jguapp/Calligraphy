package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Recorder is how the runner records a finished attempt. Two
// implementations:
//
//   - SyncRecorder: one round trip per write. Simple, and the benchmark
//     baseline.
//   - Batcher: coalesces writes into periodic multi-row flushes.
//
// Why this exists: recording one completed job costs at least two
// statements (the fenced status UPDATE and the attempt INSERT). At 1,000
// jobs/sec that is 2,000 round trips and 2,000 WAL fsync opportunities per
// second, and profiling says that -- not Go, not Redis -- is where a job
// system's throughput goes to die. The batcher turns N jobs' records into
// three statements inside one transaction: one fsync amortized over the
// whole batch.
//
// The contract callers rely on: the returned `applied` is false when the
// fencing condition rejected the write (someone else owns the job now).
// Either way, when the call returns without error the record is durable --
// the runner acks Redis only after that, so a crash can lose an ack (job
// re-runs; at-least-once) but never a record.
type Recorder interface {
	Complete(ctx context.Context, id string, epoch int64, result json.RawMessage, att Attempt) (applied bool, err error)
	Fail(ctx context.Context, id string, epoch int64, errMsg string, att Attempt) (applied bool, err error)
	Retry(ctx context.Context, id string, epoch int64, errMsg string, nextAt time.Time, att Attempt) (applied bool, err error)
	DeadLetter(ctx context.Context, id string, epoch int64, errMsg string, att Attempt) (applied bool, err error)
	Cancelled(ctx context.Context, id string, epoch int64, att Attempt) (applied bool, err error)
}

// ------------------------------------------------------------ SyncRecorder

// SyncRecorder writes every record immediately. The benchmark baseline,
// and also what you'd want under very low traffic where batching latency
// buys nothing.
type SyncRecorder struct{ Store *Store }

func (r SyncRecorder) record(ctx context.Context, att Attempt) {
	// Attempt rows are history: failing to write one must not turn a
	// successfully-recorded outcome into an error. (The batcher gets this
	// property from writing both in one transaction instead.)
	if err := r.Store.RecordAttempt(ctx, att); err != nil {
		slog.Warn("store: attempt record failed", "job", att.JobID, "err", err)
	}
}

func (r SyncRecorder) Complete(ctx context.Context, id string, epoch int64, result json.RawMessage, att Attempt) (bool, error) {
	ok, err := r.Store.CompleteJob(ctx, id, epoch, result)
	if err != nil {
		return false, err
	}
	r.record(ctx, att)
	return ok, nil
}

func (r SyncRecorder) Fail(ctx context.Context, id string, epoch int64, errMsg string, att Attempt) (bool, error) {
	ok, err := r.Store.FailJob(ctx, id, epoch, errMsg)
	if err != nil {
		return false, err
	}
	r.record(ctx, att)
	return ok, nil
}

func (r SyncRecorder) Retry(ctx context.Context, id string, epoch int64, errMsg string, nextAt time.Time, att Attempt) (bool, error) {
	ok, err := r.Store.ScheduleRetry(ctx, id, epoch, errMsg, nextAt)
	if err != nil {
		return false, err
	}
	r.record(ctx, att)
	return ok, nil
}

func (r SyncRecorder) DeadLetter(ctx context.Context, id string, epoch int64, errMsg string, att Attempt) (bool, error) {
	ok, err := r.Store.DeadLetterJob(ctx, id, epoch, errMsg)
	if err != nil {
		return false, err
	}
	r.record(ctx, att)
	return ok, nil
}

func (r SyncRecorder) Cancelled(ctx context.Context, id string, epoch int64, att Attempt) (bool, error) {
	ok, err := r.Store.CancelRunning(ctx, id, epoch)
	if err != nil {
		return false, err
	}
	r.record(ctx, att)
	return ok, nil
}

// ----------------------------------------------------------------- Batcher

type opKind int

const (
	opComplete opKind = iota
	opFail
	opRetry
	opDead
	opCancel
)

type opResult struct {
	applied bool
	err     error
}

type op struct {
	kind   opKind
	id     string
	epoch  int64
	result string // completes
	errMsg string // fail/retry/dead
	nextAt time.Time
	att    Attempt
	done   chan opResult // buffered(1); resolved exactly once per op
}

// Batcher coalesces Recorder calls. Flushes when BatchMaxSize ops are
// waiting or BatchInterval elapses, whichever comes first -- so batching
// costs at most one interval of extra latency and, under load, effectively
// none (the size trigger fires first).
type Batcher struct {
	store    *Store
	maxSize  int
	interval time.Duration
	log      *slog.Logger

	in   chan op
	stop chan struct{}
	wg   sync.WaitGroup
}

func NewBatcher(s *Store, maxSize int, interval time.Duration, log *slog.Logger) *Batcher {
	if log == nil {
		log = slog.Default()
	}
	return &Batcher{
		store:    s,
		maxSize:  maxSize,
		interval: interval,
		log:      log,
		in:       make(chan op, maxSize*2),
		stop:     make(chan struct{}),
	}
}

// Start launches the flush loop. Stop() flushes whatever is queued and
// returns once it's durable -- which is what makes worker drain safe: every
// job the worker finished has its record on disk before the process exits.
func (b *Batcher) Start() {
	b.wg.Add(1)
	go b.loop()
}

func (b *Batcher) Stop() {
	close(b.stop)
	b.wg.Wait()
}

func (b *Batcher) loop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	pending := make([]op, 0, b.maxSize)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		b.flush(pending)
		pending = pending[:0]
	}

	for {
		select {
		case o := <-b.in:
			pending = append(pending, o)
			if len(pending) >= b.maxSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-b.stop:
			// Drain everything already submitted, then flush once more.
			for {
				select {
				case o := <-b.in:
					pending = append(pending, o)
					continue
				default:
				}
				break
			}
			flush()
			return
		}
	}
}

func (b *Batcher) submit(ctx context.Context, o op) (bool, error) {
	o.done = make(chan opResult, 1)
	select {
	case b.in <- o:
	case <-b.stop:
		return false, errors.New("store: batcher stopped")
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case r := <-o.done:
		return r.applied, r.err
	case <-ctx.Done():
		// The op is already queued and WILL be flushed; we just stopped
		// waiting for the receipt. Report the context error so the caller
		// treats the record as unconfirmed (and doesn't ack).
		return false, ctx.Err()
	}
}

func (b *Batcher) Complete(ctx context.Context, id string, epoch int64, result json.RawMessage, att Attempt) (bool, error) {
	res := "null"
	if len(result) > 0 {
		res = string(result)
	}
	return b.submit(ctx, op{kind: opComplete, id: id, epoch: epoch, result: res, att: att})
}

func (b *Batcher) Fail(ctx context.Context, id string, epoch int64, errMsg string, att Attempt) (bool, error) {
	return b.submit(ctx, op{kind: opFail, id: id, epoch: epoch, errMsg: errMsg, att: att})
}

func (b *Batcher) Retry(ctx context.Context, id string, epoch int64, errMsg string, nextAt time.Time, att Attempt) (bool, error) {
	return b.submit(ctx, op{kind: opRetry, id: id, epoch: epoch, errMsg: errMsg, nextAt: nextAt, att: att})
}

func (b *Batcher) DeadLetter(ctx context.Context, id string, epoch int64, errMsg string, att Attempt) (bool, error) {
	return b.submit(ctx, op{kind: opDead, id: id, epoch: epoch, errMsg: errMsg, att: att})
}

func (b *Batcher) Cancelled(ctx context.Context, id string, epoch int64, att Attempt) (bool, error) {
	return b.submit(ctx, op{kind: opCancel, id: id, epoch: epoch, att: att})
}

// flush writes one batch in one transaction and resolves every op's done
// channel. On transaction failure it retries once (fresh transaction, short
// pause) before failing the ops -- at which point the runners won't ack,
// and redelivery re-runs the jobs. Note the ambiguity that retrying
// introduces: if the first commit actually landed but its confirmation was
// lost, the second run's fenced UPDATEs match zero rows and report
// applied=false for work that IS recorded. Callers already treat
// applied=false as "someone else owns this; ack and move on", which is the
// correct behavior for that case too.
func (b *Batcher) flush(ops []op) {
	err := b.flushOnce(ops)
	if err != nil {
		b.log.Warn("store: batch flush failed, retrying once", "ops", len(ops), "err", err)
		time.Sleep(50 * time.Millisecond)
		err = b.flushOnce(ops)
	}
	if err != nil {
		b.log.Error("store: batch flush failed permanently", "ops", len(ops), "err", err)
		for _, o := range ops {
			o.done <- opResult{err: err}
		}
	}
}

func (b *Batcher) flushOnce(ops []op) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := b.store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	applied := make(map[int]bool, len(ops)) // index into ops

	// Completions as one multi-row fenced UPDATE. RETURNING tells us which
	// rows the fence accepted.
	var cIdx []int
	var cIDs, cResults []string
	var cEpochs []int64
	for i, o := range ops {
		if o.kind == opComplete {
			cIdx = append(cIdx, i)
			cIDs = append(cIDs, o.id)
			cEpochs = append(cEpochs, o.epoch)
			cResults = append(cResults, o.result)
		}
	}
	if len(cIDs) > 0 {
		rows, err := tx.Query(ctx, `
			UPDATE jobs SET status = 'COMPLETED', result = v.result::jsonb, error = NULL,
				completed_at = now(), lease_expires_at = NULL, updated_at = now()
			FROM (SELECT unnest($1::text[]) AS id, unnest($2::bigint[]) AS epoch,
			             unnest($3::text[]) AS result) v
			WHERE jobs.id = v.id AND jobs.lease_epoch = v.epoch AND jobs.status = 'RUNNING'
			RETURNING jobs.id`, cIDs, cEpochs, cResults)
		if err != nil {
			return fmt.Errorf("batch complete: %w", err)
		}
		okIDs := map[string]bool{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			okIDs[id] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, i := range cIdx {
			applied[i] = okIDs[ops[i].id]
		}
	}

	// The rarer kinds individually, but still inside this one transaction.
	for i, o := range ops {
		var sql string
		var args []any
		switch o.kind {
		case opComplete:
			continue
		case opFail:
			sql = `UPDATE jobs SET status='FAILED', error=$3, completed_at=now(),
				lease_expires_at=NULL, updated_at=now()
				WHERE id=$1 AND lease_epoch=$2 AND status='RUNNING'`
			args = []any{o.id, o.epoch, o.errMsg}
		case opRetry:
			sql = `UPDATE jobs SET status='RETRYING', error=$3, scheduled_at=$4,
				lease_expires_at=NULL, worker_id=NULL, updated_at=now()
				WHERE id=$1 AND lease_epoch=$2 AND status='RUNNING'`
			args = []any{o.id, o.epoch, o.errMsg, o.nextAt}
		case opDead:
			sql = `UPDATE jobs SET status='DEAD_LETTER', error=$3, completed_at=now(),
				lease_expires_at=NULL, updated_at=now()
				WHERE id=$1 AND lease_epoch=$2 AND status='RUNNING'`
			args = []any{o.id, o.epoch, o.errMsg}
		case opCancel:
			sql = `UPDATE jobs SET status='CANCELLED', completed_at=now(),
				lease_expires_at=NULL, updated_at=now()
				WHERE id=$1 AND lease_epoch=$2 AND status='RUNNING'`
			args = []any{o.id, o.epoch}
		}
		tag, err := tx.Exec(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf("batch op %d: %w", o.kind, err)
		}
		applied[i] = tag.RowsAffected() == 1
	}

	// All attempt rows in one INSERT. ON CONFLICT DO NOTHING makes the
	// flush-retry path idempotent.
	ids := make([]string, len(ops))
	attempts := make([]int32, len(ops))
	workers := make([]string, len(ops))
	starts := make([]time.Time, len(ops))
	finishes := make([]time.Time, len(ops))
	outcomes := make([]string, len(ops))
	errsCol := make([]string, len(ops))
	for i, o := range ops {
		ids[i] = o.att.JobID
		attempts[i] = int32(o.att.Attempt)
		workers[i] = o.att.WorkerID
		starts[i] = o.att.StartedAt
		finishes[i] = o.att.FinishedAt
		outcomes[i] = string(o.att.Outcome)
		errsCol[i] = o.att.Error
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, outcome, error)
		SELECT * FROM unnest($1::text[], $2::int[], $3::text[], $4::timestamptz[],
		                     $5::timestamptz[], $6::text[], $7::text[])
		ON CONFLICT (job_id, attempt) DO NOTHING`,
		ids, attempts, workers, starts, finishes, outcomes, errsCol); err != nil {
		return fmt.Errorf("batch attempts: %w", err)
	}

	// Events for the state changes that want explaining later.
	var eIDs, eNames, eDetails []string
	for i, o := range ops {
		if !applied[i] {
			continue
		}
		switch o.kind {
		case opRetry:
			d, _ := json.Marshal(map[string]any{"nextAt": o.nextAt, "error": o.errMsg})
			eIDs, eNames, eDetails = append(eIDs, o.id), append(eNames, "retry_scheduled"), append(eDetails, string(d))
		case opDead:
			d, _ := json.Marshal(map[string]any{"error": o.errMsg})
			eIDs, eNames, eDetails = append(eIDs, o.id), append(eNames, "dead_lettered"), append(eDetails, string(d))
		case opCancel:
			eIDs, eNames, eDetails = append(eIDs, o.id), append(eNames, "cancelled"), append(eDetails, "null")
		}
	}
	if len(eIDs) > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO job_events (job_id, event, detail)
			SELECT * FROM unnest($1::text[], $2::text[], $3::jsonb[])`,
			eIDs, eNames, eDetails); err != nil {
			return fmt.Errorf("batch events: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	for i, o := range ops {
		o.done <- opResult{applied: applied[i]}
	}
	return nil
}
