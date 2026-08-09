package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/jguapp/forge/internal/handler"
	"github.com/jguapp/forge/internal/job"
	"github.com/jguapp/forge/internal/queue"
	"github.com/jguapp/forge/internal/retry"
	"github.com/jguapp/forge/internal/store"
)

// errLeaseLost is the cancellation cause when a heartbeat discovers this
// worker no longer owns the job (the reaper declared it dead and moved
// on). Continuing would only waste CPU on a result the fence will refuse.
var errLeaseLost = errors.New("worker: lease lost to the reaper")

// panicError marks an error as born from a recovered panic, so the
// outcome can be recorded as OutcomePanicked rather than a generic
// retryable failure. Same routing, honest history.
type panicError struct{ err error }

func (e *panicError) Error() string { return e.err.Error() }
func (e *panicError) Unwrap() error { return e.err }

func isPanic(err error) bool {
	var pe *panicError
	return errors.As(err, &pe)
}

// Observer receives execution outcomes, one method per route so the
// metrics can never conflate "retried" with "dead-lettered" (they share a
// classification but not a fate). The metrics package implements it; nil
// is fine.
type Observer interface {
	JobStarted(jobType string)
	JobCompleted(jobType string, execSeconds, e2eSeconds float64)
	JobFailed(jobType string)
	JobRetried(jobType string)
	JobDeadLettered(jobType string)
	JobCancelled(jobType string)
	ClaimSkipped()
	WriteFenced()
}

// Runner executes exactly one delivery per call. It owns the full
// lifecycle: claim arbitration, lease heartbeats, cooperative
// cancellation, panic containment, outcome classification, durable
// recording, and the ack. The pool provides concurrency; the runner
// provides correctness.
type Runner struct {
	Store    *store.Store
	Recorder store.Recorder
	Queue    *queue.Queue
	Registry *handler.Registry
	Backoff  retry.Policy
	Log      *slog.Logger
	Obs      Observer

	WorkerID    string
	LeaseTTL    time.Duration
	ExecTimeout time.Duration
}

// Run processes one delivery. It never returns an error: every path ends
// in some combination of durable record + ack, or in deliberately NOT
// acking so redelivery/reaping can take over. Crashing the pool over one
// job is never the right trade.
func (r *Runner) Run(ctx context.Context, d queue.Delivery) {
	log := r.Log.With("job", d.Env.ID, "type", d.Env.Type, "entry", d.EntryID)

	leaseTTL := r.LeaseTTL
	execTimeout := r.ExecTimeout
	reg, hasHandler := r.Registry.Get(d.Env.Type)
	if hasHandler {
		if reg.Options.LeaseTTL > 0 {
			leaseTTL = reg.Options.LeaseTTL
		}
		if reg.Options.ExecTimeout > 0 {
			execTimeout = reg.Options.ExecTimeout
		}
	}

	// --- claim: the arbitration point -----------------------------------
	claim, ok, err := r.Store.ClaimJob(ctx, d.Env.ID, r.WorkerID, leaseTTL)
	if err != nil {
		// Store unreachable. Do NOT ack -- the entry stays pending and
		// will be redelivered (or reaped) once the world heals. The pool's
		// fetch backoff keeps this from spinning.
		log.Warn("claim failed, leaving delivery pending", "err", err)
		return
	}
	if !ok {
		// Someone else owns (or already finished, or cancelled) this job.
		// Expected under at-least-once delivery; the entry's work is done.
		if r.Obs != nil {
			r.Obs.ClaimSkipped()
		}
		r.ack(ctx, d, log)
		return
	}

	if r.Obs != nil {
		r.Obs.JobStarted(d.Env.Type)
	}

	if !hasHandler {
		r.finish(ctx, d, claim, nil, job.ErrNoHandler, nil, claim.StartedAt, log)
		return
	}

	// --- execute, under lease heartbeats --------------------------------
	execCtx, cancel := context.WithCancelCause(ctx)
	timeoutCtx, timeoutCancel := context.WithTimeout(execCtx, execTimeout)
	defer timeoutCancel()
	defer cancel(nil)

	hbDone := make(chan struct{})
	go r.heartbeat(execCtx, d, claim, leaseTTL, cancel, hbDone)

	j := &job.Job{
		ID: d.Env.ID, Type: d.Env.Type, Queue: d.Env.Queue,
		Status: job.StatusRunning, Priority: d.Env.Priority,
		Payload: d.Env.Payload, AttemptCount: claim.Attempt, MaxAttempts: claim.MaxAttempts,
	}
	result, handlerErr := safeHandle(timeoutCtx, reg.Handler, j)

	// The cancellation cause lives on execCtx (the heartbeat cancels it
	// with ErrCancelRequested / errLeaseLost). Capture it BEFORE the
	// cleanup cancel(nil) below -- reading it off the pool's ctx instead
	// was a real bug: cancellation classified as a generic retryable
	// failure, and cancelled jobs went to RETRYING instead of CANCELLED.
	cause := context.Cause(execCtx)

	cancel(nil) // stop the heartbeat before recording
	<-hbDone

	r.finish(ctx, d, claim, result, handlerErr, cause, claim.StartedAt, log)
}

// heartbeat renews both halves of the lease (Redis idle clock + DB
// expiry) every TTL/3, and polls the cooperative-cancel flag on the same
// tick. Two exit conditions besides ctx: the DB says we no longer own the
// job (reaped -> cancel with errLeaseLost), or a cancel was requested
// (-> cancel with job.ErrCancelRequested, which routes to CANCELLED).
func (r *Runner) heartbeat(ctx context.Context, d queue.Delivery, claim store.Claim,
	leaseTTL time.Duration, cancel context.CancelCauseFunc, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(leaseTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		hbCtx, hbCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := r.Queue.Heartbeat(hbCtx, r.WorkerID, d.Priority, d.EntryID); err != nil {
			r.Log.Warn("redis heartbeat failed", "job", d.Env.ID, "err", err)
		}
		stillOurs, err := r.Store.ExtendLease(hbCtx, d.Env.ID, claim.Epoch, leaseTTL)
		if err == nil && !stillOurs {
			hbCancel()
			cancel(errLeaseLost)
			return
		}
		if cancelled, err := r.Queue.IsCancelRequested(hbCtx, d.Env.ID); err == nil && cancelled {
			hbCancel()
			cancel(job.ErrCancelRequested)
			return
		}
		hbCancel()
	}
}

// safeHandle contains panics. A panicking handler is a failed attempt,
// not a dead worker: the stack is logged, the error is classified
// retryable (a panic is usually a data-dependent bug, but killing the
// job's remaining attempts over it helps nobody -- if it panics every
// time, the attempt budget routes it to the DLQ where the stack is
// waiting in the attempt history).
func safeHandle(ctx context.Context, h handler.Handler, j *job.Job) (result []byte, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = &panicError{err: fmt.Errorf("worker: handler panicked: %v\n%s", rec, debug.Stack())}
		}
	}()
	return h.Handle(ctx, j)
}

// finish classifies the outcome and routes it. The ordering rule
// throughout: durable record FIRST, ack SECOND. A crash between them
// re-runs the job (at-least-once); the reverse order would lose it.
func (r *Runner) finish(ctx context.Context, d queue.Delivery, claim store.Claim,
	result []byte, handlerErr error, cause error, startedAt time.Time, log *slog.Logger) {

	// Recording must survive the worker being told to shut down -- a
	// finished job whose record is dropped because ctx closed first would
	// re-run for no reason. Bounded, detached context.
	recCtx, recCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer recCancel()

	outcome := job.ClassifyError(handlerErr, cause)
	if outcome == job.OutcomeRetryable && isPanic(handlerErr) {
		outcome = job.OutcomePanicked
	}
	if handlerErr != nil && errors.Is(cause, errLeaseLost) {
		// We were reaped mid-run. The reaper owns the record now; our
		// terminal write would bounce off the fence anyway. Still record
		// OUR attempt row (idempotent insert; reaper writes the same
		// coordinates) and do NOT ack: the reaper handles the entry.
		outcome = job.OutcomeLeaseExpired
	}

	now := time.Now().UTC()
	att := store.Attempt{
		JobID: d.Env.ID, Attempt: claim.Attempt, WorkerID: r.WorkerID,
		StartedAt: startedAt, FinishedAt: now,
		Outcome: outcome, Error: job.AttemptError(handlerErr),
	}

	var applied bool
	var recErr error
	ackAfter := true
	var notify func()

	switch outcome {
	case job.OutcomeCompleted:
		applied, recErr = r.Recorder.Complete(recCtx, d.Env.ID, claim.Epoch, result, att)
		exec := now.Sub(startedAt).Seconds()
		e2e := now.Sub(d.Env.EnqueuedAt).Seconds()
		notify = func() { r.Obs.JobCompleted(d.Env.Type, exec, e2e) }

	case job.OutcomeFailed:
		applied, recErr = r.Recorder.Fail(recCtx, d.Env.ID, claim.Epoch, job.AttemptError(handlerErr), att)
		notify = func() { r.Obs.JobFailed(d.Env.Type) }

	case job.OutcomeCancelled:
		applied, recErr = r.Recorder.Cancelled(recCtx, d.Env.ID, claim.Epoch, att)
		notify = func() { r.Obs.JobCancelled(d.Env.Type) }

	case job.OutcomeLeaseExpired:
		if err := r.Store.RecordAttempt(recCtx, att); err != nil {
			log.Warn("recording lease-expired attempt failed", "err", err)
		}
		return // no ack, no terminal write: the reaper owns this entry now

	case job.OutcomeInterrupted, job.OutcomeRetryable, job.OutcomePanicked:
		if outcome == job.OutcomePanicked {
			log.Error("handler panicked", "err", handlerErr)
		}
		if claim.Attempt >= claim.MaxAttempts {
			applied, recErr = r.Recorder.DeadLetter(recCtx, d.Env.ID, claim.Epoch, job.AttemptError(handlerErr), att)
			notify = func() { r.Obs.JobDeadLettered(d.Env.Type) }
			if recErr == nil && applied {
				if err := r.Queue.DeadLetter(recCtx, d.Env, job.AttemptError(handlerErr)); err != nil {
					log.Warn("dlq log append failed (postgres record stands)", "err", err)
				}
			}
		} else {
			// Interrupted attempts retry immediately -- the platform
			// stopped the job, so the job shouldn't pay a backoff.
			delay := time.Duration(0)
			if outcome != job.OutcomeInterrupted {
				delay = r.Backoff.Delay(claim.Attempt)
			}
			nextAt := now.Add(delay)
			applied, recErr = r.Recorder.Retry(recCtx, d.Env.ID, claim.Epoch, job.AttemptError(handlerErr), nextAt, att)
			notify = func() { r.Obs.JobRetried(d.Env.Type) }
			if recErr == nil && applied {
				next := d.Env
				next.Attempt = claim.Attempt + 1
				if err := r.Queue.ScheduleRetry(recCtx, next, nextAt); err != nil {
					// The DB says RETRYING; the delayed set never heard.
					// The stuck-retry sweep repairs exactly this, so we
					// still ack -- the entry's job is done even though
					// the handoff wasn't.
					log.Warn("retry handoff to redis failed; stuck-retry sweep will repair", "err", err)
				}
			}
		}
	}

	if recErr != nil {
		// The record never became durable. Leave the entry unacked: it
		// will be redelivered, lose the claim arbitration against our
		// still-RUNNING row, and eventually the DB sweep reaps the row
		// into a retry. Slow-path recovery, but no lost work.
		log.Error("recording outcome failed; leaving delivery pending", "outcome", outcome, "err", recErr)
		ackAfter = false
	}
	if !applied && recErr == nil {
		if r.Obs != nil {
			r.Obs.WriteFenced()
		}
		log.Info("terminal write fenced (job owned elsewhere); acking stale delivery")
	}

	if ackAfter {
		r.ack(ctx, d, log)
	}
	if r.Obs != nil && notify != nil && recErr == nil {
		notify()
	}
}

func (r *Runner) ack(ctx context.Context, d queue.Delivery, log *slog.Logger) {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.Queue.Ack(ackCtx, d.Priority, d.EntryID); err != nil {
		// The record is durable; only the ack was lost. Redelivery will
		// lose the claim arbitration and ack then. Log and move on.
		log.Warn("ack failed; redelivery will collapse via claim arbitration", "err", err)
	}
}
