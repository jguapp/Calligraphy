package job

import (
	"context"
	"errors"
	"fmt"
)

// The error taxonomy answers one question: after this attempt failed,
// should another attempt happen?
//
// The default answer is YES -- an unclassified error is treated as
// transient. That default is chosen for its failure mode, not its accuracy:
// wrongly retrying a permanent failure wastes (maxAttempts-1) executions
// and then lands in the DLQ where a human sees it; wrongly *not* retrying
// a transient failure silently loses work that would have succeeded. The
// costs are asymmetric, so the default leans toward retrying. Handlers that
// know better say so with NonRetryable.

// nonRetryableError marks an error as permanent: more attempts would
// produce the same failure (bad payload, 4xx from a callback target,
// business-rule rejection).
type nonRetryableError struct{ err error }

func (e *nonRetryableError) Error() string { return e.err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.err }

// NonRetryable wraps err so the runner routes the job to FAILED instead of
// scheduling a retry. Wrapping a nil error returns nil.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// IsNonRetryable reports whether err (anywhere in its chain) was marked
// permanent with NonRetryable.
func IsNonRetryable(err error) bool {
	var nr *nonRetryableError
	return errors.As(err, &nr)
}

// interruptedError marks a failure caused by the worker itself shutting
// down, not by the job. Retryable -- and the runner schedules the retry
// with (near) zero backoff, because backing off punishes the job for
// something the platform did.
type interruptedError struct{ err error }

func (e *interruptedError) Error() string { return e.err.Error() }
func (e *interruptedError) Unwrap() error { return e.err }

// Interrupted wraps err as a worker-shutdown interruption.
func Interrupted(err error) error {
	if err == nil {
		return nil
	}
	return &interruptedError{err: err}
}

// IsInterrupted reports whether err was marked as a shutdown interruption.
func IsInterrupted(err error) bool {
	var ie *interruptedError
	return errors.As(err, &ie)
}

// ErrCancelRequested is the cancellation cause the runner uses when a
// cooperative cancel flag is observed mid-run. Distinguishing it from a
// plain context.Canceled matters: cancel-requested ends in CANCELLED,
// while a deadline or shutdown cancellation ends in a retry.
var ErrCancelRequested = errors.New("job: cancellation requested")

// ErrNoHandler is returned (already marked non-retryable) when a worker
// receives a job type it has no registered handler for. Non-retryable on
// this worker fleet by definition -- redelivering won't grow a handler.
var ErrNoHandler = NonRetryable(errors.New("job: no handler registered for this job type"))

// Outcome classifies a finished attempt for recording and routing.
type Outcome string

const (
	OutcomeCompleted    Outcome = "completed"
	OutcomeFailed       Outcome = "failed"        // permanent, -> FAILED
	OutcomeRetryable    Outcome = "retryable"     // transient, -> RETRYING or DEAD_LETTER
	OutcomeCancelled    Outcome = "cancelled"     // cancel flag observed, -> CANCELLED
	OutcomeInterrupted  Outcome = "interrupted"   // worker shutdown, -> immediate retry
	OutcomeLeaseExpired Outcome = "lease_expired" // reaper reclaimed a silent worker's job
	OutcomePanicked     Outcome = "panicked"      // handler panicked; recovered and treated as retryable
)

// ClassifyError maps a handler error to an Outcome. ctxErr is the state of
// the job's context, which is what distinguishes "the handler failed" from
// "we cancelled the handler" -- a handler that returns ctx.Err() after
// cancellation shouldn't be blamed for it.
func ClassifyError(err error, cause error) Outcome {
	switch {
	case err == nil:
		return OutcomeCompleted
	case errors.Is(cause, ErrCancelRequested) || errors.Is(err, ErrCancelRequested):
		return OutcomeCancelled
	case IsInterrupted(err) || IsInterrupted(cause):
		return OutcomeInterrupted
	case IsNonRetryable(err):
		return OutcomeFailed
	case errors.Is(err, context.DeadlineExceeded):
		// The per-type execution timeout fired. Retryable: timeouts are
		// the canonical transient failure (slow dependency, cold cache).
		return OutcomeRetryable
	default:
		return OutcomeRetryable
	}
}

// AttemptError renders err for storage, bounded so a pathological error
// string (a dumped response body, say) can't bloat every attempt row.
func AttemptError(err error) string {
	if err == nil {
		return ""
	}
	const max = 2048
	s := err.Error()
	if len(s) > max {
		s = s[:max] + fmt.Sprintf(" [truncated %d bytes]", len(s)-max)
	}
	return s
}
