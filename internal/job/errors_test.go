package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNonRetryable(t *testing.T) {
	base := errors.New("bad payload")
	err := NonRetryable(base)

	if !IsNonRetryable(err) {
		t.Error("IsNonRetryable(NonRetryable(err)) = false")
	}
	// The mark must survive further wrapping -- handlers wrap with %w.
	wrapped := fmt.Errorf("handler: %w", err)
	if !IsNonRetryable(wrapped) {
		t.Error("mark lost through fmt.Errorf %%w wrapping")
	}
	if !errors.Is(wrapped, base) {
		t.Error("original error lost from chain")
	}
	if IsNonRetryable(base) {
		t.Error("unwrapped error reports non-retryable")
	}
	if NonRetryable(nil) != nil {
		t.Error("NonRetryable(nil) != nil")
	}
}

func TestInterrupted(t *testing.T) {
	err := Interrupted(context.Canceled)
	if !IsInterrupted(err) {
		t.Error("IsInterrupted(Interrupted(err)) = false")
	}
	if !errors.Is(err, context.Canceled) {
		t.Error("cause lost")
	}
	if IsInterrupted(context.Canceled) {
		t.Error("bare context.Canceled reports interrupted")
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		cause error
		want  Outcome
	}{
		{"nil is completed", nil, nil, OutcomeCompleted},
		{"plain error retries", errors.New("boom"), nil, OutcomeRetryable},
		{"non-retryable fails", NonRetryable(errors.New("bad")), nil, OutcomeFailed},
		{"wrapped non-retryable fails", fmt.Errorf("h: %w", NonRetryable(errors.New("bad"))), nil, OutcomeFailed},
		{"deadline retries", context.DeadlineExceeded, nil, OutcomeRetryable},
		{"wrapped deadline retries", fmt.Errorf("h: %w", context.DeadlineExceeded), nil, OutcomeRetryable},
		{"cancel-requested cause wins", context.Canceled, ErrCancelRequested, OutcomeCancelled},
		{"cancel-requested in err", fmt.Errorf("h: %w", ErrCancelRequested), nil, OutcomeCancelled},
		{"interrupted err", Interrupted(context.Canceled), nil, OutcomeInterrupted},
		{"interrupted cause", context.Canceled, Interrupted(context.Canceled), OutcomeInterrupted},
		{"no handler is permanent", ErrNoHandler, nil, OutcomeFailed},
		// A handler returning a NonRetryable error while a cancel was also
		// requested: cancellation wins, because the run was interfered with
		// and the "permanent" verdict may be an artifact of that.
		{"cancel beats non-retryable", NonRetryable(errors.New("x")), ErrCancelRequested, OutcomeCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err, tt.cause); got != tt.want {
				t.Errorf("ClassifyError() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestAttemptError(t *testing.T) {
	if AttemptError(nil) != "" {
		t.Error("nil error should render empty")
	}
	long := errors.New(strings.Repeat("e", 5000))
	s := AttemptError(long)
	if len(s) > 2100 {
		t.Errorf("truncation failed, len=%d", len(s))
	}
	if !strings.Contains(s, "truncated") {
		t.Error("truncation not marked")
	}
}
