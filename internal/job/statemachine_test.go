package job

import (
	"errors"
	"testing"
)

// TestTransitionMatrix exercises every (from, to) pair, not just the legal
// ones -- the point of an explicit state machine is that the illegal edges
// are as tested as the legal ones.
func TestTransitionMatrix(t *testing.T) {
	legal := map[[2]Status]bool{
		{StatusPending, StatusRunning}:    true,
		{StatusPending, StatusCancelled}:  true,
		{StatusRunning, StatusCompleted}:  true,
		{StatusRunning, StatusRetrying}:   true,
		{StatusRunning, StatusFailed}:     true,
		{StatusRunning, StatusDeadLetter}: true,
		{StatusRunning, StatusCancelled}:  true,
		{StatusRetrying, StatusPending}:   true,
		{StatusRetrying, StatusRunning}:   true,
		{StatusRetrying, StatusCancelled}: true,
		{StatusDeadLetter, StatusPending}: true,
	}

	for _, from := range Statuses {
		for _, to := range Statuses {
			err := Transition(from, to)
			want := legal[[2]Status{from, to}]
			if want && err != nil {
				t.Errorf("Transition(%s, %s) = %v, want legal", from, to, err)
			}
			if !want {
				var ite *InvalidTransitionError
				if err == nil {
					t.Errorf("Transition(%s, %s) allowed, want rejected", from, to)
				} else if !errors.As(err, &ite) {
					t.Errorf("Transition(%s, %s) error type = %T, want *InvalidTransitionError", from, to, err)
				} else if ite.From != from || ite.To != to {
					t.Errorf("error carries %s -> %s, want %s -> %s", ite.From, ite.To, from, to)
				}
			}
		}
	}
}

func TestTransitionUnknownStatus(t *testing.T) {
	if err := Transition("BOGUS", StatusRunning); err == nil {
		t.Error("unknown from-status accepted")
	}
	if err := Transition(StatusPending, "BOGUS"); err == nil {
		t.Error("unknown to-status accepted")
	}
}

func TestTerminal(t *testing.T) {
	want := map[Status]bool{
		StatusPending:    false,
		StatusRunning:    false,
		StatusRetrying:   false,
		StatusCompleted:  true,
		StatusFailed:     true,
		StatusDeadLetter: true,
		StatusCancelled:  true,
	}
	for s, w := range want {
		if got := s.Terminal(); got != w {
			t.Errorf("%s.Terminal() = %v, want %v", s, got, w)
		}
	}
}

// TestTerminalStatesHaveNoExits pins the invariant the Terminal() helper
// implies: no legal edge leaves a terminal state, with the one documented
// exception of the operator's DEAD_LETTER -> PENDING requeue.
func TestTerminalStatesHaveNoExits(t *testing.T) {
	for _, from := range Statuses {
		if !from.Terminal() {
			continue
		}
		for _, to := range Statuses {
			if CanTransition(from, to) && !(from == StatusDeadLetter && to == StatusPending) {
				t.Errorf("terminal state %s has exit to %s", from, to)
			}
		}
	}
}
