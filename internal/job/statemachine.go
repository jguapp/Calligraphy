package job

import "fmt"

// transitions is the complete allow-list of legal status changes. Anything
// not in this table is a bug somewhere -- there is no "force" path.
//
//	PENDING ────────► RUNNING          claim (worker won the conditional UPDATE)
//	PENDING ────────► CANCELLED        cancel before any worker claimed it
//	RUNNING ────────► COMPLETED        handler succeeded, result persisted
//	RUNNING ────────► RETRYING         transient failure (or lease expiry) with attempts left
//	RUNNING ────────► FAILED           handler declared the failure permanent
//	RUNNING ────────► DEAD_LETTER      transient failure but attempts exhausted
//	RUNNING ────────► CANCELLED        cooperative cancel observed mid-run
//	RETRYING ───────► PENDING          backoff elapsed; promoter moved it back to the stream
//	RETRYING ───────► RUNNING          a still-pending redelivery claimed it before promotion
//	RETRYING ───────► CANCELLED        cancel while waiting out backoff
//	DEAD_LETTER ────► PENDING          operator requeue (forgectl / API)
//
// RETRYING -> RUNNING deserves a note, since it looks like it skips a step:
// under at-least-once delivery the same job can legitimately have a second
// stream entry in flight (e.g. an enqueue that succeeded but whose response
// was lost got repaired by the orphan sweep). The claim UPDATE accepts
// PENDING or RETRYING precisely so that whichever delivery arrives first
// wins and the other collapses into an ack -- so the state machine must
// name that edge rather than pretend it can't happen.
var transitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusRunning:   true,
		StatusCancelled: true,
	},
	StatusRunning: {
		StatusCompleted:  true,
		StatusRetrying:   true,
		StatusFailed:     true,
		StatusDeadLetter: true,
		StatusCancelled:  true,
	},
	StatusRetrying: {
		StatusPending:   true,
		StatusRunning:   true,
		StatusCancelled: true,
	},
	StatusDeadLetter: {
		StatusPending: true,
	},
	// COMPLETED, FAILED, CANCELLED: no exits. Terminal means terminal.
	StatusCompleted: {},
	StatusFailed:    {},
	StatusCancelled: {},
}

// InvalidTransitionError reports an attempt to move a job along an edge the
// state machine does not define.
type InvalidTransitionError struct {
	From, To Status
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("job: invalid transition %s -> %s", e.From, e.To)
}

// CanTransition reports whether from -> to is a legal edge.
func CanTransition(from, to Status) bool {
	return transitions[from][to]
}

// Transition validates from -> to, returning *InvalidTransitionError on an
// illegal edge. It is pure -- the store performs the actual (conditional)
// UPDATE -- so both the worker and the store can consult the same rules,
// and the rules are testable with no infrastructure at all.
func Transition(from, to Status) error {
	if !from.Valid() {
		return fmt.Errorf("job: unknown status %q", from)
	}
	if !to.Valid() {
		return fmt.Errorf("job: unknown status %q", to)
	}
	if !CanTransition(from, to) {
		return &InvalidTransitionError{From: from, To: to}
	}
	return nil
}
