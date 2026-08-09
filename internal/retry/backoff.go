// Package retry computes backoff delays.
//
// The policy is "full jitter" exponential backoff:
//
//	delay = uniform(0, min(cap, base * 2^(attempt-1)))
//
// chosen over fixed backoff and over equal jitter for one property:
// decorrelation. When 500 jobs fail against the same downed dependency in
// the same second, fixed backoff retries all 500 in the same instant, in
// lockstep, forever -- the classic retry storm, and each synchronized wave
// lands on a dependency that's trying to recover. Full jitter spreads the
// wave uniformly across the whole window, and AWS's published measurements
// (the "Exponential Backoff and Jitter" architecture post) found it best
// among the variants for both total server load and time-to-completion.
//
// The cost of full jitter is that a retry may fire almost immediately
// (uniform includes ~0). That's fine here: an immediate retry against a
// *blipped* dependency often succeeds, and against a *down* one it just
// consumes one attempt whose successor backs off over a larger window.
package retry

import (
	"math/rand/v2"
	"time"
)

// Policy is immutable after construction; the zero value is unusable --
// use New.
type Policy struct {
	base time.Duration
	cap  time.Duration
	// rng is injectable for deterministic tests; production uses the
	// global math/rand/v2 generator, which is already properly seeded and
	// concurrency-safe.
	rng func() float64
}

func New(base, cap time.Duration) Policy {
	return Policy{base: base, cap: cap, rng: rand.Float64}
}

// NewDeterministic is for tests: rng supplies the uniform draw.
func NewDeterministic(base, cap time.Duration, rng func() float64) Policy {
	return Policy{base: base, cap: cap, rng: rng}
}

// Delay computes the backoff before attempt+1, where attempt is the
// 1-based attempt that just failed. Attempt values below 1 are treated
// as 1.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Ceiling, computed without overflow: doubling stops once past cap.
	ceiling := p.base
	for i := 1; i < attempt; i++ {
		if ceiling >= p.cap {
			break
		}
		ceiling *= 2
	}
	if ceiling > p.cap {
		ceiling = p.cap
	}
	return time.Duration(p.rng() * float64(ceiling))
}
