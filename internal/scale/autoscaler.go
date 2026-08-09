// Package scale adjusts a worker's concurrency from queue depth. This is
// the in-process half of "dynamic worker scaling" -- one worker growing
// and shrinking its own pool. The other half, more replicas, belongs to
// the orchestrator (compose --scale, a k8s HPA on forge_queue_depth); the
// two compose because each worker independently converges on a sane
// concurrency for whatever share of the backlog it sees.
//
// The asymmetry is the design: scale up eagerly (backlog is visible,
// waiting costs latency NOW), scale down reluctantly (idleness must
// persist before it means anything -- queues breathe, and a scaler that
// exhales on every lull thrashes). Concretely: growth may happen every
// interval; shrink needs IdleAfter consecutive idle observations.
package scale

import (
	"context"
	"log/slog"
	"time"
)

// Pool is what the autoscaler steers -- worker.Pool satisfies it.
type Pool interface {
	Active() int
	Target() int
	SetTarget(int)
}

// DepthsFunc reports how many entries are ready to claim right now.
type DepthsFunc func(ctx context.Context) (ready int64, err error)

type Config struct {
	Min, Max int
	// Interval between decisions (and therefore the fastest growth rate).
	Interval time.Duration
	// GrowAt is the ready-backlog-per-slot ratio that triggers growth: at
	// 2.0, a pool of 4 grows when 8+ jobs are waiting. Below it, current
	// capacity is keeping up and growth would just burn memory.
	GrowAt float64
	// IdleAfter is how many consecutive idle intervals (zero backlog AND
	// idle slots) must pass before shrinking one step.
	IdleAfter int
}

func DefaultConfig(min, max int) Config {
	return Config{Min: min, Max: max, Interval: 3 * time.Second, GrowAt: 2.0, IdleAfter: 5}
}

type Autoscaler struct {
	Cfg    Config
	Pool   Pool
	Depths DepthsFunc
	Log    *slog.Logger

	idleTicks int
}

// Next is the pure decision: given the observed backlog and pool state,
// the new target. Extracted from the loop so the policy is table-testable
// without time, Redis, or a pool.
func (a *Autoscaler) Next(ready int64, active, target int) int {
	switch {
	case target < a.Cfg.Min:
		return a.Cfg.Min

	case float64(ready) >= a.Cfg.GrowAt*float64(target):
		// Grow by half, at least one -- multiplicative growth reaches a
		// deep backlog's right size in a few intervals without stepping
		// 1-by-1 through them.
		a.idleTicks = 0
		next := target + max(1, target/2)
		return min(next, a.Cfg.Max)

	case ready == 0 && active < target:
		// Idle capacity AND an empty queue. Only persistent idleness
		// (IdleAfter consecutive observations) earns a shrink, one step
		// at a time.
		a.idleTicks++
		if a.idleTicks >= a.Cfg.IdleAfter {
			a.idleTicks = 0
			return max(a.Cfg.Min, target-1)
		}
		return target

	default:
		// Working steadily: backlog below the growth bar but not idle.
		a.idleTicks = 0
		return target
	}
}

// Run applies Next every Interval until ctx ends.
func (a *Autoscaler) Run(ctx context.Context) {
	if a.Log == nil {
		a.Log = slog.Default()
	}
	t := time.NewTicker(a.Cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ready, err := a.Depths(dctx)
		cancel()
		if err != nil {
			continue // a blind scaler holds still; it does not guess
		}
		cur := a.Pool.Target()
		next := a.Next(ready, a.Pool.Active(), cur)
		if next != cur {
			a.Log.Info("autoscale: target changed", "from", cur, "to", next, "ready", ready)
			a.Pool.SetTarget(next)
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
