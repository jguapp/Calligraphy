// Package worker is Forge's execution engine: a bounded pool of goroutines
// fed from the queue, with live resizing, graceful drain, and the runner
// that walks one delivery through claim -> execute -> record -> ack.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jguapp/forge/internal/queue"
)

// FetchFunc claims up to max deliveries. It is expected to block briefly
// (the queue's blocking read) when nothing is ready, so the pool loop
// doesn't spin.
type FetchFunc func(ctx context.Context, max int) ([]queue.Delivery, error)

// RunFunc executes one delivery to completion (including recording and
// acking). It must respect ctx.
type RunFunc func(ctx context.Context, d queue.Delivery)

// Pool runs at most `target` deliveries concurrently.
//
// The concurrency bound and the backpressure story are the same mechanism:
// the fetcher computes free = target - active before every fetch and never
// asks Redis for more than that. Claimed-but-unstarted work therefore
// cannot pile up inside the process; backlog stays in Redis, where it is
// visible (queue depth), durable, and claimable by other workers. There is
// no internal buffer channel to fill, and no way to create more than
// `target` goroutines -- the loop is the limiter.
//
// Resizing is a plain atomic store. The loop re-reads target every
// iteration: shrinking simply stops new fetches until enough jobs drain,
// growing takes effect on the next fetch. Nothing in flight is disturbed.
type Pool struct {
	fetch    FetchFunc
	run      RunFunc
	batchMax int
	log      *slog.Logger

	target atomic.Int64
	active atomic.Int64
	paused atomic.Bool
	// processed counts finished deliveries, for heartbeats and stats.
	processed atomic.Uint64

	// completions nudges the fetch loop awake when a slot frees. Buffered
	// so a finishing job never blocks on notifying; if the buffer is full
	// the loop is about to wake anyway.
	completions chan struct{}
	wg          sync.WaitGroup
}

func NewPool(fetch FetchFunc, run RunFunc, initial, batchMax int, log *slog.Logger) *Pool {
	if log == nil {
		log = slog.Default()
	}
	p := &Pool{
		fetch:       fetch,
		run:         run,
		batchMax:    batchMax,
		log:         log,
		completions: make(chan struct{}, 256),
	}
	p.target.Store(int64(initial))
	return p
}

func (p *Pool) Target() int       { return int(p.target.Load()) }
func (p *Pool) Active() int       { return int(p.active.Load()) }
func (p *Pool) Processed() uint64 { return p.processed.Load() }
func (p *Pool) SetTarget(n int)   { p.target.Store(int64(max(1, n))) }
func (p *Pool) SetPaused(v bool)  { p.paused.Store(v) }
func (p *Pool) Paused() bool      { return p.paused.Load() }

// Run fetches and dispatches until fetchCtx is cancelled, then waits for
// every in-flight job. The two contexts implement two-phase shutdown:
// cancelling fetchCtx stops new work (drain begins); cancelling jobCtx
// tells still-running handlers to wrap up. The caller (Worker.Run) owns
// the timing between the two.
func (p *Pool) Run(fetchCtx, jobCtx context.Context) {
	// Escalating pause after fetch errors: a down Redis must not be hit
	// in a hot loop, and must also not be waited on forever.
	errBackoff := time.Duration(0)

	for fetchCtx.Err() == nil {
		if p.paused.Load() {
			select {
			case <-fetchCtx.Done():
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}

		free := int(p.target.Load() - p.active.Load())
		if free <= 0 {
			// Full. Sleep until a completion, a resize tick, or shutdown.
			select {
			case <-fetchCtx.Done():
			case <-p.completions:
			case <-time.After(250 * time.Millisecond):
			}
			continue
		}

		n := free
		if n > p.batchMax {
			n = p.batchMax
		}
		deliveries, err := p.fetch(fetchCtx, n)
		if err != nil {
			if fetchCtx.Err() != nil {
				break
			}
			if errBackoff < 5*time.Second {
				errBackoff += 500 * time.Millisecond
			}
			p.log.Warn("worker: fetch failed, backing off", "err", err, "backoff", errBackoff)
			select {
			case <-fetchCtx.Done():
			case <-time.After(errBackoff):
			}
			continue
		}
		errBackoff = 0

		for _, d := range deliveries {
			d := d
			p.active.Add(1)
			p.wg.Add(1)
			go func() {
				defer func() {
					p.active.Add(-1)
					p.processed.Add(1)
					p.wg.Done()
					select {
					case p.completions <- struct{}{}:
					default:
					}
				}()
				p.run(jobCtx, d)
			}()
		}
	}
	p.wg.Wait()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
