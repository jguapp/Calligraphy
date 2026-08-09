package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jguapp/forge/internal/job"
	"github.com/jguapp/forge/internal/queue"
)

// fakeFeed produces synthetic deliveries and records how many were asked
// for at once -- the pool's honesty about free slots is the whole test.
type fakeFeed struct {
	mu        sync.Mutex
	produced  int
	maxAsked  int
	remaining int
	blockFor  time.Duration
}

func (f *fakeFeed) fetch(ctx context.Context, max int) ([]queue.Delivery, error) {
	f.mu.Lock()
	if max > f.maxAsked {
		f.maxAsked = max
	}
	n := max
	if n > f.remaining {
		n = f.remaining
	}
	f.remaining -= n
	start := f.produced
	f.produced += n
	f.mu.Unlock()

	if n == 0 {
		// Mimic the queue's blocking read on empty.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.blockFor):
			return nil, nil
		}
	}
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = queue.Delivery{
			Env:     job.Envelope{ID: fmt.Sprintf("j%d", start+i), Type: "t", Payload: json.RawMessage(`{}`)},
			EntryID: fmt.Sprintf("0-%d", start+i),
		}
	}
	return out, nil
}

func TestPoolBoundsConcurrency(t *testing.T) {
	const target = 4
	feed := &fakeFeed{remaining: 100, blockFor: 10 * time.Millisecond}

	var cur, peak atomic.Int64
	var done atomic.Int64
	run := func(ctx context.Context, d queue.Delivery) {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		cur.Add(-1)
		done.Add(1)
	}

	p := NewPool(feed.fetch, run, target, 8, nil)
	fetchCtx, cancel := context.WithCancel(context.Background())
	go func() {
		for done.Load() < 100 {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()
	p.Run(fetchCtx, context.Background())

	if done.Load() != 100 {
		t.Fatalf("processed %d, want 100", done.Load())
	}
	if got := peak.Load(); got > target {
		t.Errorf("peak concurrency %d exceeded target %d", got, target)
	}
	if feed.maxAsked > target {
		t.Errorf("fetch asked for %d at once; must never exceed free slots (%d)", feed.maxAsked, target)
	}
}

func TestPoolResizeTakesEffectLive(t *testing.T) {
	feed := &fakeFeed{remaining: 200, blockFor: 5 * time.Millisecond}
	var cur, peakAfter atomic.Int64
	var resized atomic.Bool
	var done atomic.Int64

	run := func(ctx context.Context, d queue.Delivery) {
		n := cur.Add(1)
		if resized.Load() {
			for {
				p := peakAfter.Load()
				if n <= p || peakAfter.CompareAndSwap(p, n) {
					break
				}
			}
		}
		time.Sleep(3 * time.Millisecond)
		cur.Add(-1)
		done.Add(1)
	}

	p := NewPool(feed.fetch, run, 8, 8, nil)
	fetchCtx, cancel := context.WithCancel(context.Background())
	go func() {
		for done.Load() < 50 {
			time.Sleep(time.Millisecond)
		}
		p.SetTarget(2)
		// Let in-flight work drain below the new target before measuring.
		for cur.Load() > 2 {
			time.Sleep(time.Millisecond)
		}
		resized.Store(true)
		for done.Load() < 200 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()
	p.Run(fetchCtx, context.Background())

	if got := peakAfter.Load(); got > 2 {
		t.Errorf("post-resize peak = %d, want <= 2", got)
	}
	if p.Target() != 2 {
		t.Errorf("target = %d", p.Target())
	}
}

func TestPoolDrainFinishesInFlight(t *testing.T) {
	feed := &fakeFeed{remaining: 8, blockFor: 5 * time.Millisecond}
	var started, finished atomic.Int64
	release := make(chan struct{})

	run := func(ctx context.Context, d queue.Delivery) {
		started.Add(1)
		<-release
		finished.Add(1)
	}

	p := NewPool(feed.fetch, run, 4, 4, nil)
	fetchCtx, cancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() {
		p.Run(fetchCtx, context.Background())
		close(poolDone)
	}()

	for started.Load() < 4 {
		time.Sleep(time.Millisecond)
	}
	cancel() // drain begins: no new fetches, in-flight keeps running

	select {
	case <-poolDone:
		t.Fatal("pool exited while jobs still running")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case <-poolDone:
	case <-time.After(time.Second):
		t.Fatal("pool never drained after jobs finished")
	}
	if finished.Load() != started.Load() {
		t.Errorf("started %d finished %d", started.Load(), finished.Load())
	}
}

func TestPoolPauseStopsFetching(t *testing.T) {
	feed := &fakeFeed{remaining: 100, blockFor: 2 * time.Millisecond}
	var done atomic.Int64
	run := func(ctx context.Context, d queue.Delivery) { done.Add(1) }

	p := NewPool(feed.fetch, run, 4, 4, nil)
	p.SetPaused(true)
	fetchCtx, cancel := context.WithCancel(context.Background())
	go p.Run(fetchCtx, context.Background())

	time.Sleep(50 * time.Millisecond)
	if done.Load() != 0 {
		t.Errorf("paused pool processed %d jobs", done.Load())
	}
	p.SetPaused(false)
	for done.Load() < 100 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
}

func TestPoolBacksOffOnFetchErrors(t *testing.T) {
	var calls atomic.Int64
	fetch := func(ctx context.Context, max int) ([]queue.Delivery, error) {
		calls.Add(1)
		return nil, fmt.Errorf("redis is on fire")
	}
	p := NewPool(fetch, func(context.Context, queue.Delivery) {}, 4, 4, nil)
	fetchCtx, cancel := context.WithCancel(context.Background())
	go p.Run(fetchCtx, context.Background())

	time.Sleep(300 * time.Millisecond)
	cancel()
	// With escalating backoff (500ms, 1s, ...) 300ms allows at most a
	// couple of calls; a hot loop would be in the thousands.
	if n := calls.Load(); n > 3 {
		t.Errorf("fetch called %d times in 300ms; backoff not working", n)
	}
}
