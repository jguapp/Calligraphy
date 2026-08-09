package control

// The control plane over a real gRPC connection on a loopback listener:
// worker connects, hello registers it, stats flow up, all four commands
// flow down and act on a fake pool. No external services -- this suite
// always runs.

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type fakePool struct {
	active, target atomic.Int64
	processed      atomic.Uint64
	paused         atomic.Bool
}

func (p *fakePool) Active() int       { return int(p.active.Load()) }
func (p *fakePool) Target() int       { return int(p.target.Load()) }
func (p *fakePool) Processed() uint64 { return p.processed.Load() }
func (p *fakePool) Paused() bool      { return p.paused.Load() }
func (p *fakePool) SetTarget(n int)   { p.target.Store(int64(n)) }
func (p *fakePool) SetPaused(v bool)  { p.paused.Store(v) }

func startHub(t *testing.T) (*Hub, string) {
	t.Helper()
	hub := NewHub(nil)
	srv := NewGRPCServer(hub)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)
	return hub, lis.Addr().String()
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for: " + msg)
}

func TestControlPlaneRoundTrip(t *testing.T) {
	hub, addr := startHub(t)

	pool := &fakePool{}
	pool.target.Store(4)
	pool.active.Store(2)
	pool.processed.Store(17)

	drained := make(chan struct{})
	client := &Client{
		Addr: addr, WorkerID: "w1", Types: []string{"bench.sleep"},
		Pool: pool, StatsEvery: 30 * time.Millisecond,
		OnDrain: func() { close(drained) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.Run(ctx)

	// Hello registers the worker; stats fill in shortly after.
	waitFor(t, 5*time.Second, func() bool { return len(hub.Workers()) == 1 }, "worker to register")
	waitFor(t, 5*time.Second, func() bool {
		ws := hub.Workers()
		return len(ws) == 1 && ws[0].Processed == 17 && ws[0].Active == 2
	}, "stats to arrive")

	w := hub.Workers()[0]
	if w.ID != "w1" || w.Types[0] != "bench.sleep" {
		t.Errorf("worker view = %+v", w)
	}

	// Commands land on the pool.
	if err := hub.SetConcurrency("w1", 9); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return pool.Target() == 9 }, "concurrency to apply")

	if err := hub.Pause("w1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return pool.Paused() }, "pause to apply")

	if err := hub.Resume("w1"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return !pool.Paused() }, "resume to apply")

	if err := hub.Drain("w1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("drain callback never fired")
	}

	// Unknown worker: a clean error, not a hang.
	if err := hub.SetConcurrency("nobody", 4); err == nil {
		t.Error("command to unknown worker succeeded")
	}
}

func TestClientReconnects(t *testing.T) {
	hub, addr := startHub(t)
	pool := &fakePool{}
	pool.target.Store(2)

	client := &Client{Addr: addr, WorkerID: "w1", Pool: pool, StatsEvery: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.Run(ctx)

	waitFor(t, 5*time.Second, func() bool { return len(hub.Workers()) == 1 }, "first connect")

	// Simulate a control-plane blip: sever every session server-side (as
	// a restart would). The client's stream dies and it reconnects on its
	// own -- which only works because eviction actively terminates the
	// stream; an earlier version just dropped the map entry and the
	// client never learned anything was wrong.
	hub.mu.Lock()
	for id, s := range hub.sessions {
		s.terminate()
		delete(hub.sessions, id)
	}
	hub.mu.Unlock()

	waitFor(t, 10*time.Second, func() bool { return len(hub.Workers()) == 1 }, "reconnect")
}
