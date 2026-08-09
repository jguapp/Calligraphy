package queue

// Integration tests against a real Redis, env-gated on
// CALIGRAPHY_TEST_REDIS_ADDR (same pattern as the store tests). Each test gets
// a fresh database via FLUSHDB -- the test Redis is dedicated.

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jguapp/caligraphy/internal/job"
)

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	addr := os.Getenv("CALIGRAPHY_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("CALIGRAPHY_TEST_REDIS_ADDR not set; skipping queue integration tests")
	}
	ctx := context.Background()
	q, err := New(ctx, Config{Addr: addr, Prefix: "caligraphytest", Queue: "default"})
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := q.rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func testEnv(id string, p job.Priority) job.Envelope {
	return job.Envelope{
		ID: id, Type: "test.job", Queue: "default", Priority: p, Attempt: 1,
		Payload: json.RawMessage(`{"n":1}`), EnqueuedAt: time.Now().UTC(),
	}
}

func TestEnqueueFetchAckLifecycle(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	streamID, err := q.Enqueue(ctx, testEnv("j1", job.PriorityDefault), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if streamID == "" || streamID == "delayed" {
		t.Errorf("streamID = %q, want a real entry id", streamID)
	}

	got, err := q.Fetch(ctx, "w1", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Env.ID != "j1" || got[0].EntryID != streamID {
		t.Fatalf("fetch = %+v", got)
	}

	// In flight: XLEN 1, pending 1, ready 0.
	d, err := q.Depths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.InFlight != 1 || d.TotalReady() != 0 {
		t.Errorf("depths mid-flight = %+v", d)
	}

	if err := q.Ack(ctx, got[0].Priority, got[0].EntryID); err != nil {
		t.Fatal(err)
	}
	d, _ = q.Depths(ctx)
	if d.InFlight != 0 || d.TotalReady() != 0 {
		t.Errorf("depths after ack = %+v (entry should be acked AND deleted)", d)
	}

	// Nothing left to fetch.
	got, _ = q.Fetch(ctx, "w1", 10, 0)
	if len(got) != 0 {
		t.Errorf("fetched %d after ack", len(got))
	}
}

func TestPriorityOrdering(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	q.Enqueue(ctx, testEnv("low1", job.PriorityLow), time.Now())
	q.Enqueue(ctx, testEnv("def1", job.PriorityDefault), time.Now())
	q.Enqueue(ctx, testEnv("high1", job.PriorityHigh), time.Now())
	q.Enqueue(ctx, testEnv("high2", job.PriorityHigh), time.Now())

	got, err := q.Fetch(ctx, "w1", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("fetched %d, want 3", len(got))
	}
	wantOrder := []string{"high1", "high2", "def1"}
	for i, w := range wantOrder {
		if got[i].Env.ID != w {
			t.Errorf("position %d = %s, want %s", i, got[i].Env.ID, w)
		}
	}
}

func TestDelayedEnqueueAndPromote(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	streamID, err := q.Enqueue(ctx, testEnv("j1", job.PriorityDefault), time.Now().Add(50*time.Millisecond))
	if err != nil || streamID != "delayed" {
		t.Fatalf("delayed enqueue: %v id=%q", err, streamID)
	}

	// Not fetchable yet; not promotable yet.
	if got, _ := q.Fetch(ctx, "w1", 10, 0); len(got) != 0 {
		t.Fatalf("fetched a delayed job early: %+v", got)
	}
	if promoted, _ := q.PromoteDue(ctx, 100); len(promoted) != 0 {
		t.Fatalf("promoted early: %+v", promoted)
	}

	time.Sleep(60 * time.Millisecond)
	promoted, err := q.PromoteDue(ctx, 100)
	if err != nil || len(promoted) != 1 || promoted[0].ID != "j1" {
		t.Fatalf("promote: %v %+v", err, promoted)
	}
	// Promotion is atomic: nothing left in the zset, entry on the stream.
	d, _ := q.Depths(ctx)
	if d.Delayed[job.PriorityDefault] != 0 || d.Ready[job.PriorityDefault] != 1 {
		t.Errorf("depths after promote = %+v", d)
	}

	got, _ := q.Fetch(ctx, "w1", 10, 0)
	if len(got) != 1 || got[0].Env.ID != "j1" {
		t.Errorf("fetch after promote = %+v", got)
	}
}

func TestReapAbandonedRespectsHeartbeat(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	q.Enqueue(ctx, testEnv("dead", job.PriorityDefault), time.Now())
	q.Enqueue(ctx, testEnv("alive", job.PriorityDefault), time.Now())

	got, _ := q.Fetch(ctx, "w1", 10, 0)
	if len(got) != 2 {
		t.Fatalf("fetched %d", len(got))
	}
	byID := map[string]Delivery{}
	for _, d := range got {
		byID[d.Env.ID] = d
	}

	// Let both entries age past minIdle, but heartbeat "alive" -- its idle
	// clock resets, "dead" keeps aging. This is exactly the crashed-worker
	// vs long-running-job distinction.
	time.Sleep(120 * time.Millisecond)
	if err := q.Heartbeat(ctx, "w1", byID["alive"].Priority, byID["alive"].EntryID); err != nil {
		t.Fatal(err)
	}

	reaped, err := q.ReapAbandoned(ctx, 100*time.Millisecond, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped) != 1 || reaped[0].Env.ID != "dead" {
		ids := []string{}
		for _, r := range reaped {
			ids = append(ids, r.Env.ID)
		}
		t.Fatalf("reaped %v, want just [dead]", ids)
	}
	// Reaper acks what it reclaimed once the DB decision is recorded.
	if err := q.Ack(ctx, reaped[0].Priority, reaped[0].EntryID); err != nil {
		t.Fatal(err)
	}
	d, _ := q.Depths(ctx)
	if d.InFlight != 1 {
		t.Errorf("in-flight after reap+ack = %d, want 1 (the alive one)", d.InFlight)
	}
}

func TestDeadLetterLog(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	env := testEnv("j1", job.PriorityDefault)
	env.Attempt = 5
	if err := q.DeadLetter(ctx, env, "gave up after 5 attempts"); err != nil {
		t.Fatal(err)
	}
	entries, err := q.ListDLQ(ctx, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("dlq: %v n=%d", err, len(entries))
	}
	if entries[0].Env.ID != "j1" || entries[0].Error != "gave up after 5 attempts" || entries[0].Env.Attempt != 5 {
		t.Errorf("dlq entry = %+v", entries[0])
	}
	d, _ := q.Depths(ctx)
	if d.DLQ != 1 {
		t.Errorf("dlq depth = %d", d.DLQ)
	}
}

func TestCancelFlag(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if on, _ := q.IsCancelRequested(ctx, "j1"); on {
		t.Error("cancel flag set before request")
	}
	if err := q.RequestCancel(ctx, "j1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if on, _ := q.IsCancelRequested(ctx, "j1"); !on {
		t.Error("cancel flag not observed")
	}
}

func TestLeaderLock(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	ok, err := q.AcquireLeader(ctx, "recovery", "node-a", 200*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("first acquire: %v ok=%v", err, ok)
	}
	// Contender loses while the lock is held.
	if ok, _ := q.AcquireLeader(ctx, "recovery", "node-b", 200*time.Millisecond); ok {
		t.Error("second acquire succeeded while held")
	}
	// Holder renews; contender's renew is refused.
	if ok, _ := q.RenewLeader(ctx, "recovery", "node-a", 200*time.Millisecond); !ok {
		t.Error("holder renew refused")
	}
	if ok, _ := q.RenewLeader(ctx, "recovery", "node-b", 200*time.Millisecond); ok {
		t.Error("non-holder renew succeeded")
	}
	// Non-holder release is a no-op; holder release frees it.
	q.ReleaseLeader(ctx, "recovery", "node-b")
	if ok, _ := q.AcquireLeader(ctx, "recovery", "node-b", time.Second); ok {
		t.Error("release by non-holder actually released")
	}
	q.ReleaseLeader(ctx, "recovery", "node-a")
	if ok, _ := q.AcquireLeader(ctx, "recovery", "node-b", time.Second); !ok {
		t.Error("acquire after release failed")
	}
}

func TestPoisonEntryAckedAway(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	// Inject garbage directly, as if a buggy producer wrote it.
	err := q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.streamKey(job.PriorityDefault),
		Values: map[string]any{"j": "{not json"},
	}).Err()
	if err != nil {
		t.Fatal(err)
	}
	q.Enqueue(ctx, testEnv("good", job.PriorityDefault), time.Now())

	got, err := q.Fetch(ctx, "w1", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Env.ID != "good" {
		t.Fatalf("fetch around poison = %+v", got)
	}
	// The poison entry is gone, not pending forever.
	d, _ := q.Depths(ctx)
	if d.InFlight != 1 {
		t.Errorf("in-flight = %d, want 1 (poison must not linger)", d.InFlight)
	}
}

func TestBlockingFetchWakesOnEnqueue(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	done := make(chan []Delivery, 1)
	go func() {
		got, _ := q.Fetch(ctx, "w1", 5, 2*time.Second)
		done <- got
	}()
	time.Sleep(50 * time.Millisecond) // let the fetch block
	q.Enqueue(ctx, testEnv("j1", job.PriorityHigh), time.Now())

	select {
	case got := <-done:
		if len(got) != 1 || got[0].Env.ID != "j1" {
			t.Errorf("blocking fetch = %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking fetch never woke")
	}
}
