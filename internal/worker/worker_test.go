package worker

// End-to-end integration: real Postgres, real Redis, a real Worker
// running its pool -- driven through the same submit path the API will
// use. Env-gated like the store and queue suites.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jguapp/calligraphy/internal/config"
	"github.com/jguapp/calligraphy/internal/handler"
	"github.com/jguapp/calligraphy/internal/job"
	"github.com/jguapp/calligraphy/internal/queue"
	"github.com/jguapp/calligraphy/internal/store"
)

type testRig struct {
	store *store.Store
	queue *queue.Queue
	w     *Worker
	stop  context.CancelFunc
	done  chan struct{}
}

func startRig(t *testing.T, reg *handler.Registry, mutate func(*config.Config)) *testRig {
	t.Helper()
	dsn := os.Getenv("CALLIGRAPHY_TEST_DATABASE_URL")
	addr := os.Getenv("CALLIGRAPHY_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("CALLIGRAPHY_TEST_DATABASE_URL / CALLIGRAPHY_TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.DatabaseURL = dsn
	cfg.RedisAddr = addr
	cfg.KeyPrefix = "calligraphytest"
	cfg.WorkerID = "test-worker"
	cfg.Concurrency = 4
	cfg.FetchBlock = 100 * time.Millisecond
	cfg.BatchInterval = 5 * time.Millisecond
	cfg.RetryBase = 10 * time.Millisecond
	cfg.RetryCap = 50 * time.Millisecond
	cfg.DrainTimeout = 2 * time.Second
	if mutate != nil {
		mutate(&cfg)
	}

	// Clean slate on both sides.
	st, err := store.Open(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.TruncateForTest(ctx); err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(ctx, queue.Config{Addr: addr, Prefix: "calligraphytest", Queue: cfg.Queue})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.FlushForTest(ctx); err != nil {
		t.Fatal(err)
	}

	w, err := New(ctx, cfg, reg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(runCtx)
		close(done)
	}()

	rig := &testRig{store: st, queue: q, w: w, stop: stop, done: done}
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker never shut down")
		}
		st.Close()
		q.Close()
	})
	return rig
}

// submit mirrors what the API will do: create the row, enqueue the
// envelope, record the handoff.
func (r *testRig) submit(t *testing.T, jobType string, payload string, opts job.Options) *job.Job {
	t.Helper()
	ctx := context.Background()
	j, err := job.New(jobType, json.RawMessage(payload), opts, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.store.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	streamID, err := r.queue.Enqueue(ctx, job.NewEnvelope(j, time.Now().UTC()), j.ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.store.SetEnqueued(ctx, j.ID, streamID); err != nil {
		t.Fatal(err)
	}
	return j
}

func (r *testRig) waitStatus(t *testing.T, id string, want job.Status, timeout time.Duration) *job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := r.store.GetJob(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == want {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := r.store.GetJob(context.Background(), id)
	t.Fatalf("job %s never reached %s (stuck at %s, error=%q)", id, want, j.Status, j.Error)
	return nil
}

func TestEndToEndCompletion(t *testing.T) {
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "echo",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			return json.RawMessage(`{"echoed":true}`), nil
		}),
	})
	rig := startRig(t, reg, nil)

	j := rig.submit(t, "echo", `{"n":1}`, job.Options{})
	got := rig.waitStatus(t, j.ID, job.StatusCompleted, 5*time.Second)

	if string(got.Result) != `{"echoed": true}` && string(got.Result) != `{"echoed":true}` {
		t.Errorf("result = %s", got.Result)
	}
	if got.AttemptCount != 1 {
		t.Errorf("attempts = %d, want 1", got.AttemptCount)
	}
	atts, _ := rig.store.ListAttempts(context.Background(), j.ID)
	if len(atts) != 1 || atts[0].Outcome != job.OutcomeCompleted || atts[0].WorkerID != "test-worker" {
		t.Errorf("attempt history = %+v", atts)
	}
	// The entry must be acked AND deleted: nothing in flight, nothing ready.
	d, _ := rig.queue.Depths(context.Background())
	if d.InFlight != 0 || d.TotalReady() != 0 {
		t.Errorf("queue depths after completion = %+v", d)
	}
}

func TestTransientFailureRetriesToSuccess(t *testing.T) {
	var calls atomic.Int64
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "flaky2",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			if calls.Add(1) < 3 {
				return nil, fmt.Errorf("transient %d", calls.Load())
			}
			return json.RawMessage(`{"ok":true}`), nil
		}),
	})
	rig := startRig(t, reg, nil)

	// The retry path needs the promoter, which lives in the recovery
	// package (next PR). Until then the test promotes manually -- same
	// call recovery will make.
	stopPromote := make(chan struct{})
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopPromote:
				return
			case <-tk.C:
				ctx := context.Background()
				envs, _ := rig.queue.PromoteDue(ctx, 100)
				for _, e := range envs {
					rig.store.MarkPromoted(ctx, []string{e.ID})
				}
			}
		}
	}()
	defer close(stopPromote)

	j := rig.submit(t, "flaky2", `{}`, job.Options{MaxAttempts: 5})
	got := rig.waitStatus(t, j.ID, job.StatusCompleted, 10*time.Second)

	if got.AttemptCount != 3 {
		t.Errorf("attempts = %d, want 3", got.AttemptCount)
	}
	atts, _ := rig.store.ListAttempts(context.Background(), j.ID)
	if len(atts) != 3 {
		t.Fatalf("attempt rows = %d, want 3", len(atts))
	}
	for i, want := range []job.Outcome{job.OutcomeRetryable, job.OutcomeRetryable, job.OutcomeCompleted} {
		if atts[i].Outcome != want {
			t.Errorf("attempt %d outcome = %s, want %s", i+1, atts[i].Outcome, want)
		}
	}
}

func TestExhaustedRetriesDeadLetter(t *testing.T) {
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "alwaysfails",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			return nil, errors.New("nope, forever")
		}),
	})
	rig := startRig(t, reg, nil)

	stopPromote := make(chan struct{})
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopPromote:
				return
			case <-tk.C:
				ctx := context.Background()
				envs, _ := rig.queue.PromoteDue(ctx, 100)
				for _, e := range envs {
					rig.store.MarkPromoted(ctx, []string{e.ID})
				}
			}
		}
	}()
	defer close(stopPromote)

	j := rig.submit(t, "alwaysfails", `{}`, job.Options{MaxAttempts: 3})
	got := rig.waitStatus(t, j.ID, job.StatusDeadLetter, 10*time.Second)

	if got.AttemptCount != 3 {
		t.Errorf("attempts = %d, want 3", got.AttemptCount)
	}
	// The DLQ log stream has the corpse too.
	entries, _ := rig.queue.ListDLQ(context.Background(), 10)
	if len(entries) != 1 || entries[0].Env.ID != j.ID {
		t.Errorf("dlq log = %+v", entries)
	}
}

func TestNonRetryableFailsImmediately(t *testing.T) {
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "badinput",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			return nil, job.NonRetryable(errors.New("payload is garbage"))
		}),
	})
	rig := startRig(t, reg, nil)

	j := rig.submit(t, "badinput", `{}`, job.Options{MaxAttempts: 5})
	got := rig.waitStatus(t, j.ID, job.StatusFailed, 5*time.Second)

	if got.AttemptCount != 1 {
		t.Errorf("attempts = %d, want exactly 1 (non-retryable must not retry)", got.AttemptCount)
	}
}

func TestPanickingHandlerIsContained(t *testing.T) {
	var calls atomic.Int64
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "panicky",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			return json.RawMessage(`{}`), nil
		}),
	})
	rig := startRig(t, reg, nil)

	stopPromote := make(chan struct{})
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopPromote:
				return
			case <-tk.C:
				ctx := context.Background()
				envs, _ := rig.queue.PromoteDue(ctx, 100)
				for _, e := range envs {
					rig.store.MarkPromoted(ctx, []string{e.ID})
				}
			}
		}
	}()
	defer close(stopPromote)

	j := rig.submit(t, "panicky", `{}`, job.Options{})
	rig.waitStatus(t, j.ID, job.StatusCompleted, 10*time.Second)

	atts, _ := rig.store.ListAttempts(context.Background(), j.ID)
	if len(atts) != 2 || atts[0].Outcome != job.OutcomePanicked {
		t.Errorf("attempts = %+v, want panicked then completed", atts)
	}
}

func TestUnknownTypeFailsPermanently(t *testing.T) {
	rig := startRig(t, handler.NewRegistry(), nil)
	j := rig.submit(t, "nobody.home", `{}`, job.Options{})
	got := rig.waitStatus(t, j.ID, job.StatusFailed, 5*time.Second)
	if got.AttemptCount != 1 {
		t.Errorf("attempts = %d", got.AttemptCount)
	}
}

func TestCancelRequestedMidRun(t *testing.T) {
	started := make(chan struct{}, 1)
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "slow",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			started <- struct{}{}
			<-ctx.Done() // waits for the cooperative cancel
			return nil, ctx.Err()
		}),
	})
	// Short lease so the heartbeat (lease/3) polls the cancel flag fast.
	rig := startRig(t, reg, func(c *config.Config) { c.LeaseTTL = 6 * time.Second })

	j := rig.submit(t, "slow", `{}`, job.Options{})
	<-started
	if err := rig.queue.RequestCancel(context.Background(), j.ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	got := rig.waitStatus(t, j.ID, job.StatusCancelled, 10*time.Second)
	if got.Status != job.StatusCancelled {
		t.Errorf("status = %s", got.Status)
	}
}

func TestGracefulDrainMidJob(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "holdme",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			started <- struct{}{}
			select {
			case <-release:
				return json.RawMessage(`{"finished":"gracefully"}`), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}),
	})
	rig := startRig(t, reg, nil)

	j := rig.submit(t, "holdme", `{}`, job.Options{})
	<-started

	// SIGTERM arrives while the job runs. Drain must let it finish.
	rig.stop()
	time.Sleep(50 * time.Millisecond) // fetch stopped, job still alive
	close(release)

	select {
	case <-rig.done:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not exit after drain")
	}
	got, err := rig.store.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != job.StatusCompleted {
		t.Errorf("status after drain = %s, want COMPLETED (drain must not kill in-flight work)", got.Status)
	}
}
