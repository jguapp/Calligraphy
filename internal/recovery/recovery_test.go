package recovery

// The failure suite. The centerpiece is TestWorkerSIGKILLRecovery, which
// does exactly what the README claims survivable: starts a REAL worker in
// a child process, waits until it has genuinely claimed a job and started
// executing, kills it with SIGKILL (no drain, no goodbye, no ack), and
// then proves the platform notices, reclaims, retries, and completes the
// job on another worker -- with the whole story legible in the attempt
// history.
//
// The child is this same test binary re-executed with a flag env var
// (the stdlib's own helper-process pattern), so there's no separate
// fixture binary to build or drift.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jguapp/forge/internal/config"
	"github.com/jguapp/forge/internal/handler"
	"github.com/jguapp/forge/internal/job"
	"github.com/jguapp/forge/internal/queue"
	"github.com/jguapp/forge/internal/retry"
	"github.com/jguapp/forge/internal/store"
	"github.com/jguapp/forge/internal/worker"
)

func testDSNs(t *testing.T) (string, string) {
	t.Helper()
	dsn := os.Getenv("FORGE_TEST_DATABASE_URL")
	addr := os.Getenv("FORGE_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("FORGE_TEST_DATABASE_URL / FORGE_TEST_REDIS_ADDR not set")
	}
	return dsn, addr
}

func testConfig(dsn, addr string) config.Config {
	cfg, _ := config.Load()
	cfg.DatabaseURL = dsn
	cfg.RedisAddr = addr
	cfg.KeyPrefix = "forgetest"
	cfg.Concurrency = 2
	cfg.FetchBlock = 100 * time.Millisecond
	cfg.BatchInterval = 5 * time.Millisecond
	cfg.RetryBase = 10 * time.Millisecond
	cfg.RetryCap = 50 * time.Millisecond
	cfg.DrainTimeout = 2 * time.Second
	// Short leases keep the failure tests fast. Below config.Load's floor
	// on purpose -- the floor guards production configs, and these structs
	// never pass through Load's validation.
	cfg.LeaseTTL = time.Second
	cfg.ReapMinIdle = time.Second
	cfg.SweepGrace = 500 * time.Millisecond
	cfg.OrphanAge = 500 * time.Millisecond
	return cfg
}

func cleanSlate(t *testing.T, dsn, addr string) (*store.Store, *queue.Queue) {
	t.Helper()
	ctx := context.Background()
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
	q, err := queue.New(ctx, queue.Config{Addr: addr, Prefix: "forgetest", Queue: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.FlushForTest(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close(); q.Close() })
	return st, q
}

func startLeader(t *testing.T, st *store.Store, q *queue.Queue, cfg config.Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l := &Leader{
		Store: st, Queue: q, Cfg: cfg, ID: "test-leader",
		Backoff: retry.New(cfg.RetryBase, cfg.RetryCap),
	}
	go func() {
		l.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func startWorker(t *testing.T, cfg config.Config, reg *handler.Registry) {
	t.Helper()
	w, err := worker.New(context.Background(), cfg, reg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker never shut down")
		}
	})
}

func submit(t *testing.T, st *store.Store, q *queue.Queue, jobType, payload string, opts job.Options) *job.Job {
	t.Helper()
	ctx := context.Background()
	j, err := job.New(jobType, json.RawMessage(payload), opts, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	streamID, err := q.Enqueue(ctx, job.NewEnvelope(j, time.Now().UTC()), j.ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetEnqueued(ctx, j.ID, streamID); err != nil {
		t.Fatal(err)
	}
	return j
}

func waitStatus(t *testing.T, st *store.Store, id string, want job.Status, timeout time.Duration) *job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := st.GetJob(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == want {
			return j
		}
		time.Sleep(15 * time.Millisecond)
	}
	j, _ := st.GetJob(context.Background(), id)
	t.Fatalf("job %s never reached %s (stuck at %s, error=%q)", id, want, j.Status, j.Error)
	return nil
}

// ---------------------------------------------------------- the crash test

// TestHelperCrashWorker is not a test: it is the body of the child
// process. It runs a worker whose "crashme" handler marks a Redis key so
// the parent knows execution has genuinely begun, then blocks forever --
// until the parent SIGKILLs the whole process mid-job.
func TestHelperCrashWorker(t *testing.T) {
	if os.Getenv("FORGE_CRASH_HELPER") != "1" {
		t.Skip("helper process body, not a test")
	}
	dsn := os.Getenv("FORGE_TEST_DATABASE_URL")
	addr := os.Getenv("FORGE_TEST_REDIS_ADDR")
	cfg := testConfig(dsn, addr)
	cfg.WorkerID = "doomed-worker"

	q, err := queue.New(context.Background(), queue.Config{Addr: addr, Prefix: "forgetest", Queue: "default"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "crashme",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			// Tell the parent we're genuinely mid-execution, then hang.
			q.RequestCancel(ctx, "started:"+j.ID, time.Minute) // reused as a plain flag key
			select {}                                          // never returns; SIGKILL is the only exit
		}),
	})
	w, err := worker.New(context.Background(), cfg, reg, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	w.Run(context.Background()) // runs until killed
}

func TestWorkerSIGKILLRecovery(t *testing.T) {
	dsn, addr := testDSNs(t)
	st, q := cleanSlate(t, dsn, addr)
	cfg := testConfig(dsn, addr)

	// 1. Submit the job the child will die holding.
	j := submit(t, st, q, "crashme", `{}`, job.Options{MaxAttempts: 3})

	// 2. Start the doomed worker as a real OS process.
	child := exec.Command(os.Args[0], "-test.run=^TestHelperCrashWorker$", "-test.v=false")
	child.Env = append(os.Environ(), "FORGE_CRASH_HELPER=1")
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill() //nolint:errcheck // belt and braces on test exit

	// 3. Wait until the handler is provably executing (it raises a flag
	// key), so the kill lands mid-job, not mid-startup.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("child worker never started the job")
		}
		if on, _ := q.IsCancelRequested(context.Background(), "started:"+j.ID); on {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mid, _ := st.GetJob(context.Background(), j.ID)
	if mid.Status != job.StatusRunning || mid.WorkerID != "doomed-worker" {
		t.Fatalf("pre-kill state: status=%s worker=%s", mid.Status, mid.WorkerID)
	}

	// 4. SIGKILL. No drain, no ack, no goodbye.
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	child.Wait()

	// 5. Start the leader (reaper) and a healthy worker whose "crashme"
	// handler succeeds -- the recovered execution.
	startLeader(t, st, q, cfg)
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "crashme",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			return json.RawMessage(`{"survived":true}`), nil
		}),
	})
	cfg2 := cfg
	cfg2.WorkerID = "healthy-worker"
	startWorker(t, cfg2, reg)

	// 6. The platform's whole promise, asserted: the job completes.
	got := waitStatus(t, st, j.ID, job.StatusCompleted, 20*time.Second)
	if got.AttemptCount != 2 {
		t.Errorf("attempts = %d, want 2 (one died, one succeeded)", got.AttemptCount)
	}
	atts, _ := st.ListAttempts(context.Background(), j.ID)
	if len(atts) != 2 {
		t.Fatalf("attempt rows = %d, want 2: %+v", len(atts), atts)
	}
	if atts[0].Outcome != job.OutcomeLeaseExpired || atts[0].WorkerID != "doomed-worker" {
		t.Errorf("attempt 1 = %+v, want lease_expired by doomed-worker", atts[0])
	}
	if atts[1].Outcome != job.OutcomeCompleted || atts[1].WorkerID != "healthy-worker" {
		t.Errorf("attempt 2 = %+v, want completed by healthy-worker", atts[1])
	}
}

// ----------------------------------------------------------- other duties

func TestPromoterMovesScheduledRetries(t *testing.T) {
	dsn, addr := testDSNs(t)
	st, q := cleanSlate(t, dsn, addr)
	cfg := testConfig(dsn, addr)
	startLeader(t, st, q, cfg)

	// A transiently-failing handler: the full retry loop needs promoter +
	// worker together.
	first := true
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "failonce",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			if first {
				first = false
				return nil, fmt.Errorf("transient")
			}
			return json.RawMessage(`{}`), nil
		}),
	})
	startWorker(t, cfg, reg)

	j := submit(t, st, q, "failonce", `{}`, job.Options{})
	got := waitStatus(t, st, j.ID, job.StatusCompleted, 15*time.Second)
	if got.AttemptCount != 2 {
		t.Errorf("attempts = %d, want 2", got.AttemptCount)
	}
}

func TestScheduledJobRunsAtItsTime(t *testing.T) {
	dsn, addr := testDSNs(t)
	st, q := cleanSlate(t, dsn, addr)
	cfg := testConfig(dsn, addr)
	startLeader(t, st, q, cfg)

	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "later",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		}),
	})
	startWorker(t, cfg, reg)

	runAt := time.Now().UTC().Add(600 * time.Millisecond)
	j := submit(t, st, q, "later", `{}`, job.Options{ScheduledAt: runAt})

	// Still pending before its time (with margin for the promoter tick).
	time.Sleep(200 * time.Millisecond)
	if cur, _ := st.GetJob(context.Background(), j.ID); cur.Status != job.StatusPending {
		t.Fatalf("scheduled job ran early: %s", cur.Status)
	}
	got := waitStatus(t, st, j.ID, job.StatusCompleted, 10*time.Second)
	if got.CompletedAt.Before(runAt) {
		t.Errorf("completed %v before its scheduled time %v", got.CompletedAt, runAt)
	}
}

func TestOrphanedSubmitIsRepaired(t *testing.T) {
	dsn, addr := testDSNs(t)
	st, q := cleanSlate(t, dsn, addr)
	cfg := testConfig(dsn, addr)

	// Simulate "INSERT succeeded, XADD didn't": a row with no enqueue.
	ctx := context.Background()
	j, _ := job.New("orphan", json.RawMessage(`{}`), job.Options{}, time.Now().UTC().Add(-time.Minute))
	j.ScheduledAt = time.Now().UTC().Add(-time.Minute)
	if _, err := st.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}

	startLeader(t, st, q, cfg)
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "orphan",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		}),
	})
	startWorker(t, cfg, reg)

	waitStatus(t, st, j.ID, job.StatusCompleted, 20*time.Second)
}

func TestStuckRetryIsRepaired(t *testing.T) {
	dsn, addr := testDSNs(t)
	st, q := cleanSlate(t, dsn, addr)
	cfg := testConfig(dsn, addr)

	// Simulate "DB says RETRYING, ZADD never landed": claim then schedule
	// a retry in the past, with no delayed-set member.
	ctx := context.Background()
	j := submit(t, st, q, "stuck", `{}`, job.Options{})
	// Drain the enqueued entry so no delivery exists at all.
	got, _ := q.Fetch(ctx, "drainer", 10, 0)
	for _, d := range got {
		q.Ack(ctx, d.Priority, d.EntryID)
	}
	claim, ok, _ := st.ClaimJob(ctx, j.ID, "w-old", time.Second)
	if !ok {
		t.Fatal("claim failed")
	}
	if ok, _ := st.ScheduleRetry(ctx, j.ID, claim.Epoch, "boom", time.Now().UTC().Add(-time.Minute)); !ok {
		t.Fatal("schedule retry failed")
	}

	startLeader(t, st, q, cfg)
	reg := handler.NewRegistry()
	reg.MustRegister(handler.Registration{
		Type: "stuck",
		Handler: handler.Func(func(ctx context.Context, j *job.Job) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		}),
	})
	startWorker(t, cfg, reg)

	final := waitStatus(t, st, j.ID, job.StatusCompleted, 20*time.Second)
	if final.AttemptCount != 2 {
		t.Errorf("attempts = %d, want 2", final.AttemptCount)
	}
}

// TestReapExhaustionDeadLetters: the worker dies on a job's LAST attempt;
// the reaper must route it to the DLQ, not schedule attempt N+1.
func TestReapExhaustionDeadLetters(t *testing.T) {
	dsn, addr := testDSNs(t)
	st, q := cleanSlate(t, dsn, addr)
	cfg := testConfig(dsn, addr)

	ctx := context.Background()
	j := submit(t, st, q, "doomed", `{}`, job.Options{MaxAttempts: 1})
	// Claim it as a worker that will never report back, with an instantly
	// expired lease; leave the PEL entry unacked, exactly like a crash.
	deliveries, _ := q.Fetch(ctx, "vanished-worker", 10, 0)
	if len(deliveries) != 1 {
		t.Fatalf("deliveries = %d", len(deliveries))
	}
	if _, ok, _ := st.ClaimJob(ctx, j.ID, "vanished-worker", -time.Second); !ok {
		t.Fatal("claim failed")
	}

	startLeader(t, st, q, cfg)
	got := waitStatus(t, st, j.ID, job.StatusDeadLetter, 15*time.Second)
	if got.AttemptCount != 1 {
		t.Errorf("attempts = %d, want 1", got.AttemptCount)
	}
	entries, _ := q.ListDLQ(ctx, 10)
	if len(entries) != 1 || entries[0].Env.ID != j.ID {
		t.Errorf("dlq log = %+v", entries)
	}
}
