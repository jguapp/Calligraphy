package store

// Integration tests against a real Postgres. Env-gated, not build-tag
// gated: with no CALIGRAPHY_TEST_DATABASE_URL set the whole file skips, so a
// bare `go test ./...` stays green anywhere. `make test-integration` (and
// CI) provide the DSN.

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jguapp/caligraphy/internal/job"
)

// jsonEqual compares JSON semantically: jsonb canonicalizes formatting
// (whitespace, key order), so byte comparison against what was submitted
// would test Postgres's pretty-printer, not our storage.
func jsonEqual(t *testing.T, got json.RawMessage, want string) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		return false
	}
	return reflect.DeepEqual(g, w)
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("CALIGRAPHY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CALIGRAPHY_TEST_DATABASE_URL not set; skipping store integration tests")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, 4)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `TRUNCATE jobs, job_attempts, job_events, workers`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

func mustCreate(t *testing.T, s *Store, opts job.Options) *job.Job {
	t.Helper()
	j, err := job.New("test.job", json.RawMessage(`{"n":1}`), opts, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.CreateJob(context.Background(), j)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created {
		t.Fatal("expected fresh create")
	}
	return j
}

func TestCreateAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{Priority: job.PriorityHigh, MaxAttempts: 3})

	got, err := s.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != job.StatusPending || got.Priority != job.PriorityHigh || got.MaxAttempts != 3 {
		t.Errorf("got %+v", got)
	}
	if !jsonEqual(t, got.Payload, `{"n":1}`) {
		t.Errorf("payload = %s", got.Payload)
	}
	if _, err := s.GetJob(ctx, "nope"); err != ErrNotFound {
		t.Errorf("missing job err = %v, want ErrNotFound", err)
	}
}

func TestIdempotentCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	j1, _ := job.New("t", nil, job.Options{IdempotencyKey: "article-42"}, time.Now().UTC())
	r1, err := s.CreateJob(ctx, j1)
	if err != nil || !r1.Created {
		t.Fatalf("first create: %v created=%v", err, r1.Created)
	}

	// Same (type, key): must return the ORIGINAL job, not create.
	j2, _ := job.New("t", nil, job.Options{IdempotencyKey: "article-42"}, time.Now().UTC())
	r2, err := s.CreateJob(ctx, j2)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Created || r2.Job.ID != j1.ID {
		t.Errorf("dedup failed: created=%v id=%s want %s", r2.Created, r2.Job.ID, j1.ID)
	}

	// Different type, same key: no collision.
	j3, _ := job.New("other", nil, job.Options{IdempotencyKey: "article-42"}, time.Now().UTC())
	r3, err := s.CreateJob(ctx, j3)
	if err != nil || !r3.Created {
		t.Errorf("cross-type collision: %v created=%v", err, r3.Created)
	}

	// No key: never deduplicated.
	j4, _ := job.New("t", nil, job.Options{}, time.Now().UTC())
	j5, _ := job.New("t", nil, job.Options{}, time.Now().UTC())
	if r, _ := s.CreateJob(ctx, j4); !r.Created {
		t.Error("keyless create 1 deduped")
	}
	if r, _ := s.CreateJob(ctx, j5); !r.Created {
		t.Error("keyless create 2 deduped")
	}
}

func TestClaimArbitration(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{})

	c1, ok, err := s.ClaimJob(ctx, j.ID, "w1", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("first claim: %v ok=%v", err, ok)
	}
	if c1.Epoch != 1 || c1.Attempt != 1 {
		t.Errorf("claim = %+v, want epoch 1 attempt 1", c1)
	}

	// A second delivery of the same job loses the arbitration.
	_, ok, err = s.ClaimJob(ctx, j.ID, "w2", 30*time.Second)
	if err != nil || ok {
		t.Errorf("duplicate claim: ok=%v want false", ok)
	}
}

func TestFencedCompletion(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{})

	c, _, _ := s.ClaimJob(ctx, j.ID, "w1", 30*time.Second)

	// Stale epoch: fence rejects, row untouched.
	ok, err := s.CompleteJob(ctx, j.ID, c.Epoch-1, json.RawMessage(`{"x":1}`))
	if err != nil || ok {
		t.Errorf("stale complete applied: ok=%v err=%v", ok, err)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != job.StatusRunning {
		t.Errorf("status after fenced write = %s, want RUNNING", got.Status)
	}

	// Right epoch: applies.
	ok, err = s.CompleteJob(ctx, j.ID, c.Epoch, json.RawMessage(`{"x":1}`))
	if err != nil || !ok {
		t.Fatalf("complete: ok=%v err=%v", ok, err)
	}
	got, _ = s.GetJob(ctx, j.ID)
	if got.Status != job.StatusCompleted || !jsonEqual(t, got.Result, `{"x":1}`) || got.CompletedAt == nil {
		t.Errorf("after complete: %+v", got)
	}

	// Completing again (redelivered execution): status is no longer
	// RUNNING, so even the right epoch is rejected. One persisted result.
	ok, _ = s.CompleteJob(ctx, j.ID, c.Epoch, json.RawMessage(`{"x":2}`))
	if ok {
		t.Error("double completion applied")
	}
	got, _ = s.GetJob(ctx, j.ID)
	if !jsonEqual(t, got.Result, `{"x":1}`) {
		t.Errorf("result overwritten: %s", got.Result)
	}
}

func TestReapZombieFencing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{})

	// Worker claims with a lease that expires immediately (simulating a
	// crash: LeaseTTL below the config floor is fine at the store layer).
	c, _, _ := s.ClaimJob(ctx, j.ID, "w1", -1*time.Second)

	// Reaper reclaims. Attempts left -> RETRYING, epoch bumped.
	r, err := s.ReapJob(ctx, j.ID, 5*time.Second, 0)
	if err != nil || !r.Reaped {
		t.Fatalf("reap: %v reaped=%v", err, r.Reaped)
	}
	if r.NewStatus != job.StatusRetrying || r.PrevWorker != "w1" {
		t.Errorf("reap result: %+v", r)
	}

	// The zombie wakes up and tries to write its result: fenced.
	ok, err := s.CompleteJob(ctx, j.ID, c.Epoch, json.RawMessage(`{"zombie":true}`))
	if err != nil || ok {
		t.Errorf("zombie write applied: ok=%v", ok)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != job.StatusRetrying || got.Result != nil {
		t.Errorf("after zombie write: status=%s result=%s", got.Status, got.Result)
	}
}

func TestReapExhaustedGoesToDeadLetter(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{MaxAttempts: 1})

	s.ClaimJob(ctx, j.ID, "w1", -1*time.Second)
	r, err := s.ReapJob(ctx, j.ID, time.Second, 0)
	if err != nil || !r.Reaped {
		t.Fatal(err)
	}
	if r.NewStatus != job.StatusDeadLetter {
		t.Errorf("status = %s, want DEAD_LETTER", r.NewStatus)
	}
}

func TestReapSkipsLiveLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{})
	s.ClaimJob(ctx, j.ID, "w1", 60*time.Second)

	r, err := s.ReapJob(ctx, j.ID, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Reaped {
		t.Error("reaped a job with a live lease")
	}
}

func TestRetryAndPromoteFlow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{})

	c, _, _ := s.ClaimJob(ctx, j.ID, "w1", 30*time.Second)
	next := time.Now().UTC().Add(2 * time.Second)
	ok, err := s.ScheduleRetry(ctx, j.ID, c.Epoch, "boom", next)
	if err != nil || !ok {
		t.Fatalf("schedule retry: %v ok=%v", err, ok)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != job.StatusRetrying || got.Error != "boom" {
		t.Errorf("after retry: %+v", got)
	}

	n, err := s.MarkPromoted(ctx, []string{j.ID})
	if err != nil || n != 1 {
		t.Fatalf("promote: %v n=%d", err, n)
	}
	got, _ = s.GetJob(ctx, j.ID)
	if got.Status != job.StatusPending {
		t.Errorf("after promote: %s", got.Status)
	}

	// Second claim works from PENDING with attempt 2, epoch 2.
	c2, ok, _ := s.ClaimJob(ctx, j.ID, "w2", 30*time.Second)
	if !ok || c2.Attempt != 2 || c2.Epoch != 2 {
		t.Errorf("reclaim: ok=%v %+v", ok, c2)
	}
}

func TestCancelPendingOnly(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	j := mustCreate(t, s, job.Options{})
	ok, err := s.CancelJob(ctx, j.ID)
	if err != nil || !ok {
		t.Fatalf("cancel pending: %v ok=%v", err, ok)
	}
	// Cancelled jobs can't be claimed.
	if _, ok, _ := s.ClaimJob(ctx, j.ID, "w1", time.Second); ok {
		t.Error("claimed a cancelled job")
	}

	j2 := mustCreate(t, s, job.Options{})
	s.ClaimJob(ctx, j2.ID, "w1", 30*time.Second)
	if ok, _ := s.CancelJob(ctx, j2.ID); ok {
		t.Error("CancelJob touched a RUNNING job (that path is cooperative)")
	}
}

func TestOrphanAndStuckSweepQueries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Never enqueued (no SetEnqueued call) and old enough.
	j := mustCreate(t, s, job.Options{})
	s.pool.Exec(ctx, `UPDATE jobs SET created_at = now() - interval '5 minutes' WHERE id = $1`, j.ID)

	// Enqueued properly: must NOT be listed.
	j2 := mustCreate(t, s, job.Options{})
	s.SetEnqueued(ctx, j2.ID, "1-1")
	s.pool.Exec(ctx, `UPDATE jobs SET created_at = now() - interval '5 minutes' WHERE id = $1`, j2.ID)

	orphans, err := s.ListUnenqueued(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 || orphans[0].ID != j.ID {
		t.Errorf("orphans = %+v, want just %s", orphans, j.ID)
	}

	// Stuck retrying: RETRYING with scheduled_at long past.
	j3 := mustCreate(t, s, job.Options{})
	c, _, _ := s.ClaimJob(ctx, j3.ID, "w1", 30*time.Second)
	s.ScheduleRetry(ctx, j3.ID, c.Epoch, "x", time.Now().UTC().Add(-5*time.Minute))
	stuck, err := s.ListStuckRetrying(ctx, 30*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(stuck) != 1 || stuck[0].ID != j3.ID || stuck[0].Attempt != 1 {
		t.Errorf("stuck = %+v", stuck)
	}
}

func TestDeadLetterRequeue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{MaxAttempts: 1})

	c, _, _ := s.ClaimJob(ctx, j.ID, "w1", 30*time.Second)
	if ok, _ := s.DeadLetterJob(ctx, j.ID, c.Epoch, "gave up"); !ok {
		t.Fatal("dead letter failed")
	}

	dl, err := s.ListDeadLetters(ctx, 10, 0)
	if err != nil || len(dl) != 1 {
		t.Fatalf("dlq list: %v n=%d", err, len(dl))
	}

	r, ok, err := s.RequeueDeadLetter(ctx, j.ID)
	if err != nil || !ok {
		t.Fatalf("requeue: %v ok=%v", err, ok)
	}
	if r.Attempt != 0 {
		t.Errorf("attempts not reset: %d", r.Attempt)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.Status != job.StatusPending || got.Error != "" || got.AttemptCount != 0 {
		t.Errorf("after requeue: %+v", got)
	}
	// Requeueing a non-DLQ job is a no-op.
	if _, ok, _ := s.RequeueDeadLetter(ctx, j.ID); ok {
		t.Error("requeued a PENDING job")
	}
}

func TestAttemptsRecorded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	j := mustCreate(t, s, job.Options{})

	now := time.Now().UTC()
	att := Attempt{JobID: j.ID, Attempt: 1, WorkerID: "w1", StartedAt: now,
		FinishedAt: now.Add(time.Second), Outcome: job.OutcomeRetryable, Error: "boom"}
	if err := s.RecordAttempt(ctx, att); err != nil {
		t.Fatal(err)
	}
	// Idempotent on (job, attempt).
	if err := s.RecordAttempt(ctx, att); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListAttempts(ctx, j.ID)
	if err != nil || len(got) != 1 {
		t.Fatalf("attempts: %v n=%d", err, len(got))
	}
	if got[0].Outcome != job.OutcomeRetryable || got[0].Error != "boom" {
		t.Errorf("attempt = %+v", got[0])
	}
}

func TestBatcherFlushAndFencing(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	b := NewBatcher(s, 64, 10*time.Millisecond, slog.Default())
	b.Start()
	defer b.Stop()

	// A batch of real completions plus one deliberately stale epoch.
	const n = 20
	jobs := make([]*job.Job, n)
	claims := make([]Claim, n)
	for i := range jobs {
		jobs[i] = mustCreate(t, s, job.Options{})
		claims[i], _, _ = s.ClaimJob(ctx, jobs[i].ID, "w1", 30*time.Second)
	}

	type res struct {
		applied bool
		err     error
	}
	results := make(chan res, n)
	for i := range jobs {
		go func(i int) {
			epoch := claims[i].Epoch
			if i == 0 {
				epoch-- // stale: must be fenced, not applied
			}
			att := Attempt{JobID: jobs[i].ID, Attempt: 1, WorkerID: "w1",
				StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(), Outcome: job.OutcomeCompleted}
			ok, err := b.Complete(ctx, jobs[i].ID, epoch, json.RawMessage(`{"i":true}`), att)
			results <- res{ok, err}
		}(i)
	}

	var applied, fenced int
	for i := 0; i < n; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("batched complete: %v", r.err)
		}
		if r.applied {
			applied++
		} else {
			fenced++
		}
	}
	if applied != n-1 || fenced != 1 {
		t.Errorf("applied=%d fenced=%d, want %d/1", applied, fenced, n-1)
	}

	// Fenced job untouched; the rest completed; every attempt recorded.
	got, _ := s.GetJob(ctx, jobs[0].ID)
	if got.Status != job.StatusRunning {
		t.Errorf("fenced job status = %s", got.Status)
	}
	got, _ = s.GetJob(ctx, jobs[1].ID)
	if got.Status != job.StatusCompleted {
		t.Errorf("batched job status = %s", got.Status)
	}
	atts, _ := s.ListAttempts(ctx, jobs[1].ID)
	if len(atts) != 1 {
		t.Errorf("attempt rows = %d", len(atts))
	}
}

func TestWorkerHeartbeats(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	w := WorkerInfo{ID: "w1", Hostname: "h", PID: 1, Concurrency: 4, TargetConcurrency: 4, State: "active"}
	if err := s.UpsertWorker(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.ActiveJobs = 2
	w.Processed = 10
	if err := s.UpsertWorker(ctx, w); err != nil {
		t.Fatal(err)
	}
	ws, err := s.ListWorkers(ctx)
	if err != nil || len(ws) != 1 || ws[0].Processed != 10 {
		t.Fatalf("workers: %v %+v", err, ws)
	}

	s.pool.Exec(ctx, `UPDATE workers SET last_heartbeat_at = now() - interval '10 minutes'`)
	n, err := s.MarkDeadWorkers(ctx, time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("mark dead: %v n=%d", err, n)
	}
}

func TestSummary(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	since := time.Now().UTC().Add(-time.Minute)

	for i := 0; i < 3; i++ {
		j := mustCreate(t, s, job.Options{})
		c, _, _ := s.ClaimJob(ctx, j.ID, "w1", 30*time.Second)
		s.CompleteJob(ctx, j.ID, c.Epoch, nil)
	}
	mustCreate(t, s, job.Options{}) // one still pending

	sum, err := s.Summary(ctx, since)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Counts[job.StatusCompleted] != 3 || sum.Counts[job.StatusPending] != 1 || sum.Total != 4 {
		t.Errorf("summary counts: %+v", sum.Counts)
	}
	if sum.E2ESeconds.P50 < 0 || sum.E2ESeconds.P99 < sum.E2ESeconds.P50 {
		t.Errorf("percentiles look wrong: %+v", sum.E2ESeconds)
	}
}
