// Package recovery is the answer to the question the README refuses to
// hand-wave: a worker claims a job, the worker dies, Postgres says
// RUNNING -- now what?
//
// Five duties, all idempotent, all fenced, run by whichever process holds
// the leader lock (caligraphy-api replicas race for it; losing is fine):
//
//	promote        due delayed envelopes (retries, scheduled jobs) move
//	               ZSET -> stream, and their rows flip RETRYING -> PENDING
//	reap (Redis)   XAUTOCLAIM entries whose consumer went silent past the
//	               lease: the crashed/hung/partitioned worker's jobs, found
//	               via the PEL's own idle clock
//	sweep (DB)     RUNNING rows whose lease is long expired but which have
//	               no PEL entry left -- the rarer "acked but never
//	               recorded" crash window; re-enqueued from the row itself
//	repair         PENDING rows never confirmably handed to Redis (the
//	               enqueue-failed submit), and RETRYING rows whose
//	               promotion is overdue (the ZADD-failed retry)
//	workers        heartbeat bookkeeping: silent workers marked 'gone'
//
// Leadership is an optimization, not a correctness requirement: every
// duty is safe to run twice (conditional UPDATEs, fenced epochs,
// idempotent attempt inserts), so a split-brain moment during lock
// handover costs duplicated effort, never duplicated results.
package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/jguapp/caligraphy/internal/config"
	"github.com/jguapp/caligraphy/internal/job"
	"github.com/jguapp/caligraphy/internal/queue"
	"github.com/jguapp/caligraphy/internal/retry"
	"github.com/jguapp/caligraphy/internal/store"
)

const (
	leaderRole = "recovery"
	leaderTTL  = 15 * time.Second

	// Duty cadences, in ticks of the 250ms base loop. Promotion runs every
	// tick because it gates retry latency; the sweeps are backstops for
	// rare windows and don't need to hurry.
	promoteEvery = 1  // 250ms
	reapEvery    = 8  // 2s
	sweepEvery   = 40 // 10s

	batchSize = 256
)

type Leader struct {
	Store   *store.Store
	Queue   *queue.Queue
	Cfg     config.Config
	Log     *slog.Logger
	Backoff retry.Policy
	// ID identifies this contender in the lock (hostname-pid works).
	ID string
}

// Run contends for leadership and performs the duties while holding it.
// Returns when ctx is cancelled.
func (l *Leader) Run(ctx context.Context) {
	if l.Log == nil {
		l.Log = slog.Default()
	}
	isLeader := false
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	renew := time.NewTicker(leaderTTL / 3)
	defer renew.Stop()

	n := 0
	for {
		select {
		case <-ctx.Done():
			if isLeader {
				releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				l.Queue.ReleaseLeader(releaseCtx, leaderRole, l.ID)
				cancel()
			}
			return

		case <-renew.C:
			if isLeader {
				still, err := l.Queue.RenewLeader(ctx, leaderRole, l.ID, leaderTTL)
				if err != nil || !still {
					l.Log.Warn("recovery: leadership lost", "err", err)
					isLeader = false
				}
			}

		case <-tick.C:
			if !isLeader {
				got, err := l.Queue.AcquireLeader(ctx, leaderRole, l.ID, leaderTTL)
				if err != nil {
					continue // redis unhappy; try next tick
				}
				if got {
					l.Log.Info("recovery: became leader", "id", l.ID)
					isLeader = true
				} else {
					continue
				}
			}
			n++
			if n%promoteEvery == 0 {
				l.promote(ctx)
			}
			if n%reapEvery == 0 {
				l.reapAbandoned(ctx)
			}
			if n%sweepEvery == 0 {
				l.sweepStaleRunning(ctx)
				l.repairUnenqueued(ctx)
				l.repairStuckRetrying(ctx)
				l.sweepWorkers(ctx)
			}
		}
	}
}

// promote moves due delayed envelopes onto their streams and flips their
// rows RETRYING -> PENDING. (Scheduled first-run jobs are also in the
// delayed set; their rows are already PENDING and MarkPromoted's
// conditional UPDATE simply doesn't match them -- harmless by design.)
func (l *Leader) promote(ctx context.Context) {
	envs, err := l.Queue.PromoteDue(ctx, batchSize)
	if err != nil {
		l.Log.Warn("recovery: promote failed", "err", err)
		return
	}
	if len(envs) == 0 {
		return
	}
	ids := make([]string, len(envs))
	for i, e := range envs {
		ids[i] = e.ID
	}
	if _, err := l.Store.MarkPromoted(ctx, ids); err != nil {
		// The envelopes are already on the stream; rows still say
		// RETRYING. Claim accepts RETRYING precisely for this window.
		l.Log.Warn("recovery: promote DB flip failed (claims will still win)", "err", err)
	}
}

// reapAbandoned handles entries whose consumer stopped heartbeating: the
// SIGKILLed worker, the hard-hung process, the partitioned host. The PEL's
// idle clock found them; the database decides what happens next.
func (l *Leader) reapAbandoned(ctx context.Context) {
	deliveries, err := l.Queue.ReapAbandoned(ctx, l.Cfg.ReapMinIdle, batchSize)
	if err != nil {
		l.Log.Warn("recovery: reap failed", "err", err)
		return
	}
	for _, d := range deliveries {
		l.reapOne(ctx, d)
	}
}

func (l *Leader) reapOne(ctx context.Context, d queue.Delivery) {
	res, err := l.Store.ReapJob(ctx, d.Env.ID, l.Backoff.Delay(maxInt(d.Env.Attempt, 1)), 0)
	if err != nil {
		l.Log.Warn("recovery: reap decision failed; entry left for next pass", "job", d.Env.ID, "err", err)
		return
	}
	if !res.Reaped {
		// The job isn't an expired-RUNNING row: the worker finished (or
		// was cancelled) but died before acking. The record stands; the
		// entry is just litter.
		l.ack(ctx, d)
		return
	}

	// Record the doomed attempt. The dying worker may have recorded the
	// same coordinates in its last breath -- ON CONFLICT DO NOTHING makes
	// first-writer-wins, and both tell the same story.
	now := time.Now().UTC()
	l.recordAttempt(ctx, store.Attempt{
		JobID: d.Env.ID, Attempt: res.Attempt, WorkerID: res.PrevWorker,
		StartedAt: now, FinishedAt: now,
		Outcome: job.OutcomeLeaseExpired, Error: "lease expired: worker presumed dead",
	})

	switch res.NewStatus {
	case job.StatusRetrying:
		next := d.Env
		next.Attempt = res.Attempt + 1
		if err := l.Queue.ScheduleRetry(ctx, next, now.Add(l.Backoff.Delay(res.Attempt))); err != nil {
			// Row says RETRYING; the stuck-retry sweep repairs the
			// missing handoff. Nothing lost, just slower.
			l.Log.Warn("recovery: retry handoff failed; sweep will repair", "job", d.Env.ID, "err", err)
		}
	case job.StatusDeadLetter:
		if err := l.Queue.DeadLetter(ctx, d.Env, "lease expired: attempts exhausted"); err != nil {
			l.Log.Warn("recovery: dlq log append failed (postgres record stands)", "err", err)
		}
	}
	l.ack(ctx, d)
	l.Log.Info("recovery: reaped abandoned job",
		"job", d.Env.ID, "prevWorker", res.PrevWorker, "newStatus", res.NewStatus, "attempt", res.Attempt)
}

// sweepStaleRunning is the backstop for RUNNING rows with no PEL entry
// left -- reachable when a worker acked and then died before its record
// flushed (rare: it requires the batcher's ack-after-flush ordering to be
// interrupted exactly between the two). No entry exists, so the envelope
// is rebuilt from the row and re-enqueued.
func (l *Leader) sweepStaleRunning(ctx context.Context) {
	rows, err := l.Store.ListExpiredRunning(ctx, l.Cfg.SweepGrace, batchSize)
	if err != nil {
		l.Log.Warn("recovery: stale sweep list failed", "err", err)
		return
	}
	for _, r := range rows {
		res, err := l.Store.ReapJob(ctx, r.ID, l.Backoff.Delay(maxInt(r.Attempt, 1)), l.Cfg.SweepGrace.Seconds())
		if err != nil || !res.Reaped {
			continue // raced with the Redis-side reaper or a live write; fine
		}
		now := time.Now().UTC()
		l.recordAttempt(ctx, store.Attempt{
			JobID: r.ID, Attempt: res.Attempt, WorkerID: res.PrevWorker,
			StartedAt: now, FinishedAt: now,
			Outcome: job.OutcomeLeaseExpired, Error: "lease expired: no queue entry survived",
		})
		env := requeueEnvelope(r, res.Attempt+1)
		switch res.NewStatus {
		case job.StatusRetrying:
			if err := l.Queue.ScheduleRetry(ctx, env, now.Add(l.Backoff.Delay(res.Attempt))); err != nil {
				l.Log.Warn("recovery: stale-sweep handoff failed; will retry next sweep", "job", r.ID, "err", err)
			}
		case job.StatusDeadLetter:
			l.Queue.DeadLetter(ctx, env, "lease expired: attempts exhausted")
		}
		l.Log.Info("recovery: swept stale RUNNING row", "job", r.ID, "newStatus", res.NewStatus)
	}
}

// repairUnenqueued fixes the submit-side crack: the API's INSERT landed
// but its XADD never confirmably did. Re-enqueue and mark, oldest first.
// If the original XADD actually succeeded (only its response was lost),
// this creates a duplicate delivery -- which claim arbitration collapses.
// At-least-once, working as designed.
func (l *Leader) repairUnenqueued(ctx context.Context) {
	rows, err := l.Store.ListUnenqueued(ctx, l.Cfg.OrphanAge, batchSize)
	if err != nil {
		l.Log.Warn("recovery: orphan list failed", "err", err)
		return
	}
	for _, r := range rows {
		env := requeueEnvelope(r, r.Attempt+1)
		streamID, err := l.Queue.Enqueue(ctx, env, time.Time{})
		if err != nil {
			l.Log.Warn("recovery: orphan re-enqueue failed", "job", r.ID, "err", err)
			continue
		}
		if err := l.Store.SetEnqueued(ctx, r.ID, streamID); err != nil {
			l.Log.Warn("recovery: orphan mark failed", "job", r.ID, "err", err)
		}
		l.Store.AddEvent(ctx, r.ID, "requeued", map[string]any{"reason": "orphaned submit"})
		l.Log.Info("recovery: repaired orphaned submit", "job", r.ID)
	}
}

// repairStuckRetrying fixes the retry-side crack: the row says RETRYING,
// the delayed set never heard (ZADD failed after the DB write). The
// backoff window has long passed, so skip the ZSET and enqueue directly.
func (l *Leader) repairStuckRetrying(ctx context.Context) {
	rows, err := l.Store.ListStuckRetrying(ctx, l.Cfg.OrphanAge, batchSize)
	if err != nil {
		l.Log.Warn("recovery: stuck-retry list failed", "err", err)
		return
	}
	for _, r := range rows {
		env := requeueEnvelope(r, r.Attempt+1)
		if _, err := l.Queue.Enqueue(ctx, env, time.Time{}); err != nil {
			l.Log.Warn("recovery: stuck-retry re-enqueue failed", "job", r.ID, "err", err)
			continue
		}
		if _, err := l.Store.MarkPromoted(ctx, []string{r.ID}); err != nil {
			l.Log.Warn("recovery: stuck-retry promote flip failed", "job", r.ID, "err", err)
		}
		l.Log.Info("recovery: repaired stuck retry", "job", r.ID)
	}
}

func (l *Leader) sweepWorkers(ctx context.Context) {
	// Three missed heartbeats (5s cadence) = gone. Bookkeeping only; jobs
	// are recovered by leases, never by this table.
	if n, err := l.Store.MarkDeadWorkers(ctx, 15*time.Second); err == nil && n > 0 {
		l.Log.Info("recovery: marked dead workers", "count", n)
	}
}

func (l *Leader) ack(ctx context.Context, d queue.Delivery) {
	if err := l.Queue.Ack(ctx, d.Priority, d.EntryID); err != nil {
		l.Log.Warn("recovery: ack failed; next reap pass will re-see the entry", "err", err)
	}
}

func (l *Leader) recordAttempt(ctx context.Context, a store.Attempt) {
	if err := l.Store.RecordAttempt(ctx, a); err != nil {
		l.Log.Warn("recovery: attempt record failed", "job", a.JobID, "err", err)
	}
}

func requeueEnvelope(r store.Requeueable, attempt int) job.Envelope {
	return job.Envelope{
		ID: r.ID, Type: r.Type, Queue: r.Queue, Priority: r.Priority,
		Attempt: attempt, Payload: r.Payload, EnqueuedAt: time.Now().UTC(),
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
