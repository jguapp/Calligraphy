package worker

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jguapp/forge/internal/config"
	"github.com/jguapp/forge/internal/handler"
	"github.com/jguapp/forge/internal/queue"
	"github.com/jguapp/forge/internal/retry"
	"github.com/jguapp/forge/internal/store"
)

// Worker assembles the runtime: store + queue + recorder + pool + runner,
// plus the heartbeat loop that keeps the workers table honest.
type Worker struct {
	cfg   config.Config
	log   *slog.Logger
	obs   Observer
	store *store.Store
	queue *queue.Queue
	rec   store.Recorder
	pool  *Pool

	batcher *store.Batcher // nil when running sync writes
}

func New(ctx context.Context, cfg config.Config, reg *handler.Registry, obs Observer, log *slog.Logger) (*Worker, error) {
	if log == nil {
		log = slog.Default()
	}
	st, err := store.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return nil, err
	}
	// Migrating here as well as in the API is deliberate: the migrator is
	// idempotent under an advisory lock, and a worker that can start
	// against an empty database removes a whole class of orchestration
	// ordering problems (compose races, k8s pod scheduling).
	if err := st.Migrate(ctx); err != nil {
		st.Close()
		return nil, err
	}
	q, err := queue.New(ctx, queue.Config{
		Addr: cfg.RedisAddr, PoolSize: cfg.RedisPoolSize,
		Prefix: cfg.KeyPrefix, Queue: cfg.Queue, Logger: log,
	})
	if err != nil {
		st.Close()
		return nil, err
	}
	if err := q.EnsureGroup(ctx); err != nil {
		st.Close()
		q.Close()
		return nil, err
	}

	w := &Worker{cfg: cfg, log: log, obs: obs, store: st, queue: q}
	if cfg.BatchWrites {
		w.batcher = store.NewBatcher(st, cfg.BatchMaxSize, cfg.BatchInterval, log)
		w.rec = w.batcher
	} else {
		w.rec = store.SyncRecorder{Store: st}
	}

	runner := &Runner{
		Store: st, Recorder: w.rec, Queue: q, Registry: reg,
		Backoff: retry.New(cfg.RetryBase, cfg.RetryCap),
		Log:     log, Obs: obs,
		WorkerID: cfg.WorkerID, LeaseTTL: cfg.LeaseTTL, ExecTimeout: cfg.ExecTimeout,
	}
	fetch := func(ctx context.Context, max int) ([]queue.Delivery, error) {
		return q.Fetch(ctx, cfg.WorkerID, max, cfg.FetchBlock)
	}
	w.pool = NewPool(fetch, runner.Run, cfg.Concurrency, cfg.FetchBatch, log)
	return w, nil
}

// Pool exposes the pool for the control plane (drain/pause/set-target) and
// the autoscaler.
func (w *Worker) Pool() *Pool         { return w.pool }
func (w *Worker) Queue() *queue.Queue { return w.queue }
func (w *Worker) Store() *store.Store { return w.store }

// Run executes until ctx is cancelled, then drains:
//
//  1. stop fetching (no new claims)
//  2. wait up to DrainTimeout for in-flight jobs to finish on their own
//  3. cancel their contexts (handlers see it; sleep/cpu handlers return
//     promptly) and wait a short grace
//  4. flush the batcher, final heartbeat, close connections
//
// A worker killed with SIGKILL skips all of this -- which is fine, and
// tested: the lease/reaper machinery exists precisely for the worker that
// never got to say goodbye.
func (w *Worker) Run(ctx context.Context) error {
	if w.batcher != nil {
		w.batcher.Start()
	}

	fetchCtx, stopFetch := context.WithCancel(context.Background())
	jobCtx, stopJobs := context.WithCancelCause(context.Background())
	defer stopJobs(nil)

	poolDone := make(chan struct{})
	go func() {
		w.pool.Run(fetchCtx, jobCtx)
		close(poolDone)
	}()

	hbCtx, stopHB := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go w.heartbeatLoop(hbCtx, hbDone)

	<-ctx.Done()
	w.log.Info("worker: draining", "active", w.pool.Active(), "timeout", w.cfg.DrainTimeout)
	stopFetch()

	select {
	case <-poolDone:
		w.log.Info("worker: drained clean")
	case <-time.After(w.cfg.DrainTimeout):
		w.log.Warn("worker: drain timeout; cancelling in-flight jobs", "active", w.pool.Active())
		stopJobs(context.Cause(ctx))
		select {
		case <-poolDone:
		case <-time.After(10 * time.Second):
			w.log.Error("worker: jobs ignored cancellation; abandoning to the reaper", "active", w.pool.Active())
		}
	}

	stopHB()
	<-hbDone
	if w.batcher != nil {
		w.batcher.Stop() // flushes everything queued; records are durable past here
	}
	w.finalHeartbeat()
	w.queue.Close()
	w.store.Close()
	return nil
}

func (w *Worker) heartbeatLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	host, _ := os.Hostname()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		w.upsert(ctx, host, "active")
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *Worker) upsert(ctx context.Context, host, state string) {
	hbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	err := w.store.UpsertWorker(hbCtx, store.WorkerInfo{
		ID: w.cfg.WorkerID, Hostname: host, PID: os.Getpid(),
		Concurrency: w.pool.Active(), TargetConcurrency: w.pool.Target(),
		ActiveJobs: w.pool.Active(), Processed: int64(w.pool.Processed()),
		State: state,
	})
	if err != nil {
		w.log.Warn("worker: heartbeat failed", "err", err)
	}
}

func (w *Worker) finalHeartbeat() {
	host, _ := os.Hostname()
	w.upsert(context.Background(), host, "draining")
}
