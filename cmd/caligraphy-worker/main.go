// caligraphy-worker executes jobs. Scale it by running more of them (compose
// --scale, k8s replicas) or by raising one worker's concurrency; the two
// compose, and the benchmarks measure both axes.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jguapp/caligraphy/internal/config"
	"github.com/jguapp/caligraphy/internal/control"
	"github.com/jguapp/caligraphy/internal/handlers"
	"github.com/jguapp/caligraphy/internal/metrics"
	"github.com/jguapp/caligraphy/internal/scale"
	"github.com/jguapp/caligraphy/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("caligraphy-worker: fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := handlers.NewRegistry(handlers.Config{ //nolint:staticcheck // stop is reused by OnDrain below
		CallbackSecret:       cfg.CallbackHMACSecret,
		CallbackAllowedHosts: cfg.CallbackAllowedHosts,
	})
	if cfg.CallbackHMACSecret == "" {
		log.Warn("caligraphy-worker: CALIGRAPHY_CALLBACK_SECRET is empty; http.callback deliveries are UNSIGNED")
	}

	m := metrics.New()
	w, err := worker.New(ctx, cfg, reg, metrics.Observer{M: m}, log)
	if err != nil {
		return err
	}
	m.RegisterWorkerGauges(cfg.WorkerID, w.Pool())

	// Control plane: optional (the worker is fully functional without it;
	// jobs flow through Redis either way -- this only adds operability).
	if cfg.ControlAddr != "" {
		cc := &control.Client{
			Addr: cfg.ControlAddr, WorkerID: cfg.WorkerID, Types: reg.Types(),
			Pool: w.Pool(), Log: log,
			OnDrain: stop, // a drain command IS a graceful shutdown
		}
		go cc.Run(ctx)
	}

	// Queue-depth autoscaling: also optional, also composable with the
	// control plane (an operator's SetConcurrency just becomes the next
	// baseline the scaler adjusts from).
	if cfg.AutoscaleEnabled {
		as := &scale.Autoscaler{
			Cfg:  scale.DefaultConfig(cfg.MinConcurrency, cfg.MaxConcurrency),
			Pool: w.Pool(), Log: log,
			Depths: func(dctx context.Context) (int64, error) {
				d, err := w.Queue().Depths(dctx)
				if err != nil {
					return 0, err
				}
				return d.TotalReady(), nil
			},
		}
		go as.Run(ctx)
		log.Info("caligraphy-worker: autoscaling enabled",
			"min", cfg.MinConcurrency, "max", cfg.MaxConcurrency)
	}

	// The worker's own tiny HTTP surface: metrics + liveness. No API here
	// -- workers pull work; nothing should ever need to call INTO one.
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`)) //nolint:errcheck
	})
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("caligraphy-worker: metrics listening", "addr", cfg.MetricsAddr, "worker", cfg.WorkerID)
		metricsSrv.ListenAndServe() //nolint:errcheck // shutdown handled below
	}()

	log.Info("caligraphy-worker: starting", "worker", cfg.WorkerID,
		"concurrency", cfg.Concurrency, "types", reg.Types())
	err = w.Run(ctx) // blocks until SIGTERM, then drains

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	metricsSrv.Shutdown(shutdownCtx) //nolint:errcheck
	return err
}
