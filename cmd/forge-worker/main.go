// forge-worker executes jobs. Scale it by running more of them (compose
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

	"github.com/jguapp/forge/internal/config"
	"github.com/jguapp/forge/internal/handlers"
	"github.com/jguapp/forge/internal/metrics"
	"github.com/jguapp/forge/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("forge-worker: fatal", "err", err)
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

	reg := handlers.NewRegistry(handlers.Config{
		CallbackSecret:       cfg.CallbackHMACSecret,
		CallbackAllowedHosts: cfg.CallbackAllowedHosts,
	})
	if cfg.CallbackHMACSecret == "" {
		log.Warn("forge-worker: FORGE_CALLBACK_SECRET is empty; http.callback deliveries are UNSIGNED")
	}

	m := metrics.New()
	w, err := worker.New(ctx, cfg, reg, metrics.Observer{M: m}, log)
	if err != nil {
		return err
	}
	m.RegisterWorkerGauges(cfg.WorkerID, w.Pool())

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
		log.Info("forge-worker: metrics listening", "addr", cfg.MetricsAddr, "worker", cfg.WorkerID)
		metricsSrv.ListenAndServe() //nolint:errcheck // shutdown handled below
	}()

	log.Info("forge-worker: starting", "worker", cfg.WorkerID,
		"concurrency", cfg.Concurrency, "types", reg.Types())
	err = w.Run(ctx) // blocks until SIGTERM, then drains

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	metricsSrv.Shutdown(shutdownCtx) //nolint:errcheck
	return err
}
