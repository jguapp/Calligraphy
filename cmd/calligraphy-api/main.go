// calligraphy-api is the platform's front door: the HTTP API, the metrics
// endpoint, and — because they are two tickers, not a service — the
// leader-elected recovery duties. A fourth binary for the reaper and
// promoter would be a deployment, an image, and a health probe spent on
// running two loops; they live here until they need independent scaling,
// which is an afternoon's change if it ever comes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jguapp/calligraphy/internal/api"
	"github.com/jguapp/calligraphy/internal/config"
	"github.com/jguapp/calligraphy/internal/control"
	"github.com/jguapp/calligraphy/internal/handlers"
	"github.com/jguapp/calligraphy/internal/metrics"
	"github.com/jguapp/calligraphy/internal/queue"
	"github.com/jguapp/calligraphy/internal/recovery"
	"github.com/jguapp/calligraphy/internal/retry"
	"github.com/jguapp/calligraphy/internal/store"
)

const version = "0.1.0"

func main() {
	if err := run(); err != nil {
		slog.Error("calligraphy-api: fatal", "err", err)
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

	st, err := store.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	q, err := queue.New(ctx, queue.Config{
		Addr: cfg.RedisAddr, PoolSize: cfg.RedisPoolSize,
		Prefix: cfg.KeyPrefix, Queue: cfg.Queue, Logger: log,
	})
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.EnsureGroup(ctx); err != nil {
		return err
	}

	m := metrics.New()
	m.MustRegister(metrics.NewDepthCollector(q))

	// The control plane: workers stream in; the API (and calligraphyctl through
	// it) commands them. Jobs never travel here.
	hub := control.NewHub(log)
	grpcSrv := control.NewGRPCServer(hub)
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	go func() {
		log.Info("calligraphy-api: control plane listening", "addr", cfg.GRPCAddr)
		grpcSrv.Serve(grpcLis) //nolint:errcheck // stopped in shutdown below
	}()

	// The registry is built here only for its type names: the API
	// validates submissions against what the worker fleet can execute.
	reg := handlers.NewRegistry(handlers.Config{
		CallbackSecret:       cfg.CallbackHMACSecret,
		CallbackAllowedHosts: cfg.CallbackAllowedHosts,
	})

	host, _ := os.Hostname()
	leader := &recovery.Leader{
		Store: st, Queue: q, Cfg: cfg, Log: log,
		Backoff: retry.New(cfg.RetryBase, cfg.RetryCap),
		ID:      fmt.Sprintf("%s-%d", host, os.Getpid()),
	}
	leaderDone := make(chan struct{})
	go func() {
		leader.Run(ctx)
		close(leaderDone)
	}()

	srv := api.NewServer(st, q, m, api.Config{
		Tokens:          cfg.APITokens,
		KnownTypes:      reg.Types(),
		MaxPayloadBytes: cfg.MaxPayloadBytes,
		Version:         version,
	}, log)
	srv.Hub = hub

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("calligraphy-api: listening", "addr", cfg.HTTPAddr, "version", version)
		if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Drain in-flight requests, bounded; a request that never finishes
	// must not hold the deploy open until the platform's kill timer.
	log.Info("calligraphy-api: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("calligraphy-api: shutdown timed out; closing anyway", "err", err)
	}
	grpcSrv.GracefulStop()
	<-leaderDone
	return nil
}
