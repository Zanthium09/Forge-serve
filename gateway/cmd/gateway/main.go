package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/api"
	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

func main() {
	addr := ":8080"
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		addr = v
	}
	workerURL := os.Getenv("WORKER_URL")
	if workerURL == "" {
		workerURL = "http://localhost:9000"
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		slog.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	st, err := store.New(dsn)
	if err != nil {
		slog.Error("postgres unavailable", "err", err)
		os.Exit(1)
	}
	authn := auth.New(st, 5*time.Minute)
	wc := worker.New(workerURL)
	srv := &http.Server{Addr: addr, Handler: api.NewMux(wc, authn)}

	// Graceful shutdown: SIGTERM/interrupt lets in-flight streams drain
	// instead of cutting client connections mid-token.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	// Poll the worker's scheduler state into Prometheus. Polling (not
	// pushing) keeps the worker unaware of our metrics stack.
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if s, err := wc.Stats(sctx); err == nil {
					metrics.SetWorkerStats(s.QueueDepth, s.ActiveSeqs)
				}
				cancel()
			}
		}
	}()

	slog.Info("gateway listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}
