package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/api"
	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	addr := getenv("GATEWAY_ADDR", ":8080")

	// Comma-separated fleet; the gateway health-checks each (ADR-004).
	workerURLs := strings.Split(getenv("WORKER_URLS", "http://localhost:9000"), ",")
	for i := range workerURLs {
		workerURLs[i] = strings.TrimSpace(workerURLs[i])
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
	wp := pool.New(workerURLs)
	srv := &http.Server{Addr: addr, Handler: api.NewMux(api.Deps{Pool: wp, Auth: authn})}

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

	// Health + capacity polling for the whole fleet.
	wp.Start(ctx, 2*time.Second)
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := wp.Snapshot()
				metrics.SetFleet(s.QueueDepth, s.ActiveSeqs, s.HealthyWorkers, s.TotalWorkers)
			}
		}
	}()

	slog.Info("gateway listening", "addr", addr, "workers", workerURLs)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}
