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

	"github.com/Zanthium09/forge-serve/gateway/internal/admin"
	"github.com/Zanthium09/forge-serve/gateway/internal/api"
	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/budget"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
	"github.com/Zanthium09/forge-serve/gateway/internal/ratelimit"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// usageAdapter bridges api's write-side interface to the store, keeping
// the api package free of a store import.
type usageAdapter struct{ s *store.Store }

func (u usageAdapter) RecordUsage(ev api.UsageEvent) {
	u.s.RecordUsage(store.UsageEvent{
		UserID: ev.UserID, TeamID: ev.TeamID, KeyID: ev.KeyID,
		Model: ev.Model, RequestedModel: ev.RequestedModel, Endpoint: ev.Endpoint,
		PromptTokens: ev.PromptTokens, CompletionTokens: ev.CompletionTokens, Estimated: ev.Estimated,
	})
}

// loadConfig reads FORGE_CONFIG, or falls back to the WORKER_URLS
// shorthand (one model on our own workers) so older setups keep working.
func loadConfig() (*config.Config, error) {
	if p := os.Getenv("FORGE_CONFIG"); p != "" {
		return config.Load(p)
	}
	urls := strings.Split(getenv("WORKER_URLS", "http://localhost:9000"), ",")
	return config.FromWorkerURLs(urls, getenv("WORKER_MODEL", "qwen2.5-1.5b")), nil
}

func main() {
	if len(os.Args) > 1 {
		os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
	}
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("config invalid", "err", err)
		os.Exit(1)
	}
	addr := getenv("GATEWAY_ADDR", cfg.Listen)

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
	if err := st.Migrate(context.Background()); err != nil {
		slog.Error("database migration failed", "err", err)
		os.Exit(1)
	}
	if os.Getenv("FORGE_DEV_SEED") == "1" {
		// Development only: recreate sk-forge-dev-key / sk-forge-demo-key.
		if err := devSeed(context.Background(), st); err != nil {
			slog.Error("dev seed failed", "err", err)
			os.Exit(1)
		}
		slog.Warn("FORGE_DEV_SEED=1: well-known dev keys are active; never set this in production")
	}

	// Redis is optional at boot: without it the request and TPM limiters
	// start in local-fallback mode and upgrade themselves when Redis
	// appears (ADR-006), and budget checks are skipped and counted
	// (ADR-009). A limiter must never be a startup dependency.
	limiter := ratelimit.New(nil)
	tpm := ratelimit.NewTPM(nil)
	budgets := budget.New(budget.NoRedis, st)
	if ru := os.Getenv("REDIS_URL"); ru != "" {
		rb, rerr := ratelimit.NewRedisBackend(ru)
		if rerr != nil {
			slog.Warn("redis unavailable, limits start local-only", "err", rerr)
		} else {
			limiter = ratelimit.New(rb)
			tpm = ratelimit.NewTPM(rb)
			budgets = budget.New(budget.NewRedisCounter(rb.Client()), st)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authn := auth.New(st, cfg.AuthCacheTTL, cfg.Defaults.Priority, st.MarkKeyUsed)
	fleet, err := pool.NewFleet(cfg)
	if err != nil {
		slog.Error("fleet setup failed", "err", err)
		os.Exit(1)
	}
	storeDone := st.StartBackground(ctx)

	srv := &http.Server{Addr: addr, Handler: api.NewMux(api.Deps{
		Fleet:        fleet,
		Auth:         authn,
		Limiter:      limiter,
		TPM:          tpm,
		Budgets:      budgets,
		Usage:        st,
		Record:       usageAdapter{s: st},
		MaxBodyBytes: cfg.MaxBodyBytes,
		Ready:        st.Ping,
		Admin:        admin.New(st, authn, fleet.Names(), cfg.Defaults),
	})}

	// Graceful shutdown: SIGTERM/interrupt lets in-flight streams drain
	// instead of cutting client connections mid-token.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	// Health + capacity polling for the whole fleet.
	fleet.Start(ctx, 2*time.Second)
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := fleet.Snapshot()
				metrics.SetFleet(s.QueueDepth, s.ActiveSeqs, s.HealthyWorkers, s.TotalWorkers)
			}
		}
	}()

	slog.Info("gateway listening", "addr", addr, "models", fleet.Names())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
	// ListenAndServe returns as soon as Shutdown starts; wait for in-flight
	// streams to drain, then for the final usage flush.
	<-shutdownDone
	<-storeDone
}
