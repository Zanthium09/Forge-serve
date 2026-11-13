// Package api holds the gateway's HTTP surface: health, metrics, the
// playground page, and the OpenAI-compatible endpoints.
package api

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/backend"
	"github.com/Zanthium09/forge-serve/gateway/internal/budget"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
	"github.com/Zanthium09/forge-serve/gateway/internal/ratelimit"
)

// UsageRecorder is the write side of usage accounting (async).
type UsageRecorder interface {
	RecordUsage(ev UsageEvent)
}

// UsageEvent mirrors store.UsageEvent so api does not import store for
// the write path.
type UsageEvent struct {
	UserID, TeamID, KeyID int64
	Model                 string // model actually served
	RequestedModel        string // set only when a fallback served it
	Endpoint              string
	PromptTokens          int
	CompletionTokens      int
	Estimated             bool
}

// Deps are the gateway's collaborators. Passing a struct keeps NewMux
// stable as phases add dependencies.
type Deps struct {
	Fleet        *pool.Fleet
	Auth         *auth.Authenticator
	Limiter      Limiter
	TPM          *ratelimit.TPMLimiter // nil = no TPM limits
	Budgets      *budget.Budgets       // nil = no token budgets
	Usage        UsageReporter
	Record       UsageRecorder
	MaxBodyBytes int64
	Ready        func(context.Context) error // nil = always ready (tests)
	Admin        http.Handler                // nil = no admin API
}

// NewMux returns the gateway's routes. Auth and rate limiting wrap only
// /v1/* — health, metrics and the playground stay open for probes,
// scrapers and browsers.
func NewMux(d Deps) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReady(d.Fleet, d.Ready))
	mux.Handle("GET /metrics", promhttp.Handler())

	px := &proxyHandler{fleet: d.Fleet, maxBody: d.MaxBodyBytes, record: d.Record, tpm: d.TPM, budgets: d.Budgets}
	guard := func(h http.Handler) http.Handler { return d.Auth.Middleware(limits(d, h)) }
	mux.Handle("POST /v1/chat/completions", guard(px.serve(backend.EndpointChat)))
	mux.Handle("POST /v1/completions", guard(px.serve(backend.EndpointCompletions)))
	mux.Handle("POST /v1/embeddings", guard(px.serve(backend.EndpointEmbeddings)))
	mux.Handle("GET /v1/models", d.Auth.Middleware(handleModels(d.Fleet)))

	if d.Admin != nil {
		mux.Handle("/admin/v1/", d.Auth.Middleware(d.Auth.RequireAdmin(d.Admin)))
	}
	if d.Usage != nil {
		mux.Handle("GET /v1/usage", d.Auth.Middleware(&usageHandler{store: d.Usage}))
	}
	// "/{$}" = exact root only. A "GET /" catch-all conflicts with the
	// method-less "/admin/v1/" pattern and panics at registration.
	mux.HandleFunc("GET /{$}", handlePlayground)
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
