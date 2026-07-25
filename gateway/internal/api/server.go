// Package api holds the gateway's HTTP surface: health, metrics, the
// playground page, and the OpenAI-compatible endpoints.
package api

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
)

// UsageRecorder is the write side of usage accounting (async).
type UsageRecorder interface {
	RecordUsage(ev UsageEvent)
}

// UsageEvent mirrors store.UsageEvent so api does not import store for
// the write path.
type UsageEvent struct {
	TenantID         int64
	KeyHash          string
	Model            string
	PromptTokens     int
	CompletionTokens int
}

// Deps are the gateway's collaborators. Passing a struct keeps NewMux
// stable as phases add dependencies.
type Deps struct {
	Pool    *pool.Pool
	Auth    *auth.Authenticator
	Limiter Limiter
	Usage   UsageReporter
	Record  UsageRecorder
}

// NewMux returns the gateway's routes. Auth and rate limiting wrap only
// /v1/* — health, metrics and the playground stay open for probes,
// scrapers and browsers.
func NewMux(d Deps) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())

	chat := &chatHandler{pool: d.Pool, record: d.Record}
	mux.Handle("POST /v1/chat/completions",
		d.Auth.Middleware(rateLimit(d.Limiter, chat)))

	if d.Usage != nil {
		mux.Handle("GET /v1/usage", d.Auth.Middleware(&usageHandler{store: d.Usage}))
	}
	mux.HandleFunc("GET /", handlePlayground)
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
