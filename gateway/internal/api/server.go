// Package api holds the gateway's HTTP surface: health, metrics, the
// playground page, and the OpenAI-compatible endpoints.
package api

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
)

// Deps are the gateway's collaborators. Passing a struct keeps NewMux
// stable as phases add dependencies.
type Deps struct {
	Pool *pool.Pool
	Auth *auth.Authenticator
}

// NewMux returns the gateway's routes. Auth wraps only /v1/* - health and
// metrics stay open for probes and scrapers.
func NewMux(d Deps) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("POST /v1/chat/completions",
		d.Auth.Middleware(&chatHandler{pool: d.Pool}))
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
