// Package api holds the gateway's HTTP surface: health and the
// OpenAI-compatible chat completions endpoint.
package api

import (
	"net/http"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// NewMux returns the gateway's routes. Auth wraps only /v1/* — health
// (and later metrics) stay open for probes and scrapers.
func NewMux(w *worker.Client, a *auth.Authenticator) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.Handle("POST /v1/chat/completions",
		a.Middleware(&chatHandler{worker: w}))
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
