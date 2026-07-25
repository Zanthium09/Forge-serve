// Package api holds the gateway's HTTP surface: health and the
// OpenAI-compatible chat completions endpoint.
package api

import (
	"net/http"

	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// NewMux returns the gateway's routes, wired to the given worker client.
func NewMux(w *worker.Client) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.Handle("POST /v1/chat/completions", &chatHandler{worker: w})
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
