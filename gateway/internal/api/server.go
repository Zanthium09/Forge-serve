// Package api holds the gateway's HTTP surface: health, and the
// OpenAI-compatible chat completions endpoint (stubbed in Phase 0).
package api

import "net/http"

// NewMux returns the gateway's routes. Go 1.22 method patterns replace a
// router dependency — stdlib is enough at this size.
func NewMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
