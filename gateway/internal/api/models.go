package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
)

// handleModels answers GET /v1/models from config (not from the engines),
// so clients see the gateway's model names.
func handleModels(f *pool.Fleet) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, authed := auth.FromContext(r.Context())
		type entry struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			OwnedBy string `json:"owned_by"`
		}
		data := []entry{}
		for _, n := range f.Names() {
			m, _ := f.Model(n)
			if authed && !id.CanUse(m.Name, m.Restricted) {
				continue // only models this caller may use
			}
			data = append(data, entry{ID: n, Object: "model", OwnedBy: "forge"})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
	}
}

// handleReady: the gateway can serve when its database answers and at
// least one backend is healthy.
func handleReady(f *pool.Fleet, ready func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			if err := ready(r.Context()); err != nil {
				http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		if !f.AnyHealthy() {
			http.Error(w, "not ready: no healthy backend", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}
}
