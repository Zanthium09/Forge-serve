package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
	"github.com/Zanthium09/forge-serve/gateway/internal/ratelimit"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
)

// Limiter is the rate-limiting dependency (interface so the handler tests
// can run without Redis).
type Limiter interface {
	Allow(ctx context.Context, key string, p ratelimit.Plan, now time.Time) (bool, error)
	UsingFallback() bool
}

// rateLimit rejects over-quota requests before any model work happens.
// It runs after auth, so the quota is the caller's own plan rather than a
// global guess.
func rateLimit(l Limiter, next http.Handler) http.Handler {
	if l == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := auth.FromContext(r.Context())
		if !ok {
			next.ServeHTTP(w, r) // unauthenticated routes are not limited
			return
		}
		plan := ratelimit.Plan{Burst: id.Burst, RefillPerSec: id.RefillPerSec}
		allowed, _ := l.Allow(r.Context(), id.KeyHash, plan, time.Now())
		if l.UsingFallback() {
			metrics.IncLimiterFallback()
		}
		if !allowed {
			metrics.IncRateLimited()
			metrics.IncRequest(http.StatusTooManyRequests)
			// Retry-After from the refill rate: how long until one token
			// is back, so clients back off by exactly the right amount.
			wait := 1
			if plan.RefillPerSec > 0 {
				if s := int(1/plan.RefillPerSec + 0.999); s > 1 {
					wait = s
				}
			}
			w.Header().Set("Retry-After", strconv.Itoa(wait))
			http.Error(w, `{"error":{"message":"rate limit exceeded","type":"rate_limit_error"}}`,
				http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// UsageReporter is the read side of usage accounting.
type UsageReporter interface {
	UsageForKey(ctx context.Context, hashHex string, since time.Time) (store.UsageSummary, error)
}

type usageHandler struct {
	store UsageReporter
}

// ServeHTTP answers GET /v1/usage for the calling key. Scoped to the
// caller's own key by construction — there is no key parameter to forge.
func (h *usageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id, ok := auth.FromContext(r.Context())
	if !ok {
		http.Error(w, `{"error":{"message":"unauthenticated"}}`, http.StatusUnauthorized)
		return
	}
	days := 30
	if v := r.URL.Query().Get("days"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 365 {
			days = n
		}
	}
	since := time.Now().AddDate(0, 0, -days)
	sum, err := h.store.UsageForKey(r.Context(), id.KeyHash, since)
	if err != nil {
		http.Error(w, `{"error":{"message":"usage unavailable"}}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"since": since.UTC().Format(time.RFC3339),
		"usage": sum,
	})
}
