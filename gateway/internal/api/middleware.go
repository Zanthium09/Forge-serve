package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/budget"
	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
	"github.com/Zanthium09/forge-serve/gateway/internal/ratelimit"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
)

// Limiter is the request-rate dependency (interface so the handler tests
// can run without Redis).
type Limiter interface {
	Allow(ctx context.Context, key string, p ratelimit.Plan, now time.Time) (bool, error)
	UsingFallback() bool
}

func budgetLimits(id auth.Identity) budget.Limits {
	return budget.Limits{UserID: id.UserID, TeamID: id.TeamID,
		Daily: id.DailyBudget, Monthly: id.MonthlyBudget, TeamMonthly: id.TeamMonthlyCap}
}

// limits rejects over-quota requests before any model work happens, in
// order: request rate, tokens per minute, token budgets. It runs after
// auth, so every limit is the caller's own (keyed by user, ADR-008).
func limits(d Deps, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := auth.FromContext(r.Context())
		if !ok {
			next.ServeHTTP(w, r) // unauthenticated routes are not limited
			return
		}
		now := time.Now()
		if d.Limiter != nil {
			plan := ratelimit.Plan{Burst: id.RPMBurst, RefillPerSec: id.RPMRefillPerSec}
			allowed, _ := d.Limiter.Allow(r.Context(), "u:"+strconv.FormatInt(id.UserID, 10), plan, now)
			if d.Limiter.UsingFallback() {
				metrics.IncLimiterFallback()
			}
			if !allowed {
				metrics.IncRateLimited()
				// Retry-After from the refill rate: how long until one token
				// is back, so clients back off by exactly the right amount.
				wait := 1
				if plan.RefillPerSec > 0 {
					if s := int(1/plan.RefillPerSec + 0.999); s > 1 {
						wait = s
					}
				}
				w.Header().Set("Retry-After", strconv.Itoa(wait))
				reject(w, http.StatusTooManyRequests, "rate_limit_error", "rate_limited", "rate limit exceeded")
				return
			}
		}
		if d.TPM != nil {
			if ok, wait := d.TPM.Check(r.Context(), id.UserID, id.TokensPerMinute, now); !ok {
				metrics.IncRateLimited()
				w.Header().Set("Retry-After", strconv.Itoa(wait))
				reject(w, http.StatusTooManyRequests, "rate_limit_error", "tokens_per_minute", "tokens-per-minute limit exceeded")
				return
			}
		}
		if d.Budgets != nil && !d.Budgets.Check(r.Context(), budgetLimits(id), now) {
			reject(w, http.StatusTooManyRequests, "insufficient_quota", "budget_exceeded", "token budget exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// UsageReporter is the read side of usage accounting.
type UsageReporter interface {
	UsageForUser(ctx context.Context, userID int64, since time.Time) (store.UsageReport, error)
}

type usageHandler struct {
	store UsageReporter
}

// ServeHTTP answers GET /v1/usage for the calling person. Scoped to the
// caller by construction: there is no user parameter to forge.
func (h *usageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id, ok := auth.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "unauthenticated")
		return
	}
	days := 30
	if v := r.URL.Query().Get("days"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 365 {
			days = n
		}
	}
	since := time.Now().AddDate(0, 0, -days)
	rep, err := h.store.UsageForUser(r.Context(), id.UserID, since)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "server_error", "usage_unavailable", "usage unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"since": since.UTC().Format(time.RFC3339),
		"usage": rep,
	})
}
