// Package auth authenticates API keys: SHA-256 the presented key, look the
// hex hash up in the store, cache hits briefly. Raw keys never touch disk
// or logs — same rationale as password hashing. One identity per person
// (ADR-008); a key is only a handle to its owner.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

var ErrUnknownKey = errors.New("unknown api key")

// KeyRecord mirrors store.KeyRecord without importing it (store imports
// auth for ErrUnknownKey, so the dependency runs that way).
type KeyRecord struct {
	KeyID, UserID, TeamID int64
	KeyPrefix, Role       string
	CanViewUsage          bool
	Active, Revoked       bool
	ExpiresAt             *time.Time
	RPMBurst              int
	RPMRefillPerSec       float64
	TokensPerMinute       int
	DailyBudget           int64
	MonthlyBudget         int64
	TeamMonthlyCap        int64
	Priority              *int
	Grants                []string
}

// Store resolves a key hash to its record. Implemented by internal/store.
type Store interface {
	LookupKeyForAuth(ctx context.Context, hashHex string) (KeyRecord, error)
}

// Identity is who the caller is plus what they are allowed, resolved once
// per request and carried in the context so rate limiting, budgets and
// usage accounting need no second lookup.
type Identity struct {
	KeyID, UserID, TeamID int64
	KeyHash, KeyPrefix    string
	Role                  string
	CanViewUsage          bool
	RPMBurst              int
	RPMRefillPerSec       float64
	TokensPerMinute       int // 0 = unlimited
	DailyBudget           int64
	MonthlyBudget         int64
	TeamMonthlyCap        int64
	Priority              int // effective: user, else team, else config default
	Grants                map[string]bool
	expiresAt             *time.Time
}

// CanUse: unrestricted models are open to everyone; restricted ones need a
// grant to the user or their team.
func (id Identity) CanUse(model string, restricted bool) bool {
	return !restricted || id.Grants[model]
}

type ctxKey struct{}

// FromContext returns the authenticated identity, if any.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// WithIdentity is for tests in other packages that need an authenticated
// context without a store.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

type cacheEntry struct {
	id      Identity
	expires time.Time
}

// touchEvery limits how often a key's last_used_at is refreshed.
var touchEvery = time.Minute

type Authenticator struct {
	store           Store
	ttl             time.Duration
	defaultPriority int
	touch           func(keyID int64)

	mu      sync.Mutex
	cache   map[string]cacheEntry
	touched map[int64]time.Time
}

// New: ttl bounds how long a revoked key keeps working (spec §7.4,
// default 30s); touch, if non-nil, receives key ids for last_used_at.
func New(s Store, ttl time.Duration, defaultPriority int, touch func(keyID int64)) *Authenticator {
	return &Authenticator{store: s, ttl: ttl, defaultPriority: defaultPriority, touch: touch,
		cache: map[string]cacheEntry{}, touched: map[int64]time.Time{}}
}

func deny(w http.ResponseWriter, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"message": msg, "type": typ, "code": code},
	})
}

// Middleware guards /v1/* and /admin/*. The cache is keyed by hash so a raw
// key is never retained in memory longer than the request.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := bearer(r)
		if !ok {
			deny(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "missing bearer token")
			return
		}
		hash := HashKey(key)
		now := time.Now()

		a.mu.Lock()
		e, hit := a.cache[hash]
		a.mu.Unlock()
		id := e.id
		if !hit || !now.Before(e.expires) {
			rec, err := a.store.LookupKeyForAuth(r.Context(), hash)
			switch {
			case errors.Is(err, ErrUnknownKey):
				deny(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "invalid api key")
				return
			case err != nil:
				deny(w, http.StatusServiceUnavailable, "server_error", "auth_unavailable", "auth backend unavailable")
				return
			}
			if !rec.Active || rec.Revoked {
				deny(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "invalid api key")
				return
			}
			id = a.identity(hash, rec)
			a.mu.Lock()
			a.cache[hash] = cacheEntry{id: id, expires: now.Add(a.ttl)}
			a.mu.Unlock()
		}
		// Checked on every request, cached or not: expiry is a point in time.
		if id.expiresAt != nil && !now.Before(*id.expiresAt) {
			deny(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "api key expired")
			return
		}
		a.markUsed(id.KeyID, now)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

func (a *Authenticator) identity(hash string, rec KeyRecord) Identity {
	id := Identity{
		KeyID: rec.KeyID, UserID: rec.UserID, TeamID: rec.TeamID,
		KeyHash: hash, KeyPrefix: rec.KeyPrefix, Role: rec.Role, CanViewUsage: rec.CanViewUsage,
		RPMBurst: rec.RPMBurst, RPMRefillPerSec: rec.RPMRefillPerSec,
		TokensPerMinute: rec.TokensPerMinute, DailyBudget: rec.DailyBudget,
		MonthlyBudget: rec.MonthlyBudget, TeamMonthlyCap: rec.TeamMonthlyCap,
		Priority: a.defaultPriority, Grants: map[string]bool{}, expiresAt: rec.ExpiresAt,
	}
	if rec.Priority != nil {
		id.Priority = *rec.Priority
	}
	for _, g := range rec.Grants {
		id.Grants[g] = true
	}
	return id
}

func (a *Authenticator) markUsed(keyID int64, now time.Time) {
	if a.touch == nil {
		return
	}
	a.mu.Lock()
	last, seen := a.touched[keyID]
	fire := !seen || now.Sub(last) >= touchEvery
	if fire {
		a.touched[keyID] = now
	}
	a.mu.Unlock()
	if fire {
		a.touch(keyID)
	}
}

// Evict drops one key from the cache (used by key revocation).
func (a *Authenticator) Evict(keyHash string) {
	a.mu.Lock()
	delete(a.cache, keyHash)
	a.mu.Unlock()
}

// EvictUser drops every cached key of a user (disable, quota changes).
func (a *Authenticator) EvictUser(userID int64) {
	a.mu.Lock()
	for h, e := range a.cache {
		if e.id.UserID == userID {
			delete(a.cache, h)
		}
	}
	a.mu.Unlock()
}

// RequireAdmin must run inside Middleware.
func (a *Authenticator) RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := FromContext(r.Context())
		if !ok || id.Role != "admin" {
			deny(w, http.StatusForbidden, "permission_error", "admin_required", "admin role required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) || len(h) == len(prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}
