// Package auth authenticates API keys: SHA-256 the presented key, look the
// hex hash up in the store, cache hits briefly. Raw keys never touch disk
// or logs — same rationale as password hashing.
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

var ErrUnknownKey = errors.New("unknown api key")

type Store interface {
	TenantIDByKeyHash(ctx context.Context, hashHex string) (int64, error)
}

type cacheEntry struct {
	tenantID int64
	expires  time.Time
}

type Authenticator struct {
	store Store
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]cacheEntry
}

func New(s Store, ttl time.Duration) *Authenticator {
	return &Authenticator{store: s, ttl: ttl, cache: map[string]cacheEntry{}}
}

// Middleware guards /v1/* routes. Cache is keyed by hash so a raw key is
// never retained in memory longer than the request.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := bearer(r)
		if !ok {
			http.Error(w, `{"error":{"message":"missing bearer token"}}`, http.StatusUnauthorized)
			return
		}
		sum := sha256.Sum256([]byte(key))
		hash := hex.EncodeToString(sum[:])

		a.mu.Lock()
		e, hit := a.cache[hash]
		a.mu.Unlock()
		if hit && time.Now().Before(e.expires) {
			next.ServeHTTP(w, r)
			return
		}

		tenantID, err := a.store.TenantIDByKeyHash(r.Context(), hash)
		switch {
		case errors.Is(err, ErrUnknownKey):
			http.Error(w, `{"error":{"message":"invalid api key"}}`, http.StatusUnauthorized)
			return
		case err != nil:
			http.Error(w, `{"error":{"message":"auth backend unavailable"}}`, http.StatusServiceUnavailable)
			return
		}

		a.mu.Lock()
		a.cache[hash] = cacheEntry{tenantID: tenantID, expires: time.Now().Add(a.ttl)}
		a.mu.Unlock()
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
