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

// Identity is who the caller is plus what they are allowed, resolved once
// per request and carried in the context so downstream handlers (rate
// limiting, usage accounting) need no second lookup.
type Identity struct {
	TenantID     int64
	KeyHash      string
	Burst        int
	RefillPerSec float64
}

type ctxKey struct{}

// FromContext returns the authenticated identity, if any.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// Store resolves a key hash to its record. Implemented by internal/store.
type Store interface {
	TenantIDByKeyHash(ctx context.Context, hashHex string) (int64, error)
}

// PlanStore is the richer lookup (identity + quota). Optional: a Store
// that does not implement it still authenticates, with default quota.
type PlanStore interface {
	LookupKeyForAuth(ctx context.Context, hashHex string) (KeyRecord, error)
}

// KeyRecord mirrors store.KeyRecord without importing it (the dependency
// runs the other way: store imports auth for ErrUnknownKey).
type KeyRecord struct {
	TenantID     int64
	Burst        int
	RefillPerSec float64
}

type cacheEntry struct {
	id      Identity
	expires time.Time
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
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, e.id)))
			return
		}

		id, err := a.lookup(r.Context(), hash)
		switch {
		case errors.Is(err, ErrUnknownKey):
			http.Error(w, `{"error":{"message":"invalid api key"}}`, http.StatusUnauthorized)
			return
		case err != nil:
			http.Error(w, `{"error":{"message":"auth backend unavailable"}}`, http.StatusServiceUnavailable)
			return
		}

		a.mu.Lock()
		a.cache[hash] = cacheEntry{id: id, expires: time.Now().Add(a.ttl)}
		a.mu.Unlock()
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

func (a *Authenticator) lookup(ctx context.Context, hash string) (Identity, error) {
	if ps, ok := a.store.(PlanStore); ok {
		rec, err := ps.LookupKeyForAuth(ctx, hash)
		if err != nil {
			return Identity{}, err
		}
		return Identity{TenantID: rec.TenantID, KeyHash: hash,
			Burst: rec.Burst, RefillPerSec: rec.RefillPerSec}, nil
	}
	tid, err := a.store.TenantIDByKeyHash(ctx, hash)
	if err != nil {
		return Identity{}, err
	}
	// Defaults for stores without plan support (tests, fakes).
	return Identity{TenantID: tid, KeyHash: hash, Burst: 60, RefillPerSec: 10}, nil
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) || len(h) == len(prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}
