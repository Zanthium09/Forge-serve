// Package ratelimit enforces per-key token buckets.
//
// The bucket lives in Redis so several gateway replicas share one view of
// a tenant's quota. The read-modify-write is done inside a Lua script,
// which Redis runs atomically: a naive GET/SET pair would let two gateways
// read the same token count and both spend it, so a 10 req/s limit would
// leak to 20 under concurrency.
//
// If Redis is unreachable the limiter degrades to an in-process bucket
// rather than failing requests: availability over strict accuracy
// (ADR-006).
package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Plan is a tenant's quota. Burst is the bucket size (how much a client
// may spend at once); RefillPerSec is the sustained rate.
type Plan struct {
	Burst        int
	RefillPerSec float64
}

// Backend is the shared (Redis) implementation, injected so the limiter's
// policy can be tested without a server.
type Backend interface {
	Allow(ctx context.Context, key string, p Plan, now time.Time) (bool, error)
}

type Limiter struct {
	backend Backend

	mu       sync.Mutex
	fallback map[string]*localBucket
	degraded bool
}

func New(b Backend) *Limiter {
	return &Limiter{backend: b, fallback: map[string]*localBucket{}}
}

// Allow spends one token for key. It never returns an error for a Redis
// outage - that path is handled by the local fallback.
func (l *Limiter) Allow(ctx context.Context, key string, p Plan, now time.Time) (bool, error) {
	if l.backend != nil {
		ok, err := l.backend.Allow(ctx, key, p, now)
		if err == nil {
			l.setDegraded(false)
			return ok, nil
		}
		// Fall through to the local bucket. Every call retries Redis first,
		// so recovery is automatic and needs no health checker.
		l.setDegraded(true)
	}
	return l.allowLocal(key, p, now), nil
}

// UsingFallback reports whether the last decision came from the local
// bucket (i.e. Redis is down). Surfaced as a metric so the degradation is
// visible rather than silent.
func (l *Limiter) UsingFallback() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.degraded
}

func (l *Limiter) setDegraded(v bool) {
	l.mu.Lock()
	l.degraded = v
	l.mu.Unlock()
}

// localBucket is the in-process fallback. Per gateway replica, so N
// replicas allow up to N times the configured rate while Redis is down -
// the accuracy cost we accept for staying up (ADR-006).
type localBucket struct {
	tokens float64
	last   time.Time
}

func (l *Limiter) allowLocal(key string, p Plan, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.fallback[key]
	if !ok {
		b = &localBucket{tokens: float64(p.Burst), last: now}
		l.fallback[key] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * p.RefillPerSec
		if b.tokens > float64(p.Burst) {
			b.tokens = float64(p.Burst)
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
