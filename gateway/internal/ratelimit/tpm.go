package ratelimit

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"
)

// TPMBackend is the shared (Redis) tokens-per-minute bucket. Keys arrive as
// "u:<userID>"; the backend adds its own prefix.
type TPMBackend interface {
	TPMCheck(ctx context.Context, key string, perMinute int, now time.Time) (ok bool, waitSec int, err error)
	TPMDebit(ctx context.Context, key string, tokens, perMinute int, now time.Time) error
}

// TPMLimiter enforces tokens per minute per user (ADR-012): a request is
// admitted while the bucket is above zero, and its real size is debited
// when it ends, so the bucket may go negative and block until refilled.
// Redis errors fall back to per-replica buckets, as for requests (ADR-006).
type TPMLimiter struct {
	backend TPMBackend

	mu       sync.Mutex
	local    map[string]*tpmBucket
	degraded bool
}

type tpmBucket struct {
	tokens float64
	last   time.Time
}

func NewTPM(b TPMBackend) *TPMLimiter {
	return &TPMLimiter{backend: b, local: map[string]*tpmBucket{}}
}

func tpmKey(userID int64) string { return "u:" + strconv.FormatInt(userID, 10) }

// Check reports whether the user may start a request; retryAfter is the
// number of seconds until the bucket is positive again.
func (l *TPMLimiter) Check(ctx context.Context, userID int64, perMinute int, now time.Time) (ok bool, retryAfter int) {
	if perMinute <= 0 {
		return true, 0
	}
	key := tpmKey(userID)
	if l.backend != nil {
		ok, wait, err := l.backend.TPMCheck(ctx, key, perMinute, now)
		if err == nil {
			l.setDegraded(false)
			return ok, wait
		}
		l.setDegraded(true)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.refill(key, perMinute, now)
	if b.tokens > 0 {
		return true, 0
	}
	return false, tpmWait(b.tokens, perMinute)
}

// Debit charges a finished request's tokens.
func (l *TPMLimiter) Debit(ctx context.Context, userID int64, tokens, perMinute int, now time.Time) {
	if perMinute <= 0 || tokens <= 0 {
		return
	}
	key := tpmKey(userID)
	if l.backend != nil {
		if err := l.backend.TPMDebit(ctx, key, tokens, perMinute, now); err == nil {
			l.setDegraded(false)
			return
		}
		l.setDegraded(true)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill(key, perMinute, now).tokens -= float64(tokens)
}

// UsingFallback reports whether the last decision used the local bucket.
func (l *TPMLimiter) UsingFallback() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.degraded
}

func (l *TPMLimiter) setDegraded(v bool) {
	l.mu.Lock()
	l.degraded = v
	l.mu.Unlock()
}

// refill must be called with l.mu held.
func (l *TPMLimiter) refill(key string, perMinute int, now time.Time) *tpmBucket {
	b, ok := l.local[key]
	if !ok {
		b = &tpmBucket{tokens: float64(perMinute), last: now}
		l.local[key] = b
	}
	if el := now.Sub(b.last).Seconds(); el > 0 {
		b.tokens = math.Min(float64(perMinute), b.tokens+el*float64(perMinute)/60)
		b.last = now
	}
	return b
}

// tpmWait: seconds until tokens > 0 at perMinute/60 tokens per second.
func tpmWait(tokens float64, perMinute int) int {
	return int(math.Floor(-tokens/(float64(perMinute)/60))) + 1
}
