package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeRedis lets tests drive the remote path without a server: it runs the
// same bucket arithmetic the Lua script does, and can be made to fail.
type fakeRedis struct {
	tokens map[string]float64
	last   map[string]time.Time
	err    error
	calls  int
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{tokens: map[string]float64{}, last: map[string]time.Time{}}
}

func (f *fakeRedis) Allow(_ context.Context, key string, p Plan, now time.Time) (bool, error) {
	f.calls++
	if f.err != nil {
		return false, f.err
	}
	t, seen := f.tokens[key]
	if !seen {
		t = float64(p.Burst)
		f.last[key] = now
	}
	elapsed := now.Sub(f.last[key]).Seconds()
	t = minF(float64(p.Burst), t+elapsed*p.RefillPerSec)
	f.last[key] = now
	if t < 1 {
		f.tokens[key] = t
		return false, nil
	}
	f.tokens[key] = t - 1
	return true, nil
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

var testPlan = Plan{Burst: 3, RefillPerSec: 1}

func TestAllowsUpToBurstThenRejects(t *testing.T) {
	l := New(newFakeRedis())
	now := time.Now()
	for i := 0; i < 3; i++ {
		ok, err := l.Allow(context.Background(), "k", testPlan, now)
		if err != nil || !ok {
			t.Fatalf("request %d: ok=%v err=%v, want allowed", i+1, ok, err)
		}
	}
	ok, err := l.Allow(context.Background(), "k", testPlan, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("4th request within the burst window should be rejected")
	}
}

func TestRefillsOverTime(t *testing.T) {
	l := New(newFakeRedis())
	now := time.Now()
	for i := 0; i < 3; i++ {
		_, _ = l.Allow(context.Background(), "k", testPlan, now)
	}
	// Two seconds later, two tokens have dripped back in.
	later := now.Add(2 * time.Second)
	for i := 0; i < 2; i++ {
		ok, _ := l.Allow(context.Background(), "k", testPlan, later)
		if !ok {
			t.Fatalf("refilled request %d should be allowed", i+1)
		}
	}
	if ok, _ := l.Allow(context.Background(), "k", testPlan, later); ok {
		t.Fatal("only two tokens should have refilled")
	}
}

func TestBucketsArePerKey(t *testing.T) {
	l := New(newFakeRedis())
	now := time.Now()
	for i := 0; i < 3; i++ {
		_, _ = l.Allow(context.Background(), "a", testPlan, now)
	}
	if ok, _ := l.Allow(context.Background(), "a", testPlan, now); ok {
		t.Fatal("key a should be exhausted")
	}
	if ok, _ := l.Allow(context.Background(), "b", testPlan, now); !ok {
		t.Fatal("key b must have its own bucket")
	}
}

func TestFallsBackToLocalWhenRedisFails(t *testing.T) {
	f := newFakeRedis()
	f.err = errors.New("redis down")
	l := New(f)
	now := time.Now()

	// Availability over strict accuracy (ADR-006): traffic keeps flowing,
	// enforced by an in-process bucket instead.
	for i := 0; i < 3; i++ {
		ok, err := l.Allow(context.Background(), "k", testPlan, now)
		if err != nil {
			t.Fatalf("fallback must not surface an error: %v", err)
		}
		if !ok {
			t.Fatalf("request %d should be allowed by the local bucket", i+1)
		}
	}
	if ok, _ := l.Allow(context.Background(), "k", testPlan, now); ok {
		t.Fatal("local fallback must still enforce the burst limit")
	}
	if !l.UsingFallback() {
		t.Fatal("limiter should report that it is degraded")
	}
}

func TestReturnsToRedisWhenItRecovers(t *testing.T) {
	f := newFakeRedis()
	f.err = errors.New("redis down")
	l := New(f)
	now := time.Now()
	_, _ = l.Allow(context.Background(), "k", testPlan, now)
	if !l.UsingFallback() {
		t.Fatal("should be in fallback")
	}

	f.err = nil
	callsBefore := f.calls
	_, _ = l.Allow(context.Background(), "k", testPlan, now)
	if f.calls == callsBefore {
		t.Fatal("limiter should retry Redis rather than staying local forever")
	}
	if l.UsingFallback() {
		t.Fatal("should have left fallback after a successful Redis call")
	}
}
