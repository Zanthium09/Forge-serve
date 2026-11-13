package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

// failingTPM simulates Redis being down.
type failingTPM struct{ calls int }

func (f *failingTPM) TPMCheck(context.Context, string, int, time.Time) (bool, int, error) {
	f.calls++
	return false, 0, errors.New("redis down")
}
func (f *failingTPM) TPMDebit(context.Context, string, int, int, time.Time) error {
	f.calls++
	return errors.New("redis down")
}

// tpmScenario: 60 tokens/min = 1 token/s. A 100-token request drives the
// bucket to -40, which blocks for 41 s, after which it is positive again.
func tpmScenario(t *testing.T, l *TPMLimiter, user int64) {
	t.Helper()
	ctx := context.Background()
	if ok, _ := l.Check(ctx, user, 60, t0); !ok {
		t.Fatal("full bucket must allow")
	}
	l.Debit(ctx, user, 100, 60, t0)
	ok, wait := l.Check(ctx, user, 60, t0)
	if ok || wait != 41 {
		t.Fatalf("after debit: ok = %v wait = %d, want false 41", ok, wait)
	}
	if ok, _ := l.Check(ctx, user, 60, t0.Add(40*time.Second)); ok {
		t.Fatal("bucket at exactly 0 must still block")
	}
	if ok, _ := l.Check(ctx, user, 60, t0.Add(41*time.Second)); !ok {
		t.Fatal("refilled bucket must allow")
	}
}

func TestTPMAllowsWhilePositiveThenBlocks(t *testing.T) {
	tpmScenario(t, NewTPM(nil), 1)
}

func TestTPMBucketsArePerUser(t *testing.T) {
	l := NewTPM(nil)
	l.Debit(context.Background(), 1, 1000, 60, t0)
	if ok, _ := l.Check(context.Background(), 2, 60, t0); !ok {
		t.Fatal("user 2 must not pay for user 1")
	}
}

func TestTPMUnlimited(t *testing.T) {
	f := &failingTPM{}
	l := NewTPM(f)
	l.Debit(context.Background(), 1, 1_000_000, 0, t0)
	if ok, _ := l.Check(context.Background(), 1, 0, t0); !ok || f.calls != 0 {
		t.Fatalf("unlimited user blocked or backend called (%d calls)", f.calls)
	}
}

func TestTPMFallsBackLocally(t *testing.T) {
	f := &failingTPM{}
	l := NewTPM(f)
	tpmScenario(t, l, 1)
	if !l.UsingFallback() || f.calls == 0 {
		t.Fatalf("fallback = %v calls = %d", l.UsingFallback(), f.calls)
	}
}
