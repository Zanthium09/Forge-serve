package budget

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeCounter struct {
	vals map[string]int64
	ttls map[string]time.Duration
	err  error
	ops  int
}

func newCounter() *fakeCounter {
	return &fakeCounter{vals: map[string]int64{}, ttls: map[string]time.Duration{}}
}

func (f *fakeCounter) Get(_ context.Context, k string) (int64, bool, error) {
	f.ops++
	if f.err != nil {
		return 0, false, f.err
	}
	v, ok := f.vals[k]
	return v, ok, nil
}

func (f *fakeCounter) SetNX(_ context.Context, k string, v int64, ttl time.Duration) error {
	f.ops++
	if f.err != nil {
		return f.err
	}
	if _, ok := f.vals[k]; !ok {
		f.vals[k], f.ttls[k] = v, ttl
	}
	return nil
}

func (f *fakeCounter) AddIfExists(_ context.Context, k string, n int64) error {
	f.ops++
	if f.err != nil {
		return f.err
	}
	if _, ok := f.vals[k]; ok {
		f.vals[k] += n
	}
	return nil
}

type fakeSource map[string]int64 // "user:7" -> tokens

func (f fakeSource) TokensUsed(_ context.Context, scope string, id int64, _ time.Time) (int64, error) {
	return f[scope+":"+itoa(id)], nil
}

var now = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

func newBudgets(c Counter, s Source) (*Budgets, *int) {
	b := New(c, s)
	skipped := 0
	b.onSkip = func() { skipped++ }
	return b, &skipped
}

func TestBudgetRebuildsFromPostgresOnMiss(t *testing.T) {
	b, _ := newBudgets(newCounter(), fakeSource{"user:7": 900})
	l := Limits{UserID: 7, Daily: 1000}
	if !b.Check(context.Background(), l, now) {
		t.Fatal("900 of 1000 used: must allow")
	}
	b.Add(context.Background(), l, 150, now)
	if b.Check(context.Background(), l, now) {
		t.Fatal("1050 of 1000 used: must reject")
	}
}

func TestBudgetTeamCapApplies(t *testing.T) {
	b, _ := newBudgets(newCounter(), fakeSource{"team:3": 100})
	if b.Check(context.Background(), Limits{UserID: 7, TeamID: 3, TeamMonthly: 100}, now) {
		t.Fatal("team at its cap: must reject")
	}
}

func TestBudgetUnlimitedSkipsCounters(t *testing.T) {
	c := newCounter()
	b, _ := newBudgets(c, fakeSource{})
	if !b.Check(context.Background(), Limits{UserID: 7, TeamID: 3}, now) || c.ops != 0 {
		t.Fatalf("unlimited user: ops = %d", c.ops)
	}
}

func TestBudgetRedisDownAllowsAndCounts(t *testing.T) {
	c := newCounter()
	c.err = errors.New("redis down")
	b, skipped := newBudgets(c, fakeSource{})
	if !b.Check(context.Background(), Limits{UserID: 7, Daily: 1}, now) || *skipped != 1 {
		t.Fatalf("skipped = %d", *skipped)
	}
}

func TestBudgetKeysAndTTL(t *testing.T) {
	c := newCounter()
	b, _ := newBudgets(c, fakeSource{})
	b.Check(context.Background(), Limits{UserID: 7, TeamID: 3, Daily: 10, Monthly: 10, TeamMonthly: 10}, now)
	for _, k := range []string{"forge:tok:u:7:d:2026-11-12", "forge:tok:u:7:m:2026-11", "forge:tok:t:3:m:2026-11"} {
		if _, ok := c.vals[k]; !ok {
			t.Fatalf("key %s missing; have %v", k, c.vals)
		}
	}
	if ttl := c.ttls["forge:tok:u:7:d:2026-11-12"]; ttl != 15*time.Hour {
		t.Fatalf("daily ttl = %v, want 15h", ttl)
	}
}

func TestRedisCounterAgainstRealRedis(t *testing.T) {
	u := os.Getenv("FORGE_TEST_REDIS_URL")
	if u == "" {
		t.Skip("FORGE_TEST_REDIS_URL not set")
	}
	opt, err := redis.ParseURL(u)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	defer func() { _ = rdb.Close() }()
	c := NewRedisCounter(rdb)
	r := make([]byte, 6)
	_, _ = rand.Read(r)
	k := "forge:tok:test:" + hex.EncodeToString(r)
	ctx := context.Background()

	if err := c.AddIfExists(ctx, k, 5); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(ctx, k); ok {
		t.Fatal("AddIfExists must not create a missing key")
	}
	_ = c.SetNX(ctx, k, 10, time.Minute)
	_ = c.SetNX(ctx, k, 99, time.Minute) // must not overwrite
	_ = c.AddIfExists(ctx, k, 5)
	if v, ok, err := c.Get(ctx, k); err != nil || !ok || v != 15 {
		t.Fatalf("v = %d ok = %v err = %v, want 15", v, ok, err)
	}
	_ = rdb.Del(ctx, k)
}
