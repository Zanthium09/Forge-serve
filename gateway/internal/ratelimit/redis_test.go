package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
)

// realRedis connects to FORGE_TEST_REDIS_URL or skips. Keys are random so
// parallel runs do not collide.
func realRedis(t *testing.T) (*RedisBackend, string) {
	t.Helper()
	u := os.Getenv("FORGE_TEST_REDIS_URL")
	if u == "" {
		t.Skip("FORGE_TEST_REDIS_URL not set")
	}
	b, err := NewRedisBackend(u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	r := make([]byte, 6)
	_, _ = rand.Read(r)
	return b, "test-" + hex.EncodeToString(r)
}

func TestRedisScriptsAgainstRealRedis(t *testing.T) {
	b, key := realRedis(t)
	ctx := context.Background()
	plan := Plan{Burst: 2, RefillPerSec: 0.001}
	var got []bool
	for i := 0; i < 3; i++ {
		ok, err := b.Allow(ctx, key, plan, t0)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ok)
	}
	if !got[0] || !got[1] || got[2] {
		t.Fatalf("request bucket = %v, want allow allow deny", got)
	}

	// The TPM scenario must give identical answers through Lua.
	l := NewTPM(b)
	tpmScenario(t, l, int64(r64(t)))
	if l.UsingFallback() {
		t.Fatal("real Redis must not trigger the local fallback")
	}
}

// r64 is a random positive user id so reruns never share a TPM bucket.
func r64(t *testing.T) uint32 {
	t.Helper()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])>>1
}
