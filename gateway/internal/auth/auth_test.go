package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu    sync.Mutex
	keys  map[string]KeyRecord // hashHex -> record
	calls int
	err   error
}

func (f *fakeStore) LookupKeyForAuth(_ context.Context, h string) (KeyRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return KeyRecord{}, f.err
	}
	rec, ok := f.keys[h]
	if !ok {
		return KeyRecord{}, ErrUnknownKey
	}
	return rec, nil
}

func (f *fakeStore) set(h string, rec KeyRecord) {
	f.mu.Lock()
	f.keys[h] = rec
	f.mu.Unlock()
}

// sha256 hex of "sk-test" — precomputed so the test states its expectation.
const skTestHash = "f3abf2a6cc4f00987743db5f544ba345b4899ae31f326d8ee9c4816de153c9e0"

var activeRec = KeyRecord{KeyID: 7, UserID: 3, Role: "member", Active: true, RPMBurst: 60, RPMRefillPerSec: 10}

func newStore(recs map[string]KeyRecord) *fakeStore { return &fakeStore{keys: recs} }

// do sends one request through the middleware; the inner handler records
// the identity it saw.
func do(t *testing.T, h http.Handler, key string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func okHandler(seen *Identity) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seen != nil {
			*seen, _ = FromContext(r.Context())
		}
		w.WriteHeader(http.StatusOK)
	})
}

func TestMissingKeyIs401(t *testing.T) {
	a := New(newStore(nil), time.Minute, 0, nil)
	if code := do(t, a.Middleware(okHandler(nil)), ""); code != http.StatusUnauthorized {
		t.Fatalf("code = %d", code)
	}
}

func TestUnknownKeyIs401(t *testing.T) {
	a := New(newStore(map[string]KeyRecord{}), time.Minute, 0, nil)
	if code := do(t, a.Middleware(okHandler(nil)), "sk-wrong"); code != http.StatusUnauthorized {
		t.Fatalf("code = %d", code)
	}
}

func TestValidKeyPassesAndIsCached(t *testing.T) {
	fs := newStore(map[string]KeyRecord{skTestHash: activeRec})
	var seen Identity
	h := New(fs, time.Minute, 0, nil).Middleware(okHandler(&seen))
	for i := 0; i < 2; i++ {
		if code := do(t, h, "sk-test"); code != 200 {
			t.Fatalf("call %d code = %d", i, code)
		}
	}
	if fs.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (second hit must come from cache)", fs.calls)
	}
	if seen.UserID != 3 || seen.KeyID != 7 || seen.KeyHash != skTestHash || seen.RPMBurst != 60 {
		t.Fatalf("identity = %+v", seen)
	}
}

func TestStoreErrorIs503(t *testing.T) {
	a := New(&fakeStore{err: errors.New("db down")}, time.Minute, 0, nil)
	if code := do(t, a.Middleware(okHandler(nil)), "sk-test"); code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", code)
	}
}

func TestRevokedExpiredDisabledAre401(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	cases := map[string]KeyRecord{
		"revoked":  {KeyID: 1, UserID: 1, Active: true, Revoked: true},
		"expired":  {KeyID: 1, UserID: 1, Active: true, ExpiresAt: &past},
		"disabled": {KeyID: 1, UserID: 1, Active: false},
	}
	for name, rec := range cases {
		a := New(newStore(map[string]KeyRecord{skTestHash: rec}), time.Minute, 0, nil)
		if code := do(t, a.Middleware(okHandler(nil)), "sk-test"); code != http.StatusUnauthorized {
			t.Fatalf("%s: code = %d, want 401", name, code)
		}
	}
}

func TestCacheTTLThenRevocationTakesEffect(t *testing.T) {
	fs := newStore(map[string]KeyRecord{skTestHash: activeRec})
	h := New(fs, 50*time.Millisecond, 0, nil).Middleware(okHandler(nil))
	if do(t, h, "sk-test") != 200 {
		t.Fatal("first call rejected")
	}
	revoked := activeRec
	revoked.Revoked = true
	fs.set(skTestHash, revoked)
	if do(t, h, "sk-test") != 200 {
		t.Fatal("cached entry should still be served within the TTL")
	}
	time.Sleep(60 * time.Millisecond)
	if code := do(t, h, "sk-test"); code != http.StatusUnauthorized {
		t.Fatalf("after TTL code = %d, want 401", code)
	}
}

func TestEvictUserDropsCachedKeys(t *testing.T) {
	fs := newStore(map[string]KeyRecord{skTestHash: activeRec})
	a := New(fs, time.Hour, 0, nil)
	h := a.Middleware(okHandler(nil))
	do(t, h, "sk-test")
	a.EvictUser(3)
	do(t, h, "sk-test")
	if fs.calls != 2 {
		t.Fatalf("store calls = %d, want 2 after EvictUser", fs.calls)
	}
	a.Evict(skTestHash)
	do(t, h, "sk-test")
	if fs.calls != 3 {
		t.Fatalf("store calls = %d, want 3 after Evict", fs.calls)
	}
}

func TestEffectivePriorityDefault(t *testing.T) {
	var seen Identity
	h := New(newStore(map[string]KeyRecord{skTestHash: activeRec}), time.Minute, 2, nil).Middleware(okHandler(&seen))
	do(t, h, "sk-test")
	if seen.Priority != 2 {
		t.Fatalf("priority = %d, want config default 2", seen.Priority)
	}
	one := 1
	withPrio := activeRec
	withPrio.Priority = &one
	h = New(newStore(map[string]KeyRecord{skTestHash: withPrio}), time.Minute, 2, nil).Middleware(okHandler(&seen))
	do(t, h, "sk-test")
	if seen.Priority != 1 {
		t.Fatalf("priority = %d, want the record's 1", seen.Priority)
	}
}

func TestRequireAdmin(t *testing.T) {
	member := New(newStore(map[string]KeyRecord{skTestHash: activeRec}), time.Minute, 0, nil)
	if code := do(t, member.Middleware(member.RequireAdmin(okHandler(nil))), "sk-test"); code != http.StatusForbidden {
		t.Fatalf("member code = %d, want 403", code)
	}
	adminRec := activeRec
	adminRec.Role = "admin"
	admin := New(newStore(map[string]KeyRecord{skTestHash: adminRec}), time.Minute, 0, nil)
	if code := do(t, admin.Middleware(admin.RequireAdmin(okHandler(nil))), "sk-test"); code != http.StatusOK {
		t.Fatalf("admin code = %d, want 200", code)
	}
}

func TestTouchAtMostOncePerMinute(t *testing.T) {
	var touched []int64
	a := New(newStore(map[string]KeyRecord{skTestHash: activeRec}), time.Minute, 0,
		func(id int64) { touched = append(touched, id) })
	h := a.Middleware(okHandler(nil))
	for i := 0; i < 3; i++ {
		do(t, h, "sk-test")
	}
	if len(touched) != 1 || touched[0] != 7 {
		t.Fatalf("touched = %v, want [7]", touched)
	}
}

func TestCanUse(t *testing.T) {
	id := Identity{Grants: map[string]bool{"big": true}}
	if !id.CanUse("small", false) || !id.CanUse("big", true) || id.CanUse("secret", true) {
		t.Fatal("CanUse wrong")
	}
}
