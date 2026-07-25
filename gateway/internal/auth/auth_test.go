package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeStore struct {
	keys  map[string]int64 // hashHex -> tenant
	calls int
	err   error
}

func (f *fakeStore) TenantIDByKeyHash(_ context.Context, h string) (int64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	id, ok := f.keys[h]
	if !ok {
		return 0, ErrUnknownKey
	}
	return id, nil
}

// sha256 hex of "sk-test" — precomputed so the test states its expectation.
const skTestHash = "f3abf2a6cc4f00987743db5f544ba345b4899ae31f326d8ee9c4816de153c9e0"

func do(t *testing.T, a *Authenticator, key string) int {
	t.Helper()
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestMissingKeyIs401(t *testing.T) {
	a := New(&fakeStore{}, time.Minute)
	if code := do(t, a, ""); code != http.StatusUnauthorized {
		t.Fatalf("code = %d", code)
	}
}

func TestUnknownKeyIs401(t *testing.T) {
	a := New(&fakeStore{keys: map[string]int64{}}, time.Minute)
	if code := do(t, a, "sk-wrong"); code != http.StatusUnauthorized {
		t.Fatalf("code = %d", code)
	}
}

func TestValidKeyPassesAndIsCached(t *testing.T) {
	fs := &fakeStore{keys: map[string]int64{skTestHash: 1}}
	a := New(fs, time.Minute)
	if code := do(t, a, "sk-test"); code != http.StatusOK {
		t.Fatalf("first call code = %d", code)
	}
	if code := do(t, a, "sk-test"); code != http.StatusOK {
		t.Fatalf("second call code = %d", code)
	}
	if fs.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (second hit must come from cache)", fs.calls)
	}
}

func TestStoreErrorIs503(t *testing.T) {
	a := New(&fakeStore{err: errors.New("db down")}, time.Minute)
	if code := do(t, a, "sk-test"); code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", code)
	}
}
