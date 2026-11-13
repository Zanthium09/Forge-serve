package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/store"
	"github.com/Zanthium09/forge-serve/gateway/internal/store/storetest"
)

// runBackground starts the writers and returns a func that stops them and
// waits for the final flush (cancelling flushes the tail).
func runBackground(t *testing.T, st *store.Store) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := st.StartBackground(ctx)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("background writers did not stop")
		}
	}
}

func TestUsageWriterAndReports(t *testing.T) {
	st := storetest.Open(t)
	db := st.DB()
	user := mustID(t, db, `INSERT INTO users (email) VALUES ('u@x.com') RETURNING id`)
	key := mustID(t, db, `INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES ($1, 'h1', 'p') RETURNING id`, user)

	stop := runBackground(t, st)
	st.RecordUsage(store.UsageEvent{UserID: user, KeyID: key, Model: "a", Endpoint: "chat", PromptTokens: 5, CompletionTokens: 10})
	st.RecordUsage(store.UsageEvent{UserID: user, KeyID: key, Model: "b", RequestedModel: "a", Endpoint: "chat",
		PromptTokens: 1, CompletionTokens: 2, Estimated: true})
	stop()

	n, err := st.TokensUsed(context.Background(), "user", user, time.Unix(0, 0))
	if err != nil || n != 18 {
		t.Fatalf("TokensUsed = %d err = %v, want 18", n, err)
	}
	rep, err := st.UsageForUser(context.Background(), user, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Requests != 2 || rep.TotalTokens != 18 || len(rep.ByModel) != 2 {
		t.Fatalf("report = %+v", rep)
	}
	var est bool
	var req string
	_ = db.QueryRow(`SELECT estimated, requested_model FROM usage_events WHERE model = 'b'`).Scan(&est, &req)
	if !est || req != "a" {
		t.Fatalf("estimated = %v requested_model = %q", est, req)
	}
}

func TestMarkKeyUsedSetsLastUsed(t *testing.T) {
	st := storetest.Open(t)
	db := st.DB()
	user := mustID(t, db, `INSERT INTO users (email) VALUES ('u@x.com') RETURNING id`)
	key := mustID(t, db, `INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES ($1, 'h1', 'p') RETURNING id`, user)

	stop := runBackground(t, st)
	st.MarkKeyUsed(key)
	stop()

	var used bool
	_ = db.QueryRow(`SELECT last_used_at IS NOT NULL FROM api_keys WHERE id = $1`, key).Scan(&used)
	if !used {
		t.Fatal("last_used_at not set after flush")
	}
}
