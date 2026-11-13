package store_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/store/storetest"
)

func mustID(t *testing.T, db *sql.DB, q string, args ...any) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(q, args...).Scan(&id); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return id
}

func TestLookupKeyJoinsUserTeamAndGrants(t *testing.T) {
	st := storetest.Open(t)
	db := st.DB()
	team := mustID(t, db, `INSERT INTO teams (name, priority, monthly_token_cap) VALUES ('ml', 3, 1000) RETURNING id`)
	user := mustID(t, db, `INSERT INTO users (email, team_id, tokens_per_minute) VALUES ('u@x.com', $1, 500) RETURNING id`, team)
	key := mustID(t, db, `INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES ($1, 'h1', 'abcd1234') RETURNING id`, user)
	mustID(t, db, `INSERT INTO model_grants (model, team_id) VALUES ('big', $1) RETURNING id`, team)

	rec, err := st.LookupKey(context.Background(), "h1")
	if err != nil {
		t.Fatal(err)
	}
	if rec.KeyID != key || rec.UserID != user || rec.TeamID != team || rec.KeyPrefix != "abcd1234" {
		t.Fatalf("ids = %+v", rec)
	}
	if rec.TeamMonthlyCap != 1000 || rec.Priority == nil || *rec.Priority != 3 || rec.TokensPerMinute != 500 {
		t.Fatalf("limits = %+v", rec)
	}
	if !reflect.DeepEqual(rec.Grants, []string{"big"}) || !rec.Active || rec.Revoked || rec.Role != "member" {
		t.Fatalf("access = %+v", rec)
	}
	if rec.RPMBurst != 60 || rec.RPMRefillPerSec != 10 {
		t.Fatalf("schema defaults = %d %v", rec.RPMBurst, rec.RPMRefillPerSec)
	}
}

func TestLookupKeyUserPriorityWins(t *testing.T) {
	st := storetest.Open(t)
	db := st.DB()
	team := mustID(t, db, `INSERT INTO teams (name, priority) VALUES ('ml', 3) RETURNING id`)
	user := mustID(t, db, `INSERT INTO users (email, team_id, priority) VALUES ('u@x.com', $1, 1) RETURNING id`, team)
	mustID(t, db, `INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES ($1, 'h1', 'p') RETURNING id`, user)
	rec, err := st.LookupKey(context.Background(), "h1")
	if err != nil || rec.Priority == nil || *rec.Priority != 1 {
		t.Fatalf("rec = %+v err = %v", rec, err)
	}
}

func TestLookupKeyUnknown(t *testing.T) {
	st := storetest.Open(t)
	if _, err := st.LookupKey(context.Background(), "nope"); !errors.Is(err, auth.ErrUnknownKey) {
		t.Fatalf("err = %v, want ErrUnknownKey", err)
	}
}

func TestLookupKeyReportsRevokedAndDisabled(t *testing.T) {
	st := storetest.Open(t)
	db := st.DB()
	user := mustID(t, db, `INSERT INTO users (email, status) VALUES ('u@x.com', 'disabled') RETURNING id`)
	mustID(t, db, `INSERT INTO api_keys (user_id, key_hash, key_prefix, revoked_at) VALUES ($1, 'h1', 'p', now()) RETURNING id`, user)
	rec, err := st.LookupKey(context.Background(), "h1")
	if err != nil || !rec.Revoked || rec.Active || rec.TeamID != 0 || rec.Priority != nil {
		t.Fatalf("rec = %+v err = %v", rec, err)
	}
}
