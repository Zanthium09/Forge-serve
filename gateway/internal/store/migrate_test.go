package store_test

import (
	"context"
	"sync"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/store"
	"github.com/Zanthium09/forge-serve/gateway/internal/store/storetest"
)

func TestMigrateCreatesSchema(t *testing.T) {
	st := storetest.Open(t)
	want := []string{"teams", "users", "api_keys", "model_grants", "usage_events", "admin_audit", "schema_migrations"}
	for _, tbl := range want {
		var n int
		err := st.DB().QueryRow(`SELECT count(*) FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1`, tbl).Scan(&n)
		if err != nil || n != 1 {
			t.Fatalf("table %s missing (n=%d err=%v)", tbl, n, err)
		}
	}
	var v int
	if err := st.DB().QueryRow(`SELECT max(version) FROM schema_migrations`).Scan(&v); err != nil || v != 1 {
		t.Fatalf("version = %d err = %v", v, err)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	st := storetest.Open(t)
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = st.DB().QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&n)
	if n != 1 {
		t.Fatalf("schema_migrations rows = %d, want 1", n)
	}
}

// Review Focus 5: two gateway copies starting together on a fresh DB.
func TestMigrateConcurrentCopies(t *testing.T) {
	dsn := storetest.Schema(t)
	a, err := store.New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.New(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close(); _ = b.Close() }()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, s := range []*store.Store{a, b} {
		wg.Add(1)
		go func(i int, s *store.Store) {
			defer wg.Done()
			errs[i] = s.Migrate(context.Background())
		}(i, s)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("errs = %v", errs)
	}
	var n int
	_ = a.DB().QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&n)
	if n != 1 {
		t.Fatalf("schema_migrations rows = %d, want exactly 1", n)
	}
}

func TestEmailUniqueCaseInsensitive(t *testing.T) {
	st := storetest.Open(t)
	if _, err := st.DB().Exec(`INSERT INTO users (email) VALUES ('A@x.com')`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`INSERT INTO users (email) VALUES ('a@x.com')`); err == nil {
		t.Fatal("case-variant email must be rejected")
	}
}

func TestModelGrantNeedsExactlyOneTarget(t *testing.T) {
	st := storetest.Open(t)
	db := st.DB()
	var uid, tid int64
	_ = db.QueryRow(`INSERT INTO teams (name) VALUES ('ml') RETURNING id`).Scan(&tid)
	_ = db.QueryRow(`INSERT INTO users (email) VALUES ('u@x.com') RETURNING id`).Scan(&uid)
	if _, err := db.Exec(`INSERT INTO model_grants (model, user_id, team_id) VALUES ('big', $1, $2)`, uid, tid); err == nil {
		t.Fatal("grant with both user and team must be rejected")
	}
	if _, err := db.Exec(`INSERT INTO model_grants (model) VALUES ('big')`); err == nil {
		t.Fatal("grant with neither user nor team must be rejected")
	}
}
