// Package storetest gives each test its own Postgres schema, migrated,
// and drops it afterwards. Tests skip when FORGE_TEST_DATABASE_URL is
// unset, so `go test ./...` stays runnable without a database.
package storetest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/Zanthium09/forge-serve/gateway/internal/store"
)

// Schema creates an empty schema and returns a DSN whose search_path
// points at it. The schema is dropped when the test ends.
func Schema(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("FORGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("FORGE_TEST_DATABASE_URL not set")
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "t_" + hex.EncodeToString(b)

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		_ = admin.Close()
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema) // pgx passes unknown params as runtime params
	u.RawQuery = q.Encode()
	return u.String()
}

// Open returns a migrated store on a fresh schema.
func Open(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.New(Schema(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return st
}
