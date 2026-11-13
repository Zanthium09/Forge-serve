package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrateLockID serialises migrations across gateway copies starting at
// the same time (Review Focus 5). Any constant works; it only has to be
// the same in every copy.
const migrateLockID = 727301

// Migrate applies embedded migrations/NNN_*.sql in order, each in its own
// transaction, recording versions in schema_migrations. Idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	// Advisory locks are per session, so the whole run uses one connection.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("migrate lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateLockID)
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version     INTEGER PRIMARY KEY,
		applied_at  TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	files, err := migrationFiles()
	if err != nil {
		return err
	}
	for _, f := range files {
		var done bool
		if err := conn.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, f.version).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		if err := apply(ctx, conn, f); err != nil {
			return fmt.Errorf("migration %s: %w", f.name, err)
		}
	}
	return nil
}

type migration struct {
	version int
	name    string
	sql     string
}

func migrationFiles() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var out []migration
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		v, err := strconv.Atoi(prefix)
		if !ok || err != nil {
			return nil, fmt.Errorf("migration %q: name must start with NNN_", e.Name())
		}
		b, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: v, name: e.Name(), sql: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

func apply(ctx context.Context, conn *sql.Conn, m migration) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// No arguments: pgx uses the simple protocol, which allows a
	// multi-statement script.
	if _, err := tx.ExecContext(ctx, m.sql); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, m.version); err != nil {
		return err
	}
	return tx.Commit()
}

// DB exposes the pool for this package's tests and admin queries.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Close() error { return s.db.Close() }
