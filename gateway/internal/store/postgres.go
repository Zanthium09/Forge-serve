// Package store is the gateway's Postgres access layer.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
)

type Store struct {
	db *sql.DB
}

// New opens and pings the database. Fail fast at boot: a gateway that
// cannot authenticate anyone should not report healthy.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) TenantIDByKeyHash(ctx context.Context, hashHex string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx,
		`SELECT tenant_id FROM api_keys WHERE key_hash = $1`, hashHex).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, auth.ErrUnknownKey
	}
	return id, err
}
