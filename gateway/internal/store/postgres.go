// Package store is the gateway's Postgres access layer.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
)

type Store struct {
	db *sql.DB

	// usage events are queued and flushed in batches: accounting must
	// never sit on the request path.
	events chan UsageEvent
}

// usageBatch/usageFlush bound how long an event can sit unwritten and how
// many rows one INSERT carries.
const (
	usageBatch = 50
	usageFlush = 2 * time.Second
)

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
	return &Store{db: db, events: make(chan UsageEvent, 4096)}, nil
}

// KeyRecord is everything the request path needs about a key, fetched in
// one query: identity plus quota.
type KeyRecord struct {
	TenantID     int64
	Burst        int
	RefillPerSec float64
}

func (s *Store) LookupKey(ctx context.Context, hashHex string) (KeyRecord, error) {
	var k KeyRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT tenant_id, burst, refill_per_sec FROM api_keys WHERE key_hash = $1`,
		hashHex).Scan(&k.TenantID, &k.Burst, &k.RefillPerSec)
	if errors.Is(err, sql.ErrNoRows) {
		return k, auth.ErrUnknownKey
	}
	return k, err
}

// TenantIDByKeyHash satisfies auth.Store.
func (s *Store) TenantIDByKeyHash(ctx context.Context, hashHex string) (int64, error) {
	k, err := s.LookupKey(ctx, hashHex)
	return k.TenantID, err
}

// UsageEvent is one completed request's token accounting.
type UsageEvent struct {
	TenantID         int64
	KeyHash          string
	Model            string
	PromptTokens     int
	CompletionTokens int
}

// RecordUsage queues an event. It never blocks the caller: if the queue is
// full the event is dropped and counted, because billing data is worth
// less than the latency of the request that produced it.
func (s *Store) RecordUsage(ev UsageEvent) {
	select {
	case s.events <- ev:
	default:
		slog.Warn("usage queue full, dropping event", "tenant", ev.TenantID)
	}
}

// StartUsageWriter drains the queue into batched INSERTs until ctx ends.
func (s *Store) StartUsageWriter(ctx context.Context) {
	go func() {
		t := time.NewTicker(usageFlush)
		defer t.Stop()
		buf := make([]UsageEvent, 0, usageBatch)
		flush := func() {
			if len(buf) == 0 {
				return
			}
			if err := s.insertUsage(buf); err != nil {
				slog.Error("usage insert failed", "err", err, "rows", len(buf))
			}
			buf = buf[:0]
		}
		for {
			select {
			case <-ctx.Done():
				flush() // don't lose the tail on shutdown
				return
			case ev := <-s.events:
				buf = append(buf, ev)
				if len(buf) >= usageBatch {
					flush()
				}
			case <-t.C:
				flush()
			}
		}
	}()
}

func (s *Store) insertUsage(evs []UsageEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO usage_events
		   (tenant_id, key_hash, model, prompt_tokens, completion_tokens)
		 VALUES ($1,$2,$3,$4,$5)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, e := range evs {
		if _, err := stmt.ExecContext(ctx, e.TenantID, e.KeyHash, e.Model,
			e.PromptTokens, e.CompletionTokens); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UsageSummary is the /v1/usage response body.
type UsageSummary struct {
	Requests         int64 `json:"requests"`
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

func (s *Store) UsageForKey(ctx context.Context, hashHex string, since time.Time) (UsageSummary, error) {
	var u UsageSummary
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0)
		   FROM usage_events WHERE key_hash = $1 AND created_at >= $2`,
		hashHex, since).Scan(&u.Requests, &u.PromptTokens, &u.CompletionTokens)
	u.TotalTokens = u.PromptTokens + u.CompletionTokens
	return u, err
}
