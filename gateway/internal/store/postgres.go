// Package store is the gateway's Postgres access layer. The gateway owns
// its identity data (ADR-015); see migrations/ for the schema.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"
)

type Store struct {
	db *sql.DB

	// Usage events and key "last used" marks are queued and written in
	// batches: accounting must never sit on the request path.
	events chan UsageEvent
	used   chan int64
}

// usageBatch/usageFlush bound how long an event can sit unwritten and how
// many rows one transaction carries. lastUsedFlush is a var so tests can
// shorten it; once a minute is plenty for an "inactive users" report.
const (
	usageBatch = 50
	usageFlush = 2 * time.Second
)

var lastUsedFlush = time.Minute

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
	return &Store{db: db, events: make(chan UsageEvent, 4096), used: make(chan int64, 4096)}, nil
}

// Ping reports whether the database answers (readiness probe).
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// UsageEvent is one completed request's token accounting. Counts only,
// never text.
type UsageEvent struct {
	UserID, TeamID, KeyID int64  // TeamID 0 = no team
	Model                 string // model actually served
	RequestedModel        string // set only when a fallback served the request
	Endpoint              string // chat | completions | embeddings
	PromptTokens          int
	CompletionTokens      int
	Estimated             bool
}

// RecordUsage queues an event. It never blocks the caller: if the queue is
// full the event is dropped and logged, because billing data is worth less
// than the latency of the request that produced it.
func (s *Store) RecordUsage(ev UsageEvent) {
	select {
	case s.events <- ev:
	default:
		slog.Warn("usage queue full, dropping event", "user", ev.UserID)
	}
}

// MarkKeyUsed queues a last_used_at update; non-blocking like RecordUsage.
func (s *Store) MarkKeyUsed(id int64) {
	select {
	case s.used <- id:
	default:
	}
}

// StartBackground runs the usage and last-used writers until ctx ends,
// flushing what is buffered on the way out. The returned channel closes
// once the final flush is done.
func (s *Store) StartBackground(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ut := time.NewTicker(usageFlush)
		lt := time.NewTicker(lastUsedFlush)
		defer ut.Stop()
		defer lt.Stop()
		buf := make([]UsageEvent, 0, usageBatch)
		used := map[int64]bool{}
		flushUsage := func() {
			if len(buf) == 0 {
				return
			}
			if err := s.insertUsage(buf); err != nil {
				slog.Error("usage insert failed", "err", err, "rows", len(buf))
			}
			buf = buf[:0]
		}
		flushUsed := func() {
			if len(used) == 0 {
				return
			}
			ids := make([]int64, 0, len(used))
			for id := range used {
				ids = append(ids, id)
			}
			if err := s.markUsed(ids); err != nil {
				slog.Error("last_used update failed", "err", err)
			}
			used = map[int64]bool{}
		}
		for {
			select {
			case <-ctx.Done():
				for drained := false; !drained; {
					select {
					case ev := <-s.events:
						buf = append(buf, ev)
					case id := <-s.used:
						used[id] = true
					default:
						drained = true
					}
				}
				flushUsage() // don't lose the tail on shutdown
				flushUsed()
				return
			case ev := <-s.events:
				buf = append(buf, ev)
				if len(buf) >= usageBatch {
					flushUsage()
				}
			case id := <-s.used:
				used[id] = true
			case <-ut.C:
				flushUsage()
			case <-lt.C:
				flushUsed()
			}
		}
	}()
	return done
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
		   (user_id, team_id, key_id, model, requested_model, endpoint,
		    prompt_tokens, completion_tokens, estimated)
		 VALUES ($1, NULLIF($2, 0), $3, $4, NULLIF($5, ''), $6, $7, $8, $9)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, e := range evs {
		if _, err := stmt.ExecContext(ctx, e.UserID, e.TeamID, e.KeyID, e.Model, e.RequestedModel,
			e.Endpoint, e.PromptTokens, e.CompletionTokens, e.Estimated); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) markUsed(ids []int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.db.ExecContext(ctx, `UPDATE api_keys SET last_used_at = now() WHERE id = ANY($1)`, ids)
	return err
}
