// Package budget enforces daily/monthly token budgets per user and monthly
// caps per team (ADR-009): checked before a request against usage so far,
// counted after it ends. Counters live in Redis and are rebuilt from
// Postgres when missing; if Redis is down, checks are skipped (allowed) and
// counted, because Postgres still records every request.
package budget

import (
	"context"
	"strconv"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
)

type Counter interface {
	Get(ctx context.Context, key string) (val int64, exists bool, err error)
	SetNX(ctx context.Context, key string, val int64, ttl time.Duration) error
	AddIfExists(ctx context.Context, key string, n int64) error
}

// Source is the durable record (Postgres usage_events).
type Source interface {
	TokensUsed(ctx context.Context, scope string, id int64, since time.Time) (int64, error)
}

// Limits for one request. Zero means unlimited; TeamID 0 means no team.
type Limits struct {
	UserID, TeamID              int64
	Daily, Monthly, TeamMonthly int64
}

type Budgets struct {
	c      Counter
	src    Source
	onSkip func()
}

func New(c Counter, src Source) *Budgets {
	return &Budgets{c: c, src: src, onSkip: metrics.IncBudgetSkipped}
}

type window struct {
	key        string
	scope      string
	id         int64
	limit      int64
	start, end time.Time
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// windows lists every budget window that applies to l (UTC).
func windows(l Limits, now time.Time) []window {
	now = now.UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	dk, mk := day.Format("2006-01-02"), month.Format("2006-01")
	var ws []window
	if l.Daily > 0 {
		ws = append(ws, window{"forge:tok:u:" + itoa(l.UserID) + ":d:" + dk, "user", l.UserID, l.Daily, day, day.AddDate(0, 0, 1)})
	}
	if l.Monthly > 0 {
		ws = append(ws, window{"forge:tok:u:" + itoa(l.UserID) + ":m:" + mk, "user", l.UserID, l.Monthly, month, month.AddDate(0, 1, 0)})
	}
	if l.TeamID != 0 && l.TeamMonthly > 0 {
		ws = append(ws, window{"forge:tok:t:" + itoa(l.TeamID) + ":m:" + mk, "team", l.TeamID, l.TeamMonthly, month, month.AddDate(0, 1, 0)})
	}
	return ws
}

// Check reports whether the request may start. A request can overshoot a
// budget by its own size; pre-reserving max_tokens would reject requests
// that fit (ADR-009).
func (b *Budgets) Check(ctx context.Context, l Limits, now time.Time) bool {
	for _, w := range windows(l, now) {
		used, ok, err := b.c.Get(ctx, w.key)
		if err != nil {
			b.onSkip()
			return true
		}
		if !ok {
			// Counter missing (first use, expiry, or Redis restart): rebuild
			// from the durable record. SetNX so a concurrent rebuild or add
			// is never overwritten.
			used, err = b.src.TokensUsed(ctx, w.scope, w.id, w.start)
			if err == nil {
				err = b.c.SetNX(ctx, w.key, used, w.end.Sub(now)+time.Hour)
			}
			if err != nil {
				b.onSkip()
				return true
			}
		}
		if used >= w.limit {
			return false
		}
	}
	return true
}

// Add counts a finished request. Only existing counters are incremented: a
// missing one is rebuilt from Postgres on the next Check, which already
// includes this request's usage row.
func (b *Budgets) Add(ctx context.Context, l Limits, tokens int64, now time.Time) {
	if tokens <= 0 {
		return
	}
	for _, w := range windows(l, now) {
		if err := b.c.AddIfExists(ctx, w.key, tokens); err != nil {
			b.onSkip()
			return
		}
	}
}
