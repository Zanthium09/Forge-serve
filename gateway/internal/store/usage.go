package store

import (
	"context"
	"fmt"
	"time"
)

// TokensUsed sums prompt + completion tokens for a user or team since a
// time. Budgets rebuild their Redis counters from this (ADR-009).
func (s *Store) TokensUsed(ctx context.Context, scope string, id int64, since time.Time) (int64, error) {
	col := map[string]string{"user": "user_id", "team": "team_id"}[scope]
	if col == "" {
		return 0, fmt.Errorf("store: unknown usage scope %q", scope)
	}
	var n int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(prompt_tokens + completion_tokens), 0) FROM usage_events
		 WHERE `+col+` = $1 AND created_at >= $2`, id, since).Scan(&n)
	return n, err
}

type ModelUsage struct {
	Model            string `json:"model"`
	Requests         int64  `json:"requests"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
}

// UsageReport is the /v1/usage body: the caller's totals plus a per-model
// breakdown.
type UsageReport struct {
	Requests         int64        `json:"requests"`
	PromptTokens     int64        `json:"prompt_tokens"`
	CompletionTokens int64        `json:"completion_tokens"`
	TotalTokens      int64        `json:"total_tokens"`
	ByModel          []ModelUsage `json:"by_model"`
}

func (s *Store) UsageForUser(ctx context.Context, userID int64, since time.Time) (UsageReport, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT model, COUNT(*), SUM(prompt_tokens), SUM(completion_tokens)
		 FROM usage_events WHERE user_id = $1 AND created_at >= $2
		 GROUP BY model ORDER BY model`, userID, since)
	if err != nil {
		return UsageReport{}, err
	}
	defer func() { _ = rows.Close() }()
	rep := UsageReport{ByModel: []ModelUsage{}}
	for rows.Next() {
		var m ModelUsage
		if err := rows.Scan(&m.Model, &m.Requests, &m.PromptTokens, &m.CompletionTokens); err != nil {
			return UsageReport{}, err
		}
		rep.ByModel = append(rep.ByModel, m)
		rep.Requests += m.Requests
		rep.PromptTokens += m.PromptTokens
		rep.CompletionTokens += m.CompletionTokens
	}
	rep.TotalTokens = rep.PromptTokens + rep.CompletionTokens
	return rep, rows.Err()
}
