package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
)

// KeyRecord is everything the request path needs about a key, in one
// query: who it belongs to, their limits, and which restricted models
// they may use. Zero budgets/TPM mean unlimited; TeamID 0 means no team.
type KeyRecord struct {
	KeyID, UserID, TeamID int64
	KeyPrefix, Role       string
	CanViewUsage          bool
	Active, Revoked       bool
	ExpiresAt             *time.Time
	RPMBurst              int
	RPMRefillPerSec       float64
	TokensPerMinute       int
	DailyBudget           int64
	MonthlyBudget         int64
	TeamMonthlyCap        int64
	Priority              *int // user's, else team's; nil = config default
	Grants                []string
}

const lookupKeySQL = `
SELECT k.id, u.id, COALESCE(u.team_id, 0), k.key_prefix, u.role, u.can_view_usage,
       u.status = 'active', k.revoked_at IS NOT NULL, k.expires_at,
       u.rpm_burst, u.rpm_refill_per_sec, COALESCE(u.tokens_per_minute, 0),
       COALESCE(u.daily_token_budget, 0), COALESCE(u.monthly_token_budget, 0),
       COALESCE(t.monthly_token_cap, 0), COALESCE(u.priority, t.priority),
       COALESCE((SELECT string_agg(DISTINCT g.model, ',' ORDER BY g.model) FROM model_grants g
                 WHERE g.user_id = u.id OR (u.team_id IS NOT NULL AND g.team_id = u.team_id)), '')
FROM api_keys k
JOIN users u ON u.id = k.user_id
LEFT JOIN teams t ON t.id = u.team_id
WHERE k.key_hash = $1`

func (s *Store) LookupKey(ctx context.Context, hashHex string) (KeyRecord, error) {
	var k KeyRecord
	var expires sql.NullTime
	var prio sql.NullInt64
	var grants string
	err := s.db.QueryRowContext(ctx, lookupKeySQL, hashHex).Scan(
		&k.KeyID, &k.UserID, &k.TeamID, &k.KeyPrefix, &k.Role, &k.CanViewUsage,
		&k.Active, &k.Revoked, &expires,
		&k.RPMBurst, &k.RPMRefillPerSec, &k.TokensPerMinute,
		&k.DailyBudget, &k.MonthlyBudget, &k.TeamMonthlyCap, &prio, &grants)
	if errors.Is(err, sql.ErrNoRows) {
		return KeyRecord{}, auth.ErrUnknownKey
	}
	if err != nil {
		return KeyRecord{}, err
	}
	if expires.Valid {
		t := expires.Time
		k.ExpiresAt = &t
	}
	if prio.Valid {
		p := int(prio.Int64)
		k.Priority = &p
	}
	if grants != "" {
		k.Grants = strings.Split(grants, ",")
	}
	return k, nil
}
