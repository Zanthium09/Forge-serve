package store

import (
	"context"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
)

// LookupKeyForAuth adapts Store to auth.Store. A thin copy keeps store's
// KeyRecord independent of the auth package.
func (s *Store) LookupKeyForAuth(ctx context.Context, hashHex string) (auth.KeyRecord, error) {
	k, err := s.LookupKey(ctx, hashHex)
	return auth.KeyRecord{
		KeyID: k.KeyID, UserID: k.UserID, TeamID: k.TeamID,
		KeyPrefix: k.KeyPrefix, Role: k.Role, CanViewUsage: k.CanViewUsage,
		Active: k.Active, Revoked: k.Revoked, ExpiresAt: k.ExpiresAt,
		RPMBurst: k.RPMBurst, RPMRefillPerSec: k.RPMRefillPerSec, TokensPerMinute: k.TokensPerMinute,
		DailyBudget: k.DailyBudget, MonthlyBudget: k.MonthlyBudget, TeamMonthlyCap: k.TeamMonthlyCap,
		Priority: k.Priority, Grants: k.Grants,
	}, err
}
