package store

import (
	"context"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
)

// LookupKeyForAuth adapts Store to auth.PlanStore. Kept as a thin method
// so store's own KeyRecord type stays independent of the auth package.
func (s *Store) LookupKeyForAuth(ctx context.Context, hashHex string) (auth.KeyRecord, error) {
	k, err := s.LookupKey(ctx, hashHex)
	return auth.KeyRecord{TenantID: k.TenantID, Burst: k.Burst, RefillPerSec: k.RefillPerSec}, err
}
