package budget

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// addIfExists increments only an existing counter, so an Add can never
// create a partial counter that would undercount until it expires.
var addIfExists = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.call('INCRBY', KEYS[1], ARGV[1])
end
return 0`)

// NoRedis is the counter used when Redis is not configured: every call
// fails, so every budget check is skipped and counted (ADR-009).
var NoRedis Counter = noRedis{}

type noRedis struct{}

var errNoRedis = errors.New("budget: redis not configured")

func (noRedis) Get(context.Context, string) (int64, bool, error) { return 0, false, errNoRedis }
func (noRedis) SetNX(context.Context, string, int64, time.Duration) error {
	return errNoRedis
}
func (noRedis) AddIfExists(context.Context, string, int64) error { return errNoRedis }

type redisCounter struct{ rdb *redis.Client }

func NewRedisCounter(rdb *redis.Client) Counter { return redisCounter{rdb} }

// Short timeouts: budgets are on the request path, so a slow Redis must
// degrade to "skip" quickly rather than add latency.
func short(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 250*time.Millisecond)
}

func (r redisCounter) Get(ctx context.Context, key string) (int64, bool, error) {
	ctx, cancel := short(ctx)
	defer cancel()
	v, err := r.rdb.Get(ctx, key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	return v, err == nil, err
}

func (r redisCounter) SetNX(ctx context.Context, key string, val int64, ttl time.Duration) error {
	ctx, cancel := short(ctx)
	defer cancel()
	return r.rdb.SetNX(ctx, key, val, ttl).Err()
}

func (r redisCounter) AddIfExists(ctx context.Context, key string, n int64) error {
	ctx, cancel := short(ctx)
	defer cancel()
	return addIfExists.Run(ctx, r.rdb, []string{key}, n).Err()
}
