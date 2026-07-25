package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// bucketScript is the whole limiter, executed atomically inside Redis.
//
// Why Lua and not GET/SET: the refill computation is a read-modify-write.
// With two gateway replicas, both could GET the same count, both decide
// there is a token left, and both SET the decremented value - spending one
// token twice. Redis runs a script to completion without interleaving, so
// the check and the spend cannot be split.
//
// State is two fields (tokens, last-refill-ms) in a hash, expired
// automatically once a bucket has been idle long enough to be full again.
const bucketScript = `
local key      = KEYS[1]
local burst    = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now_ms   = tonumber(ARGV[3])

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
  tokens = burst
  ts = now_ms
end

local elapsed = math.max(0, now_ms - ts) / 1000.0
tokens = math.min(burst, tokens + elapsed * refill)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now_ms)
-- TTL covers a full refill from empty, so idle keys evict themselves.
local ttl = math.ceil(burst / math.max(refill, 0.001)) + 60
redis.call('EXPIRE', key, ttl)
return allowed
`

type RedisBackend struct {
	rdb    *redis.Client
	script *redis.Script
}

func NewRedisBackend(url string) (*RedisBackend, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return &RedisBackend{
		rdb:    redis.NewClient(opt),
		script: redis.NewScript(bucketScript),
	}, nil
}

func (b *RedisBackend) Allow(ctx context.Context, key string, p Plan, now time.Time) (bool, error) {
	// Short timeout: the limiter is on the request path, so a slow Redis
	// must degrade to the local bucket quickly rather than add latency.
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	res, err := b.script.Run(ctx, b.rdb, []string{"forge:rl:" + key},
		p.Burst, p.RefillPerSec, now.UnixMilli()).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (b *RedisBackend) Close() error { return b.rdb.Close() }
