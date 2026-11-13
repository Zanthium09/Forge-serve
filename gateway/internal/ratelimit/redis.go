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

// Client shares the connection pool with other Redis users (budgets).
func (b *RedisBackend) Client() *redis.Client { return b.rdb }

// tpmCheckScript / tpmDebitScript are the tokens-per-minute bucket
// (ADR-012) with the same atomicity argument as bucketScript. Unlike the
// request bucket, tokens may go negative: the debit happens after the
// request, when its real size is known.
const tpmRefill = `
local key    = KEYS[1]
local cap    = tonumber(ARGV[1])
local now_ms = tonumber(ARGV[2])
local rate   = cap / 60.0
local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = cap
  ts = now_ms
end
tokens = math.min(cap, tokens + math.max(0, now_ms - ts) / 1000.0 * rate)
`

const tpmCheckScript = tpmRefill + `
redis.call('HMSET', key, 'tokens', tokens, 'ts', now_ms)
redis.call('EXPIRE', key, math.ceil((cap - tokens) / rate) + 60)
if tokens > 0 then
  return {1, 0}
end
return {0, math.floor(-tokens / rate) + 1}
`

const tpmDebitScript = tpmRefill + `
tokens = tokens - tonumber(ARGV[3])
redis.call('HMSET', key, 'tokens', tokens, 'ts', now_ms)
redis.call('EXPIRE', key, math.ceil((cap - tokens) / rate) + 60)
return 1
`

var (
	tpmCheck = redis.NewScript(tpmCheckScript)
	tpmDebit = redis.NewScript(tpmDebitScript)
)

func (b *RedisBackend) TPMCheck(ctx context.Context, key string, perMinute int, now time.Time) (bool, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	res, err := tpmCheck.Run(ctx, b.rdb, []string{"forge:tpm:" + key}, perMinute, now.UnixMilli()).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, int(res[1]), nil
}

func (b *RedisBackend) TPMDebit(ctx context.Context, key string, tokens, perMinute int, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	return tpmDebit.Run(ctx, b.rdb, []string{"forge:tpm:" + key}, perMinute, now.UnixMilli(), tokens).Err()
}
