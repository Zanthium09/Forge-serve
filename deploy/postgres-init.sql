-- Runs once, on first boot of a fresh pgdata volume
-- (docker-entrypoint-initdb.d). To re-run: docker compose down -v.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE api_keys (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  BIGINT NOT NULL REFERENCES tenants(id),
    -- SHA-256 hex of the raw key. Raw keys are never stored: a leaked DB
    -- yields nothing usable.
    key_hash   TEXT NOT NULL UNIQUE,
    -- Quota, denormalised onto the key so authentication and rate-limit
    -- lookup are one query on the request path.
    burst      INTEGER NOT NULL DEFAULT 60,
    refill_per_sec DOUBLE PRECISION NOT NULL DEFAULT 10,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Usage is append-only and written asynchronously off the request path.
CREATE TABLE usage_events (
    id                BIGSERIAL PRIMARY KEY,
    tenant_id         BIGINT NOT NULL REFERENCES tenants(id),
    key_hash          TEXT NOT NULL,
    model             TEXT NOT NULL DEFAULT '',
    prompt_tokens     INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The /v1/usage endpoint aggregates by key over a time range.
CREATE INDEX usage_events_key_time ON usage_events (key_hash, created_at DESC);

INSERT INTO tenants (name) VALUES ('dev'), ('demo');

-- Dev key: generous quota for local use.  sk-forge-dev-key
INSERT INTO api_keys (tenant_id, key_hash, burst, refill_per_sec)
SELECT id, encode(digest('sk-forge-dev-key', 'sha256'), 'hex'), 60, 10
FROM tenants WHERE name = 'dev';

-- Demo key: deliberately tiny quota so rate limiting is demonstrable.
-- 5 burst, 1/s sustained.  sk-forge-demo-key
INSERT INTO api_keys (tenant_id, key_hash, burst, refill_per_sec)
SELECT id, encode(digest('sk-forge-demo-key', 'sha256'), 'hex'), 5, 1
FROM tenants WHERE name = 'demo';
