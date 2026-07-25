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
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO tenants (name) VALUES ('dev');

-- Dev-only key for local use: sk-forge-dev-key
INSERT INTO api_keys (tenant_id, key_hash)
SELECT id, encode(digest('sk-forge-dev-key', 'sha256'), 'hex')
FROM tenants WHERE name = 'dev';
