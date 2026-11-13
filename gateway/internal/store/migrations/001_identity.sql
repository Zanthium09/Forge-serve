-- 001: identity schema (spec §7.1, ADR-015). The gateway owns its users.
-- Drops the MVP's tables: they held dev data only (plan decision); dev keys
-- are recreated by FORGE_DEV_SEED=1. schema_migrations is created by the
-- runner itself before any migration runs.
DROP TABLE IF EXISTS usage_events, api_keys, tenants;

-- Teams: optional grouping for caps, priority and grants.
CREATE TABLE teams (
    id                 BIGSERIAL PRIMARY KEY,
    name               TEXT NOT NULL UNIQUE,
    monthly_token_cap  BIGINT CHECK (monthly_token_cap > 0),   -- NULL = no cap
    priority           INTEGER,                                -- NULL = default (§7.5)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Users: one row per person. Never hard-deleted (usage history points
-- here); leavers are set to 'disabled', which blocks all their keys.
CREATE TABLE users (
    id                    BIGSERIAL PRIMARY KEY,
    email                 TEXT NOT NULL,
    display_name          TEXT NOT NULL DEFAULT '',
    team_id               BIGINT REFERENCES teams(id) ON DELETE SET NULL,
    role                  TEXT NOT NULL DEFAULT 'member'
                          CHECK (role IN ('member', 'admin')),
    can_view_usage        BOOLEAN NOT NULL DEFAULT false,      -- per-person usage visibility
    status                TEXT NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active', 'disabled')),
    rpm_burst             INTEGER NOT NULL DEFAULT 60 CHECK (rpm_burst > 0),
    rpm_refill_per_sec    DOUBLE PRECISION NOT NULL DEFAULT 10
                          CHECK (rpm_refill_per_sec > 0),
    tokens_per_minute     INTEGER CHECK (tokens_per_minute > 0),       -- NULL = unlimited (§7.2)
    daily_token_budget    BIGINT  CHECK (daily_token_budget > 0),      -- NULL = unlimited (§7.3)
    monthly_token_budget  BIGINT  CHECK (monthly_token_budget > 0),
    priority              INTEGER,                                     -- NULL = team's, then default
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX users_email_ci ON users (lower(email));  -- one person, one row

-- API keys: a person may hold several (laptop, server, CI).
CREATE TABLE api_keys (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL REFERENCES users(id),
    key_hash      TEXT NOT NULL UNIQUE,     -- SHA-256 hex; the raw key is never stored
    key_prefix    TEXT NOT NULL,            -- e.g. 'ab12cd34', shown in listings
    label         TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ,              -- NULL = never
    revoked_at    TIMESTAMPTZ,              -- NULL = active
    last_used_at  TIMESTAMPTZ
);
CREATE INDEX api_keys_user ON api_keys (user_id);

-- Access to restricted models, per user or per team (exactly one).
CREATE TABLE model_grants (
    id       BIGSERIAL PRIMARY KEY,
    model    TEXT NOT NULL,                 -- client-facing model name from config
    user_id  BIGINT REFERENCES users(id),
    team_id  BIGINT REFERENCES teams(id) ON DELETE CASCADE,
    CHECK ((user_id IS NULL) <> (team_id IS NULL))
);
CREATE UNIQUE INDEX model_grants_user ON model_grants (model, user_id) WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX model_grants_team ON model_grants (model, team_id) WHERE team_id IS NOT NULL;

-- Usage: one row per completed request. Counts only, never text.
CREATE TABLE usage_events (
    id                 BIGSERIAL PRIMARY KEY,
    user_id            BIGINT NOT NULL REFERENCES users(id),
    team_id            BIGINT REFERENCES teams(id) ON DELETE SET NULL,  -- team at request time
    key_id             BIGINT NOT NULL REFERENCES api_keys(id),
    model              TEXT NOT NULL,       -- model actually served
    requested_model    TEXT,                -- set only when fallback served another (§6.7)
    endpoint           TEXT NOT NULL CHECK (endpoint IN ('chat', 'completions', 'embeddings')),
    prompt_tokens      INTEGER NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens  INTEGER NOT NULL CHECK (completion_tokens >= 0),
    estimated          BOOLEAN NOT NULL DEFAULT false,  -- engine gave no usage (§6.3)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX usage_user_time  ON usage_events (user_id, created_at DESC);
CREATE INDEX usage_team_time  ON usage_events (team_id, created_at DESC);
CREATE INDEX usage_model_time ON usage_events (model, created_at DESC);

-- Who changed what. `detail` must never contain a raw key.
CREATE TABLE admin_audit (
    id             BIGSERIAL PRIMARY KEY,
    actor_user_id  BIGINT REFERENCES users(id),   -- NULL = CLI bootstrap/import
    action         TEXT NOT NULL,                 -- e.g. 'key.create', 'user.disable'
    target         TEXT NOT NULL,                 -- e.g. 'user:42', 'key:17'
    detail         JSONB NOT NULL DEFAULT '{}',
    at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX admin_audit_at ON admin_audit (at DESC);
