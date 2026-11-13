# Production Gateway — Design

Status: **approved 2026-11-12** (drafted 2026-11-10; updated 2026-11-12
with the reference deployment, §1.1, and own user database, §7.1/§7.7).
Supersedes the portfolio framing of `FORGE_SERVE_BLUEPRINT.md` for all work
after the MVP; the blueprint needs an amendment-log entry pointing here
before build starts (CLAUDE.md rule: deviations need a record).

Items marked **[builder's call]** were decided by the assistant, not the
owner, and can be overturned without changing the overall design.

---

## 1. Intent

Forge-Serve today is a complete portfolio MVP: a Go gateway (auth, Redis
rate limiting, usage accounting, least-outstanding routing, failover,
metrics) in front of our own continuous-batching CPU worker.

The goal is **real infrastructure**:

1. **First, inside one organization** that runs LLMs with **vLLM** on its
   own GPU servers. Engines like vLLM ship without per-person keys or usage
   tracking. Forge-Serve goes *in front of* those servers, without
   replacing them, so that **each person has their own API key** and
   access and usage can be tracked and controlled per person.
2. **Then as an open-source project** other companies and institutions
   can run on whatever setup they have.

The design targets the reference deployment below (§1.1), but must still
work with different engines, different numbers of models and servers, and
both single-server and Kubernetes installs. Site-specific choices live in
config and deployment docs, never in code.

### 1.1 Reference deployment

| Question | Answer | Consequence for this design |
|---|---|---|
| Engine | vLLM, latest version | `kind: vllm` is the first and main backend (§6.4). Priority scheduling and `/metrics` are available. |
| Models | Chosen per use case; several | Model routing (G2) and per-model access grants are needed from day one. |
| Hardware | **2 GPU servers** on a private network | Failover only works for a model served on both. Fallback (§6.7) covers models on one server. See §6.8. |
| Deployment | Plain machines, no Kubernetes | Docker Compose is the production install. Helm moves to the open-source step. |
| API keys | Assume none on the engines | Locking vLLM down so only the gateway can reach it is the first and biggest security win (§8). |
| Monitoring | Assume none | Prometheus + Grafana provide it; also scrape vLLM and GPU metrics (§9). |
| Login | Organization's own user store | **Not integrated** (design decision): the gateway keeps its own user database (§7.1, §7.7). No passwords in this project. |

Why this is valuable: inference engines (vLLM, SGLang, TGI, Ollama) are
good at running models and weak at everything around them — no per-person
identity, no quotas, no usage records, no cross-server failover, no fail-fast
admission. That is exactly the layer the gateway already is.

## 2. Goals

- **G1 Engine-agnostic.** Works in front of any OpenAI-compatible server
  (vLLM, SGLang, TGI, Ollama, llama.cpp server) plus our own worker.
  An unknown engine still works, with reduced load-awareness.
- **G2 Multi-model.** Routes by the `model` field to a configured group of
  servers per model.
- **G3 One key per person.** Every user has their own key(s); usage and
  access are tracked and decided per person, with optional team grouping.
- **G4 Controls.** Per-person request-rate limits, per-person token budgets
  (daily/monthly), optional team token caps, per-model access.
- **G5 Administrable without SQL.** An admin API to manage users, teams,
  keys, quotas, model access, and to read usage.
- **G6 Fail fast, fail over.** Keep today's guarantees: 429 + `Retry-After`
  at admission, retry-once only when zero tokens reached the client
  (ADR-005), clean error event otherwise.
- **G7 Passes a company security review.** TLS, no default secrets, no
  prompt logging, audit trail, closed internal ports.
- **G8 Visible.** Usage per person, team and model; fleet health and
  latency per model.
- **G9 Drop-in for clients.** Any OpenAI SDK works by changing base URL and
  key. Request fields the gateway does not understand still reach the
  engine untouched.
- **G10 Shared-GPU fairness** (features neither vLLM nor a plain proxy has
  alone, added 2026-11-10):
  - **Priority per person/team**, enforced by the engine's own scheduler
    (§7.5).
  - **Tokens-per-minute limit** per person alongside the request limit
    (§7.2).
  - **Model fallback**: optionally serve a configured smaller model when a
    model's servers are full or down (§6.7).

## 3. Non-goals (this design)

- New inference-engine work, beyond the two known engine bugs (§11 step 1).
- Company login / SSO, and any link to the company's user database. The
  gateway owns its users (§7.7). Standard SSO (OIDC) is for the
  open-source version.
- An admin web UI. API + Grafana first; a UI only if those prove
  insufficient.
- Billing, invoices, payments, money-denominated budgets.
- Prompt/response caching, cloud-API fallback, multi-region.
- Self-service key creation by ordinary users (admins issue keys for now).

## 4. Anti-goals — failure even if it "works"

- **Worse than today's direct access.** If the gateway makes LLM access
  slower or less available than calling vLLM directly, the pilot has
  failed. It must add negligible
  latency and never be the single thing that breaks access.
- **Logging what people ask.** Prompt and response text is never logged or
  stored. Usage = counts only.
- **Breaking OpenAI compatibility.** Any request that worked directly
  against the engine must work through the gateway.
- **Locking to vLLM.** No vLLM-only assumptions outside the vLLM load probe.
- **Per-person data used beyond its purpose.** Per-person usage is personal
  data; who can see it is an explicit admin permission (§7.6).

## 5. Constraints

- Keep the existing stack: Go gateway, Postgres, Redis, Prometheus,
  Grafana, Docker. New dependencies only where the stdlib clearly loses.
- Gateway stays **stateless**, so several copies can run.
- Reference deployment (§1.1): vLLM, 2 GPU servers, plain machines. The
  gateway keeps its own user database.
- Must remain runnable on one laptop with Docker Compose (dev + demos),
  using our own worker or Ollama as the backend.

---

## 6. Architecture

```
clients (OpenAI SDKs, curl, internal tools)
        │  HTTPS, Bearer sk-forge-…
        ▼
┌──────────────── GATEWAY (Go, N stateless copies) ────────────────┐
│ auth (per-person key) → rate limit → budget check → model lookup │
│ → access check → pick backend in model's pool → admission        │
│ → forward body → relay stream → record usage                     │
│                                                                  │
│ /admin/v1/*  (admin keys only)        /metrics  /healthz /readyz │
└──────┬──────────────┬───────────────────────┬────────────────────┘
       │              │                       │
   Postgres         Redis              Backend pools (per model)
   users, keys,     rate buckets,      ├─ kind=vllm    → vLLM servers
   usage, audit     budget counters    ├─ kind=openai  → any compatible
                                       ├─ kind=ollama  → Ollama
                                       └─ kind=forge   → our worker
```

### 6.1 Request flow

1. **Auth.** Hash the bearer key (SHA-256, as today), resolve to
   key → user → team, plus quotas and grants. Cached; see §7.4 for TTL.
   Disabled user or revoked/expired key → 401.
2. **Rate limit.** Existing Redis Lua token bucket, now keyed **per user**
   (all of a person's keys share one bucket) **[builder's call]**, plus
   the tokens-per-minute bucket (§7.2).
3. **Budget check.** If the user's (or team's) token usage in the current
   window already meets the budget → 429, OpenAI-style error type
   `insufficient_quota`. See §7.3.
4. **Model lookup.** Read `model` from the body. Unknown model → 404,
   `model_not_found`. Model marked `restricted` and user has no grant →
   403.
5. **Pick backend.** Least-outstanding-tokens among healthy backends in
   that model's pool (existing `pool.Pick`, now one pool per model).
6. **Admission.** If every healthy backend in the pool is at its limit
   (§6.4), or none is healthy → use the model's fallback if configured and
   allowed (§6.7), else 429 + `Retry-After` (503 when none healthy).
   Never accept-then-queue-forever.
7. **Forward.** Send the client's JSON body to the backend **as received**,
   with only these changes (§6.2).
8. **Relay.** Stream the response back (§6.3). Failure before any content
   reached the client → retry once on another backend in the pool;
   after → terminal SSE error event (ADR-005, unchanged).
9. **Record usage.** Async, batched, as today, now with user, team, model.

### 6.2 Pass-through proxy

Today the gateway rebuilds requests from a small struct, which silently
drops `top_p`, `stop`, `tools`, `response_format`, `seed`, etc. The new
gateway forwards the **original body**, parsing it only to read `model`,
`stream`, `max_tokens`, `stream_options`.

Permitted body edits, and nothing else:
- `model` rewritten to the pool's `upstream_model` if configured
  (lets the company expose friendly names).
- When `stream: true` and the client did not set
  `stream_options.include_usage`, the gateway sets it to `true` so that the
  engine reports token counts. The usage-only chunk is then **stripped**
  before reaching the client, so the client sees exactly what it asked for
  **[builder's call]**.
- `priority` is **always removed** from the client body, then set from the
  user's effective priority when the backend supports it (§7.5). A client
  must never choose its own priority.

Validation at the trust boundary stays: body must be JSON, `model`
present, `messages` (chat) / `prompt` (completions) / `input`
(embeddings) present, size limit on the body (configurable, default
1 MiB **[builder's call]**). Field-level semantics are the engine's job.

### 6.3 Relaying responses

- **Streaming (SSE).** Read the backend's `data:` lines. Lines with no
  content (e.g. vLLM's first role-only delta) are **held** until the first
  content-bearing chunk, then flushed together. "Tokens sent" for the retry
  rule = content chunks already written. This keeps "nothing reached the
  client" true for as long as possible, so a backend that dies before its
  first real token is still retryable **[builder's call]**.
- **Non-streaming and embeddings.** Response is buffered in full, so any
  failure is retryable once (as today).
- **Usage numbers** come from the engine's `usage` object. If an engine
  provides none, fall back to counting content chunks as completion tokens,
  prompt tokens = 0, and store the row with `estimated = true`
  **[builder's call]**.
- Response headers from the backend are not passed through except
  `Content-Type`; the gateway sets its own `X-Request-Id`.

### 6.4 Backends and load probes

```go
// One implementation per protocol, not per engine.
type Backend interface {
    // Forward sends the request and relays events to sink. It returns how
    // many content chunks reached the client (the ADR-005 retry signal).
    Forward(ctx context.Context, r *ProxyRequest, sink Sink) (sent int, err error)
    // Load reports health and, where the engine exposes it, queue state.
    Load(ctx context.Context) (Load, error)
}

type Load struct {
    Waiting, Running int
    Known            bool // false: engine exposes no queue data
}
```

Two `Forward` implementations: **`openai`** (all OpenAI-compatible engines)
and **`forge`** (our worker's NDJSON protocol, translated). `Load` varies by
`kind`:

| `kind` | Forward | Load source |
|---|---|---|
| `vllm` | openai | `/metrics`: `vllm:num_requests_waiting`, `vllm:num_requests_running` |
| `sglang`, `tgi` | openai | their `/metrics`; exact metric names verified by the builder against current versions |
| `ollama` | openai (`/v1`) | health only (`Known=false`) |
| `openai` (generic) | openai | `GET /v1/models` as health only |
| `forge` | forge | `/stats` (existing) |

The `forge` worker only implements chat; a model backed by it answers
`/v1/completions` and `/v1/embeddings` with 400, OpenAI error shape
**[builder's call]**.

Health rules unchanged: poll every 2 s, 3 misses → unhealthy, 1 success →
healthy (ADR-004).

**Admission limits** per backend, from config:
- `max_waiting`: reject when the engine reports this many waiting
  (only when `Known`).
- `max_inflight`: reject when the **gateway's own** count of requests in
  flight to that backend reaches this. Works for every engine and is not
  2 s stale like the poll. Default 64 **[builder's call]**.

With several gateway copies, `max_inflight` is per copy. Documented, not
solved; `max_waiting` from the engine is the fleet-wide signal.

### 6.5 Configuration

One file, `forge.yaml`, path from `FORGE_CONFIG`. Secrets (DB URL, Redis
URL, TLS key) come from environment variables, never the file.
YAML via `gopkg.in/yaml.v3` **[builder's call]**: operators expect YAML and
it is one small dependency.

```yaml
listen: ":8080"
tls: { cert_file: "", key_file: "" }   # empty = plain HTTP behind a proxy
auth_cache_ttl: 30s
max_body_bytes: 1048576

models:
  - name: llama-3.1-70b            # what clients send
    upstream_model: meta-llama/Llama-3.1-70B-Instruct
    restricted: true               # needs a grant
    default_max_tokens: 512        # routing estimate when client omits it
    backends:
      - { url: http://gpu1:8000, kind: vllm, max_waiting: 32, max_inflight: 64 }
      - { url: http://gpu2:8000, kind: vllm, max_waiting: 32, max_inflight: 64 }
  - name: qwen2.5-1.5b
    backends:
      - { url: http://worker1:9000, kind: forge }
```

Additional keys used by G10: `fallback: <model name>` on a model (§6.7);
`supports_priority: true` on a backend (§7.5). Backend credential:
`api_key_env: VLLM_GPU1_KEY` names an environment variable holding the
key the gateway sends to that backend (§6.8).

`WORKER_URLS` keeps working for one release as shorthand for a single
`forge` model, so existing compose setups do not break **[builder's call]**.
Config is read at start; changes need a restart (hot reload deferred).

### 6.6 Endpoints

| Path | Who | Notes |
|---|---|---|
| `POST /v1/chat/completions` | user key | stream + non-stream |
| `POST /v1/completions` | user key | same path, different body field |
| `POST /v1/embeddings` | user key | non-stream only |
| `GET /v1/models` | user key | from config, filtered to models the user may use |
| `GET /v1/usage` | user key | the caller's own usage (exists; extended to per-model) |
| `/admin/v1/*` | admin key | §7.6 |
| `GET /healthz` | open | process alive |
| `GET /readyz` | open | DB reachable **and** ≥1 healthy backend overall |
| `GET /metrics` | open on internal port | §9 |
| `GET /` playground | dev profile only | off by default in production |

### 6.7 Model fallback

- Configured per model: `fallback: qwen2.5-7b`. **Off unless configured.**
- Used only at admission time (step 6): the requested model's pool is full
  or has no healthy backend. Not used after a request has started; the
  ADR-005 retry stays within the requested model's pool.
- **One hop only.** The fallback model's own `fallback` is ignored, so
  there are no chains or loops **[builder's call]**.
- The user must be allowed to use the fallback model (grants, §7.1);
  otherwise the original 429/503 is returned.
- A client opts out per request with header `X-Forge-No-Fallback: 1`, for
  callers that would rather wait than get a weaker model
  **[builder's call]**.
- Transparency: the response `model` field reports the model actually
  served, and the gateway adds `X-Forge-Model-Fallback: <served model>`.
  Usage, budgets and metrics are recorded against the **served** model.
- The body's `model` is rewritten to the fallback's `upstream_model`; all
  other fields pass through. A request using features the fallback model
  lacks fails at the engine like any other unsupported request.

---

### 6.8 Reference deployment (2 GPU servers, plain machines)

```
internal network
  │
  ▼  HTTPS :443 (only open door)
┌─────────── gateway host (Docker Compose) ───────────┐
│ gateway · Postgres · Redis · Prometheus · Grafana   │
└───────┬───────────────────────────┬─────────────────┘
        │ private, firewalled       │
  ┌─────▼──────┐             ┌──────▼─────┐
  │ GPU-1 vLLM │             │ GPU-2 vLLM │   ports reachable ONLY
  │ :8000      │             │ :8000      │   from the gateway host
  └────────────┘             └────────────┘
```

- **Gateway host:** a small separate machine or VM is preferred: losing a
  GPU server must not also take down the gateway, Postgres and the
  dashboards. If no spare machine exists, run the Compose stack on one GPU
  server's CPU side and accept that coupling, documented
  **[builder's call]**. Open question §14.
- **Model placement drives resilience.** A model served on both GPUs gets
  failover (ADR-005 retry within its pool). A model on one GPU has no
  failover; give it a `fallback` model on the other GPU where a weaker
  answer beats an error. The config makes this explicit per model.
- **Defence in depth:** each vLLM is also started with `--api-key`, a
  secret only the gateway knows (config: `api_key_env` per backend, sent
  as `Authorization: Bearer` to the backend). If the firewall is ever
  misconfigured, vLLM still refuses direct callers **[builder's call]**.
- Each vLLM is started with `--scheduling-policy priority` to enable §7.5.

## 7. Identity, quotas and access

### 7.1 Data model

The gateway has **its own small Postgres database** and is the only source
of truth for users, teams and keys. It does **not** connect to the
organization's user database (design decision, 2026-11-12). It stores no
passwords: an API key is the only credential, and only its SHA-256 hash is
kept.

Replaces today's `tenants` / `api_keys` / `usage_events`. Existing rows
migrate: each tenant becomes a team; each existing key becomes a key owned
by a placeholder user of that team **[builder's call]**.

Seven tables. Schema as the first migration (`migrations/001_identity.sql`):

```sql
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

-- Applied migrations (§8).
CREATE TABLE schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Choices in this schema, all **[builder's call]**:
- **Disable, never delete** users and keys: usage history and the audit
  log keep pointing at real rows. Foreign keys to `users` and `api_keys`
  therefore have no `ON DELETE CASCADE`.
- Status and role are `TEXT` + `CHECK` rather than Postgres enums: adding
  a value later is a one-line migration, not an enum rewrite.
- Email is unique case-insensitively (`lower(email)`), so `A@x.com` and
  `a@x.com` cannot become two people.
- `usage_events` records `key_id` (not the key hash) and both served and
  requested model, so fallback use is visible in reports.
- Expected size for the company: tens to hundreds of users, a few thousand
  usage rows a day. No partitioning or rollup needed; revisit if
  `usage_events` passes tens of millions of rows.

- **Keys:** `sk-forge-` + 32 random bytes, base62. Raw key shown **once**
  at creation. `key_prefix` = first 8 characters after `sk-forge-`, shown
  in listings so people can tell keys apart. One person may hold several
  keys (laptop, server, CI).
- `NULL` budget/cap = unlimited. New users get defaults from config
  (`defaults.user`) **[builder's call]**.
- `last_used_at` updated at most once a minute per key, off the request
  path **[builder's call]**.

### 7.2 Request-rate limit

Existing Redis Lua bucket and local fallback (ADR-006), keyed by
`user_id`, plan from the user row.

**Tokens per minute (TPM).** A second bucket per user, capacity and refill
= `tokens_per_minute` (`NULL` = unlimited). A request's real size is only
known when it ends, so:
- **Before forwarding:** allowed only if the bucket is above zero.
  Otherwise 429, `Retry-After` = seconds until it is positive again.
- **After it ends:** debit prompt + completion tokens. The bucket may go
  negative, which then blocks the user until refill catches up. A large
  request is therefore never cut off mid-way, and the long-run rate still
  holds.
- Same Lua-script atomicity and same Redis-down local fallback as the
  request bucket (ADR-006).
- Pre-reserving `max_tokens` was rejected: most requests stop well short
  of it, so reserving would reject users who are under their limit.

### 7.3 Token budgets

- Counters in Redis: `forge:tok:u:<id>:d:<YYYY-MM-DD>`,
  `…:m:<YYYY-MM>`, and the same for teams. `INCRBY` after each completed
  request with prompt + completion tokens; TTL just past the window end.
  Windows are UTC **[builder's call]**.
- All endpoints count, embeddings included (their prompt tokens). User
  budget and team cap both apply; whichever is reached first rejects
  **[builder's call]**.
- Check happens **before** forwarding, against usage so far. A request can
  therefore overshoot a budget by its own size. Accepted and documented:
  pre-reserving `max_tokens` would reject requests that would have fit.
- **Redis down:** budget checks are skipped (allow), the rate limiter falls
  back as today, and `forge_budget_check_skipped_total` increments.
  Postgres `usage_events` stays the source of truth, so nothing is lost,
  only enforcement is paused. Same availability-over-accuracy reasoning as
  ADR-006; needs its own ADR.
- On Redis recovery, counters for the current windows are rebuilt from
  Postgres on first miss **[builder's call]**.

### 7.4 Revocation

- Auth cache TTL drops from 5 min to **30 s** (configurable). A revoke
  through the admin API also evicts the entry on the copy that served it.
  Requirement: a revoked key or disabled user stops working everywhere
  within the TTL.
- Disabling a user revokes all their keys.
- Streams already in progress are not cut **[builder's call]**.

### 7.5 Priority

Goal: when GPUs are busy, interactive people go ahead of bulk scripts.
The gateway knows *who* is asking; the engine's scheduler knows *how* to
reorder its queue. Neither can do this alone.

- **Effective priority** = user's `priority`, else team's, else
  `defaults.priority` from config. An integer; its meaning follows the
  engine (for vLLM, lower runs first, to be verified against the deployed
  version by the builder).
- Sent as the body field `priority` **only** to backends with
  `supports_priority: true`. vLLM must be started with
  `--scheduling-policy priority`; it is off by default and may reject the
  field otherwise, so the flag is explicit per backend, never auto-detected
  **[builder's call]**.
- The client's own `priority` field is always stripped (§6.2).
- Other engines and our `forge` worker ignore priority; it is a no-op, not
  an error. Our engine stays FCFS (ADR-003).
- Admin API sets priority on users and teams; changes are audited.
- Priority reorders the engine queue only. The gateway's own admission and
  routing do not consider it in this design **[builder's call]**.

### 7.6 Admin API

JSON over HTTP under `/admin/v1`, authenticated by keys of users with
`role = admin`. Every write appends to `admin_audit`.

- Users: create, get, list (filter by team/status), update quotas
  (request rate, TPM, budgets), priority, role, team; disable, enable.
- Teams: create, list, update cap, delete (only if empty).
- Keys: create for a user (returns raw key once), list (prefix, label,
  dates — never the hash), revoke.
- Grants: add/remove model access for a user or team.
- Usage: aggregate by user / team / model over a date range; CSV export.
  **Per-person usage is only returned to admins with `can_view_usage`**, so
  the company can separate "manages keys" from "sees what individuals
  used".
- Audit: list entries.

**Bootstrap:** `gateway admin bootstrap --email <e>` (a subcommand of the
same binary) creates the first admin and prints its key once. Production
installs contain **no seeded keys**; the two dev keys move to a
`dev` compose profile only.

### 7.7 Managing users (own database, no company-DB link)

Decision 2026-11-12: the gateway keeps its own users (§7.1) and does
not read the organization's user database. Consequences:

- **Adding people:** admins create users through the admin API, one at a
  time or in bulk with `gateway users import people.csv`
  (`email,display_name,team`), which upserts by email and is audited
  **[builder's call]**. Keys are then issued per person and handed over
  privately.
- **Leavers are manual:** an admin disables the user, which blocks every key
  they hold within the auth-cache TTL (§7.4). There is no automatic sync, so
  offboarding needs a checklist entry; the admin API offers "list users
  with no requests in N days" to catch forgotten accounts
  **[builder's call]**.
- **No passwords anywhere in this project.** People never log in to the
  gateway; an API key is their only credential.
- **Open-source benefit:** no company-specific integration code exists, so
  the public version needs no changes. Standard SSO (OIDC) can be added
  later as an optional way to issue keys.

---

## 8. Hardening

- **Lock down vLLM first.** If anyone on the internal network can call the
  GPU servers directly, per-person keys mean nothing. Firewall vLLM ports on both
  GPU servers so only the gateway host can connect, plus `--api-key` on
  vLLM (§6.8). Until this is done, per-person keys can be bypassed and
  usage numbers are incomplete. This happens in the pilot, step 6, before
  anyone is told to switch.
- **TLS:** built-in when `tls.cert_file`/`key_file` are set; otherwise the
  docs say to run behind the company's reverse proxy. Admin API can be
  restricted to an internal listen address (`admin_listen`)
  **[builder's call]**.
- **Logging:** structured (`slog` JSON). Per request: request id, user id,
  key prefix, model, backend, status, tokens, latency. **Never** request or
  response bodies, never raw keys. A test asserts a prompt string does not
  appear in log output.
- **Compose production profile:** Postgres/Redis not published to the host,
  Grafana login required (anonymous only in `dev`), playground off.
- **Migrations:** numbered SQL files embedded in the binary, applied at
  start inside a transaction, tracked in `schema_migrations`. Replaces
  `postgres-init.sql`, which only runs on an empty volume
  **[builder's call]**: ~40 lines, no migration-tool dependency.
- **Timeouts:** connect/first-byte timeout to backends (default 30 s) for a
  fast failover decision; no total timeout on generation (ADR-002 reasoning).
- **Backups:** documented `pg_dump` procedure; not automated by the project.
- **Multiple gateway copies:** supported; caveats documented (per-copy
  `max_inflight`, per-copy local rate-limit fallback).

## 9. Visibility

- **Prometheus:** existing metrics gain a `model` label (bounded by
  config). **No `user` label** — unbounded label values would overload
  Prometheus. Add backend load gauges per backend, admission rejections,
  budget rejections, budget-check skips.
- **Grafana:** add Postgres as a second, read-only data source. New
  dashboard: tokens over time by team and model, top users, budget use as
  % of budget, error rates. Provisioned from `deploy/grafana/` like today.
- **Usage export:** admin CSV endpoint (§7.6).
- **Engine and GPU monitoring:** Prometheus
  also scrapes each vLLM's own `/metrics` (queue, KV-cache use, throughput)
  and, optionally, NVIDIA's `dcgm-exporter` on each GPU server for GPU
  utilisation, memory and temperature. One extra container per GPU server;
  a Grafana panel row for each **[builder's call]**.

## 10. Testing

- Unit: body pass-through and the two permitted edits; SSE relay incl.
  holding role-only chunks and stripping the injected usage chunk; retry
  rule across per-model pools; admission on `max_waiting` and
  `max_inflight`; budget check and Redis-down skip; revocation within TTL;
  access grants; migrations from the current schema; no-prompt-in-logs;
  TPM allow-when-positive and debit-after (incl. going negative);
  client `priority` always stripped and effective priority sent only to
  `supports_priority` backends; fallback taken only at admission, one hop,
  respects grants and the opt-out header, usage recorded against the
  served model.
- Integration (CI): a fake OpenAI-compatible backend (Go `httptest`) that
  can stream, delay, die before or after its first token, and omit usage.
  No real model in CI (existing policy).
- Existing tests keep passing; the `forge` backend keeps today's behaviour.

### 10.1 Testing without production hardware

Verification is layered. Each layer catches what the previous one cannot,
and production hardware is needed only for the pilot.

| Layer | Where | Setup | Proves | Cannot prove |
|---|---|---|---|---|
| 1. Automated | CI + laptop | Fake vLLM in Go `httptest` (above): streams, delays, dies before/after first token, omits usage, reports a full queue, rejects `priority` when told to | All gateway logic: keys, limits, TPM, budgets, priority handling, retry, fallback, admission | Real engine behaviour |
| 2. Real engines | Laptop, Docker Compose | **Two Ollama containers** standing in for GPU-1/GPU-2; one model on both, one model on only one; `kind: ollama` | Streaming, model routing, unmodified OpenAI SDK, failover and fallback via the chaos script (`docker kill`) | vLLM specifics (queue metrics, priority, `--api-key`, usage in stream) |
| 3. Real vLLM | Laptop **or** rented cloud GPU | Laptop: vLLM CPU build, tiny model (e.g. Qwen2.5-0.5B), slow but sufficient. Cloud: 1–2 h on a rented GPU with the production vLLM version | `/metrics` parsing, `--scheduling-policy priority` + `priority` field, backend `--api-key`, `stream_options.include_usage` numbers | Production network, firewall, real users |
| 4. Production | GPU servers | Gateway host, firewall rules, `--api-key` on both vLLMs, real users (Steps 6–7) | It works in production | — |

- Layer 3 is tried on the laptop first; a cloud GPU is rented only if the
  CPU build is impractical **[builder's call]**. Run it once, near the end
  of Step 3, and again before the pilot.
- Exit criteria in §11 that say "real vLLM" are met by layer 3; layer 4 is
  only for Steps 6–7.
- A `deploy/compose.test.yaml` provides the layer-2 setup (gateway + two
  Ollama + Postgres + Redis + Prometheus + Grafana) so it starts with one
  command **[builder's call]**.

## 11. Build order

| Step | Content | Done when |
|---|---|---|
| 0 | Deployment facts (§1.1). Remaining: privacy policy, model placement, gateway host, review checklist | Remaining answers recorded in §14 |
| 1 | Cleanup: resolve benchmark CSVs vs README; set `N_THREADS` per worker; fix prefill-blocking and slow sampling in the engine; blueprint amendment + ADRs | README matches committed CSVs; ADRs merged |
| 2 | Pass-through proxy, `Backend` interface, `openai`/`forge` forwarders, `vllm` probe, per-model pools, admission, **model fallback**, config file, new endpoints | On layer 2 (two Ollama): streams, routes by model, survives a killed server, falls back when a pool is full, records non-zero usage. On layer 3: `vllm` probe reads real `/metrics` (§10.1) |
| 3 | Identity: schema + migrations, per-user keys, rate limit per user, **TPM limit**, budgets, grants, **priority**, revocation, admin API, bootstrap | Admin creates a person's key via API; TPM and budgets enforced; priority reaches a `--scheduling-policy priority` vLLM; revoke effective ≤ 30 s; tests green |
| 4 | Hardening (§8), incl. backend `api_key_env` | Passes the company's security checklist |
| 5 | Visibility (§9), incl. vLLM + GPU metrics | Admin sees usage per person/team/model and GPU health in Grafana |
| 6 | Pilot: gateway host up; one team through the gateway; then firewall + `--api-key` on vLLM so direct access closes | ~2 weeks, no incidents, team wants to keep it, direct vLLM access closed |
| 7 | Company rollout: bulk-import users (§7.7), issue personal keys, add offboarding step to the company's leaver checklist | Everyone uses personal keys; direct vLLM access closed |
| 8 | Open source: IP sign-off, license, name check, Helm chart, docs per engine, tested against vLLM + SGLang + Ollama, v0.1 | Public release |

Steps 2–3 get a detailed implementation plan next
(`superpowers:writing-plans`).

## 12. ADRs to write before building

- ADR-007 Pass-through proxy instead of request rebuilding.
- ADR-008 Per-person identity; rate limit per user, not per key.
- ADR-009 Token budgets: check-before, count-after; skip when Redis is down.
- ADR-010 Admission in the gateway (`max_waiting` + `max_inflight`).
- ADR-011 Config file + env secrets; `WORKER_URLS` compatibility.
- ADR-012 TPM limit: allow-when-positive, debit-after.
- ADR-013 Priority delegated to the engine scheduler; never client-set.
- ADR-014 Model fallback: admission-time only, one hop, opt-out.
- ADR-015 Gateway owns identity in its own database; no company-DB link,
  no passwords.
- Amendment-log entry in `FORGE_SERVE_BLUEPRINT.md` pointing here.

## 13. Left to the builder

Package layout inside `gateway/internal`, admin API JSON field names and
pagination, exact SGLang/TGI metric names, base62 implementation, error
message wording (OpenAI error shape is required), Grafana panel layout,
SSE line-parsing details, default quota values in the sample config.

## 14. Open questions

Recorded here so they are not lost; none blocks Step 1.

1. **Privacy.** Does the deploying organization's policy allow per-person
   usage tracking, and who may see it? (Shapes `can_view_usage` defaults.)
2. **Remaining deployment facts:** which models run on which GPU server;
   whether a small separate machine/VM is available for the gateway;
   the security review checklist.
3. **Benchmark resolution** for Step 1: rerun with `N_THREADS` set, or
   restore the committed single-worker CSVs.
