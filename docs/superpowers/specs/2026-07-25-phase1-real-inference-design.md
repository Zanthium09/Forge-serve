# Phase 1 — Real Inference, Single Worker: Design

Approved by owner 2026-07-25. Scope per FORGE_SERVE_BLUEPRINT.md §8 Phase 1.

## Big picture

A new `worker` container (Python + llama.cpp) generates real tokens from the
downloaded Qwen2.5-1.5B GGUF. The gateway becomes a true middleman: validate
OpenAI-format request → authenticate API key (Postgres) → forward to worker →
relay token stream back as SSE (or aggregate for non-stream). Redis stays
unused until Phase 4.

```
client → gateway (auth, schema, SSE, metrics) → worker (llama.cpp, sequential)
              ↕ Postgres (api_keys, tenants)
```

## Worker (`worker/`)

- Python 3.11, FastAPI + uvicorn, llama-cpp-python **high-level API**
  (`create_chat_completion(stream=True)`) — applies the model's chat template.
  Deliberately sequential: this is the baseline the Phase 2 batching engine
  gets benchmarked against.
- One generation at a time, guarded by a lock; concurrent requests queue.
- Endpoints: `POST /generate` (NDJSON token stream), `GET /healthz`.
- Docker-only (Linux wheels exist; native Windows would need a C++ toolchain).
- Not exposed to the host — reachable only as `worker:9000` on the compose
  network. The gateway is the sole public door.
- `prompt_tokens` is approximated by tokenizing concatenated message content
  (high-level API hides the templated prompt); exact counts come in Phase 2
  when we own the decode loop.

## Worker ↔ gateway protocol (ADR-002)

NDJSON over HTTP/1.1 chunked: one JSON object per line.

```
{"token": "Hello", "done": false}
{"done": true, "prompt_tokens": 12, "completion_tokens": 2, "finish_reason": "stop"}
```

Why not SSE internally: SSE is a browser standard (reconnection semantics we
don't need service-to-service). NDJSON is one `json.dumps` per line in Python,
one `json.Unmarshal` per line in Go. Gateway translates NDJSON → SSE at the
edge (ADR-001: SSE for the client edge).

## Gateway changes

- Full OpenAI request schema: `model`, `messages`, `temperature`,
  `max_tokens`, `stream`. Validation at the trust boundary (400 on bad input).
- `stream: true` → SSE relay of real tokens; `stream: false` → aggregate
  internally, return one `chat.completion` JSON with `usage`.
- Cancellation propagates two hops: client disconnect → ctx cancel → worker
  request aborted → generation stops.
- Auth: `Authorization: Bearer sk-...`; SHA-256 hash looked up in Postgres
  (`api_keys.key_hash`, hex). Hashes not raw keys (DB leak yields nothing).
  In-memory cache (5 min TTL) avoids per-request DB hits. Dev tenant + key
  seeded by `deploy/postgres-init.sql` on first Postgres boot.
  `/healthz` and `/metrics` are unauthenticated.
- First Go dependencies: `jackc/pgx/v5` (via database/sql) and
  `prometheus/client_golang`. Everything else stays stdlib.

## Metrics

`GET /metrics` (Prometheus format): `forge_requests_total{code}`,
`forge_ttft_seconds` histogram, `forge_tokens_per_second` gauge. Grafana
waits until Phase 4.

## Testing

- Gateway unit tests run against a **fake worker** (httptest NDJSON server)
  and a **fake key store** (in-memory map). CI never loads the model.
- Real model exercised by exit checks on owner hardware:
  1. Real tokens stream end-to-end via compose.
  2. Official OpenAI Python SDK works unmodified (stream + non-stream).
  3. 10 concurrent requests all complete (sequential worker → they queue;
     slow is fine, stuck is not).

## ADRs to write

- 001: SSE for the client edge (vs WebSockets/gRPC).
- 002: NDJSON over chunked HTTP for worker protocol (vs SSE/gRPC).
