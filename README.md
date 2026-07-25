# Forge-Serve

A self-hosted, distributed **LLM inference platform** for open-weight
models: an OpenAI-compatible streaming API in front of model workers you
run yourself, with a continuous-batching engine, multi-worker routing and
failover, distributed rate limiting, usage accounting, and full metrics.

**The model is the commodity; every hard part is ours.** No external LLM
APIs — a quantized Qwen2.5-1.5B runs on CPU, and the serving system around
it is the project. See
[FORGE_SERVE_BLUEPRINT.md](FORGE_SERVE_BLUEPRINT.md) for the full spec and
[docs/adr/](docs/adr/) for the decision record.

**Status: MVP complete** (Phases 0–4).

## Quickstart

```bash
make model            # download Qwen2.5-1.5B GGUF (~1.1 GB, once)
docker compose up --build
```

That brings up the gateway, two inference workers, Postgres, Redis,
Prometheus and Grafana. Then:

| What | Where |
|---|---|
| Playground (streaming chat UI) | http://localhost:8080 |
| Grafana dashboard | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Metrics endpoint | http://localhost:8080/metrics |

Any OpenAI client works by changing one line — the base URL:

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8080/v1", api_key="sk-forge-dev-key")
s = client.chat.completions.create(model="qwen2.5-1.5b", stream=True,
        messages=[{"role": "user", "content": "Count from 1 to 5."}])
for chunk in s:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
```

Seeded keys: `sk-forge-dev-key` (60 burst, 10/s) and `sk-forge-demo-key`
(5 burst, 1/s — small on purpose, so rate limiting is easy to demonstrate).

## Architecture

```
                    clients (OpenAI SDKs, curl, playground)
                                    │  HTTPS + SSE
                    ┌───────────────▼────────────────┐
                    │  GATEWAY (Go)                  │
                    │  · OpenAI-compatible schema    │
   Postgres ◀───────│  · API-key auth (SHA-256)      │
   tenants,         │  · Redis token-bucket limiting │──────▶ Redis
   keys, usage      │  · least-outstanding routing   │        buckets
                    │  · failover + admission 429    │
                    │  · Prometheus /metrics         │
                    └───────┬────────────────┬───────┘
                            │   HTTP/NDJSON  │
                    ┌───────▼──────┐  ┌──────▼───────┐
                    │  WORKER 1    │  │  WORKER 2    │
                    │  continuous  │  │  continuous  │
                    │  batching    │  │  batching    │
                    │  llama.cpp   │  │  llama.cpp   │
                    └──────────────┘  └──────────────┘

   Observability: Prometheus scrapes the gateway; Grafana dashboard is
   provisioned from deploy/grafana/ (no manual setup).
```

## Results

Intel Core Ultra 9 185H (16C/22T), 32 GB, Docker Desktop/WSL2,
Qwen2.5-1.5B Q4_K_M. Every number below traces to a committed artifact in
[`benchmarks/`](benchmarks/) — see [the full report](benchmarks/report.md)
for methodology and caveats.

### Continuous batching vs sequential serving

| Concurrency | Sequential | Batched | Multiplier |
|---|---|---|---|
| 1 | 29.56 tok/s | 21.02 tok/s | 0.71× |
| 8 | 3.03 tok/s | 33.33 tok/s | 11.00× |
| 16 | 1.80 tok/s | 40.54 tok/s | **22.52×** |

The 22× is honest but composite, and the decomposition matters more than
the number: **batching's own ceiling on this CPU is 2.5×** (measured in
isolation — one `llama_decode` across 16 sequences costs 250 ms versus
39 ms for one), while the rest comes from sequential serving *degrading*
under concurrency rather than merely serializing. At concurrency 1
batching is a net loss, which is the expected trade-off.

### Latency, failover, rate limiting

| Metric | Result | Target |
|---|---|---|
| p99 TTFT @ concurrency 8 / 10 / 16 | 4.24 s / 4.87 s / 4.59 s | < 5 s ✅ |
| p99 TTFT @ concurrency 50 | 27.5 s | ❌ capacity-bound, see below |
| Failover recovery (`docker kill` under load) | **1.44 s**, 163/163 requests OK | < 5 s, zero loss ✅ |
| Rate-limit accuracy @ 10× burst | **100%** (11 admitted vs 11.53 budget) | > 99% ✅ |
| Load shedding @ concurrency 50 | 1 693 × `429 + Retry-After`, 0 errors | fail fast ✅ |

The TTFT SLO holds up to concurrency 16 — which equals the batch size, so
every request is admitted immediately. Beyond that, p99 is bounded by
*service time*, not queue depth: shrinking the queue from 8 to 2 barely
moved it (27.5 s → 17.3 s). That is a capacity limit of one CPU worker,
answered by adding workers, not by tuning.

## What's interesting in here

- **[`worker/engine.py`](worker/engine.py)** — the continuous-batching
  scheduler: admit (FCFS) → one shared decode step across all sequences →
  immediate eviction, written as an explicit state machine with named
  invariants. Its logic sits behind a backend protocol, so the whole
  scheduler is unit-tested against a fake model **in CI, with no GGUF**.
- **[`worker/backend_llama.py`](worker/backend_llama.py)** — llama.cpp's
  low-level API: one `llama_batch` per iteration, per-sequence KV ranges
  freed on eviction, manual sampling. The high-level API cannot interleave
  sequences, which is why this exists.
- **Cancellation, end to end** — a client disconnect cancels the gateway's
  context, aborts the worker request, and evicts the sequence from the live
  batch at the next iteration boundary (verified: `active_seqs` 1 → 0 in
  under 2 s). The stream generator is `async` on purpose: asyncio
  cancellation cannot interrupt a thread blocked on `queue.get()`.
- **[`gateway/internal/pool`](gateway/internal/pool)** — least-outstanding-
  tokens routing over pull-based health checks, with failover that retries
  **only** when zero tokens have reached the client (ADR-005).
- **[`gateway/internal/ratelimit`](gateway/internal/ratelimit)** — the token
  bucket runs as a Lua script inside Redis so the read-modify-write is
  atomic across replicas, and degrades to per-replica local buckets when
  Redis is down (ADR-006).
- **[`loadgen/`](loadgen/)** — open- and closed-loop generator writing
  per-request TTFT/TPOT rows, so no published number is hand-typed.

## Hardest problems, and how they were settled

Each of these is written up as an ADR in [`docs/adr/`](docs/adr/):

1. **Streaming protocol** — SSE at the client edge (OpenAI compatibility,
   one-directional traffic, plain HTTP) but NDJSON between gateway and
   worker, where SSE's framing is dead weight. (ADR-001, ADR-002)
2. **Scheduling policy** — FCFS with a bounded queue. SJF needs output-length
   prediction we don't have; an unbounded queue converts overload into
   latency. Queue depth is a *latency budget*, not a buffer. (ADR-003)
3. **Worker liveness** — pull-based health polling instead of push
   heartbeats, so liveness is a by-product of the `/stats` call routing
   already needs, and the gateway exposes no registration endpoint. (ADR-004)
4. **Failover semantics** — never migrate a stream mid-response. Sampling is
   stochastic, so a second worker produces *different* text; a half-streamed
   response cannot be resumed, only corrupted. Zero tokens sent is the safe
   retry boundary. (ADR-005)
5. **Rate limiting under failure** — degrade to local buckets rather than
   fail closed (a Redis blip becomes a total outage) or fail open (an
   attacker gets unlimited quota). (ADR-006)

## Development

```bash
make test     # Go unit tests
make lint     # golangci-lint
make bench    # full benchmark matrix (~18 min, local hardware only)
make plots    # regenerate summary.csv and charts
make chaos    # kill a worker under load, assert zero loss + <5s recovery
make ratelimit-test   # 10x burst enforcement accuracy
```

CI runs lint and unit tests only — inference benchmarks on shared runners
produce meaningless numbers, so they stay local and hardware-disclosed.
