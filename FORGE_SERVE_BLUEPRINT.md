# FORGE-SERVE — Project Blueprint

> **Purpose of this file:** This is the master specification for Forge-Serve. It is written to be consumed by Claude Code as project context (referenced from `CLAUDE.md`). It defines what to build, in what order, with what constraints.
>
> **Owner:** Rushabh Patwatkar — B.Tech CSE (Data Science), targeting Backend / Platform / AI-Infrastructure roles.
>
> **Working rule for AI-assisted development:** The owner must be able to defend every design decision in an interview. When generating code, always explain the *why* (concurrency choices, data structures, trade-offs) in comments and in PR-style summaries. Prefer smaller, understandable implementations over clever ones.

---

## 1. One-line pitch

A self-hosted, distributed **LLM inference gateway and serving platform** for open-weight models — an OpenAI-compatible API in front of self-managed model workers, with continuous batching, streaming, routing, rate limiting, failover, and full observability. Think "mini vLLM + API gateway + control plane," built from scratch.

**Key principle: the model is a commodity; every hard part is ours.** No external LLM APIs. We run small open-weight models locally (llama.cpp, CPU-only, quantized 1–3B) and build the entire serving system around them.

## 2. Problem it solves

Companies self-hosting open models waste hardware: naive one-request-at-a-time serving leaves 10–20× throughput on the table, traffic is bursty, and multi-tenant fairness is unsolved out of the box. Forge-Serve maximizes hardware utilization and enforces per-tenant SLOs for self-hosted inference.

## 3. Success criteria (what "done" means)

The project is interview-ready when ALL of these are true:

1. A **demo GIF** in the README shows tokens streaming from a real local model via an OpenAI-compatible endpoint, and `docker compose up` brings the whole system up with one command. *(Public demo URL on a cloud VM is deferred until the owner opts to rent one; the system is built cloud-ready so this is a deploy step, not a rewrite.)*
2. A benchmark report shows **continuous batching vs. sequential serving** with a headline throughput multiplier (target: **≥3×** on CPU — report the measured number; hardware may deliver more) and TTFT/TPOT latency percentiles at concurrency 10/50/200.
3. Killing a worker mid-load causes zero accepted-request loss (failed streams are retried or cleanly errored) with recovery < 5s, proven by an automated chaos test.
4. Rate limiting enforces per-key quotas with >99% accuracy under 10× burst, proven by the load harness.
5. A Grafana dashboard (local; screenshots committed to README) shows live p50/p95/p99 TTFT, tokens/sec, queue depth, batch occupancy. *(Public read-only Grafana comes with the cloud VM, later.)*
6. README contains: architecture diagram, demo GIF, benchmark table, quickstart that works with one command (`docker compose up`).

---

## 4. Architecture

Three planes:

```
                        ┌─────────────────────────────────────────────┐
                        │                CLIENTS                       │
                        │   (OpenAI SDKs, curl, playground web UI)     │
                        └──────────────────┬──────────────────────────┘
                                           │ HTTPS (SSE streaming)
                        ┌──────────────────▼──────────────────────────┐
   DATA PLANE           │  GATEWAY (Go)                                │
                        │  - /v1/chat/completions (OpenAI-compatible)  │
                        │  - API-key auth                              │
                        │  - token-bucket rate limiting (Redis-backed) │
                        │  - request validation, admission control     │
                        │  - SSE streaming w/ cancellation             │
                        └──────────────────┬──────────────────────────┘
                                           │ HTTP/1.1 chunked streaming
                        ┌──────────────────▼──────────────────────────┐
                        │  SCHEDULER / ROUTER (Go, inside gateway svc) │
                        │  - priority queue per model                  │
                        │  - least-outstanding-tokens routing          │
                        │  - backpressure: reject early when saturated │
                        └──────┬──────────────────┬───────────────────┘
                               │                  │
                    ┌──────────▼─────┐   ┌────────▼───────┐
                    │ WORKER 1       │   │ WORKER 2..N     │
                    │ (Python +      │   │                 │
                    │  llama.cpp)    │   │                 │
                    │ - continuous   │   │                 │
                    │   batching loop│   │                 │
                    │ - KV-cache mgmt│   │                 │
                    └────────────────┘   └─────────────────┘

   CONTROL PLANE:  worker registry, heartbeats, health checks, drain/failover.
                   (MVP: in-process registry in gateway. Later: separate service.)

   OBSERVABILITY:  Prometheus metrics + Grafana dashboards + structured logs.
                   (Later: OpenTelemetry traces.)

   STATE:          Redis  → distributed rate-limit buckets
                   Postgres → tenants, API keys (hashed), usage accounting
```

### Request lifecycle (happy path)

1. Client POSTs to `/v1/chat/completions` with `stream: true`.
2. Gateway authenticates key (Postgres-cached), checks token bucket (Redis), validates payload.
3. Admission control: if queue depth > threshold → immediate 429 with `Retry-After` (fail fast, never time out late).
4. Scheduler enqueues; router picks the worker with fewest outstanding tokens.
5. Worker inserts the sequence into its running batch at the next decode step (continuous batching).
6. Tokens stream back worker → gateway → client via SSE. Client disconnect propagates cancellation all the way to the batch (sequence evicted).
7. On completion: usage (prompt/completion tokens) written to Postgres asynchronously.

### Failure paths (must be handled, and documented)

- Worker dies mid-stream → gateway detects via heartbeat timeout + broken stream; in-flight requests for that worker are retried from scratch once on another worker if no tokens were sent, else the client receives a clean error event. (Decision: no mid-stream migration — document why in ADR.)
- Redis unavailable → rate limiter degrades to local in-process buckets (availability over strict accuracy — document in ADR).
- Queue saturation → 429 at admission, never accept-then-drop.

---

## 5. Tech stack (fixed — do not substitute without an ADR)

| Component | Technology | Why |
|---|---|---|
| Gateway, scheduler, control plane | **Go 1.22+** | Infra lingua franca; goroutines map to per-request streaming; strong hiring signal |
| Inference worker | **Python 3.11 + llama-cpp-python** (MVP) | Fastest path to a working batched runtime on CPU; can drop to C++ later |
| Model | Quantized 1–3B open-weight (Llama-3.2-1B or Qwen2.5-1.5B, GGUF Q4) | Runs on CPU; scheduling logic identical to GPU serving |
| Rate-limit state | **Redis** | Atomic Lua token buckets, low latency |
| Tenants/keys/usage | **PostgreSQL** | Relational fit, transactional usage accounting |
| Metrics | **Prometheus + Grafana** | Industry standard; dashboard is a recruiter artifact |
| Load testing | **Custom Go load generator** (`/loadgen`) | Open-loop load generation; own the methodology |
| Packaging | **Docker + docker-compose** (MVP); k3s in Phase 3 | One-command quickstart |
| CI | **GitHub Actions** | Lint, test, build on PR (no inference benchmarks in CI — see §8 Phase 0) |

Worker ↔ gateway protocol: **HTTP/1.1 with chunked streaming.** One stream per request means HTTP/2 multiplexing buys nothing; plain HTTP is the simplest debuggable choice. (Revisit via ADR only if a measured need appears.)

### Development environment

- **Host:** Windows 11, Intel Ultra 9 185H (16C/22T), 32 GB RAM.
- **Workflow:** develop natively on Windows (Go, Python, editor, PowerShell). **Docker Desktop** (which auto-installs WSL2 as its engine — no day-to-day Linux interaction needed) runs infra containers (Redis, Postgres, Grafana) and the full compose demo.
- **Benchmarks:** run natively, hardware disclosed as "Windows 11, Ultra 9 185H, 32 GB" in every report.

## 6. Repository structure

```
forge-serve/
├── CLAUDE.md                  # Claude Code instructions (points to this file)
├── FORGE_SERVE_BLUEPRINT.md   # this file
├── README.md                  # recruiter-facing: pitch, diagram, GIF, benchmarks, quickstart
├── docker-compose.yml
├── Makefile                   # make run / test / bench / lint / model (GGUF download)
├── gateway/                   # Go module
│   ├── cmd/gateway/main.go
│   └── internal/
│       ├── api/               # HTTP handlers, OpenAI-compatible schema, SSE
│       ├── auth/              # API-key auth, hashing, caching
│       ├── ratelimit/         # token bucket (local + Redis Lua)
│       ├── scheduler/         # queues, admission control, routing
│       ├── registry/          # worker registry, heartbeats, health
│       ├── store/             # Postgres access (tenants, usage)
│       └── metrics/           # Prometheus instrumentation
├── worker/                    # Python package
│   ├── server.py              # HTTP streaming server
│   ├── engine.py              # continuous batching loop  ← crown jewel
│   ├── kvcache.py             # cache bookkeeping / eviction
│   └── config.py
├── loadgen/                   # Go load generator + workload replay
├── deploy/                    # compose files, later k3s manifests
├── benchmarks/                # scripts, raw results (CSV), report.md, plots
├── docs/
│   ├── architecture.md        # diagrams (mermaid), request lifecycle
│   └── adr/                   # Architecture Decision Records, numbered
└── .github/workflows/ci.yml
```

## 7. Coding conventions

- Go: standard project layout, `golangci-lint`, table-driven tests, contexts propagated everywhere, no global mutable state, graceful shutdown on SIGTERM (drain in-flight streams).
- Python worker: type hints, `ruff`, single event loop, no blocking calls in the batching loop except the model step itself.
- Every non-obvious design choice gets either a code comment explaining *why* or an ADR in `docs/adr/`.
- Commits: small, conventional-commit style (`feat(gateway): ...`), each phase = one milestone with issues.
- Tests: unit tests for scheduler and rate limiter logic (these are the interview-critical components); integration test that spins up compose and runs a smoke completion **against a stub engine**; chaos test script that kills a worker under load.

---

## 8. Build phases

**Pacing: the owner's available time is unpredictable (placements ongoing). Therefore every phase exit is a resume-usable checkpoint** — stopping after any completed phase still leaves a defensible project. Do not start a later phase until the current phase's exit criteria pass.

### Phase 0 — Skeleton (Days 1–2)

- **Day zero:** verify/install Docker Desktop (checks virtualization); Makefile target `make model` downloads the quantized GGUF from Hugging Face.
- Repo scaffold per structure above, Makefile, docker-compose with Redis + Postgres + placeholder services, CI running lint+test.
- CI policy: **lint + unit tests + stub-engine integration test only.** No inference benchmarks in CI — shared runners make the numbers meaningless and slow every PR. Real benchmarks run locally on disclosed hardware.
- Gateway serves `/healthz` and a stub `/v1/chat/completions` that streams fake tokens over SSE ("lorem ipsum" at 20 tok/s).
- **Exit:** `curl -N` shows streamed fake tokens; client disconnect cancels the goroutine (verify with logs).

### Phase 1 — Real inference, single worker (Days 3–7)

- Worker: llama-cpp-python loads a quantized 1B GGUF; simple sequential generation endpoint with token streaming.
- Gateway proxies to worker; full OpenAI-compatible request/response schema (messages, temperature, max_tokens, stream + non-stream modes, usage object).
- API-key auth: keys hashed (SHA-256) in Postgres, LRU-cached in gateway; `Authorization: Bearer` parsing.
- Basic Prometheus metrics: request count, TTFT histogram, tokens/sec gauge.
- **Exit:** real model streams through the gateway; OpenAI Python SDK pointed at the gateway works unmodified; concurrency-10 load test passes without deadlock.

### Phase 2 — Continuous batching (Days 8–14) ← THE CROWN JEWEL

- **Known technical risk, stated upfront:** continuous batching requires llama-cpp-python's **low-level API** (`llama_batch`, `llama_decode` with per-sequence IDs, per-sequence KV-cache management). The high-level `create_completion()` API cannot interleave sequences. Budget learning time for this; it is the fiddliest code in the project.
- Replace sequential generation with a batching engine:
  - Central decode loop steps ALL active sequences once per iteration.
  - New requests are inserted into the batch at iteration boundaries (no waiting for the batch to finish).
  - Finished sequences are evicted immediately; their slots freed.
  - Per-sequence state: prompt tokens, generated tokens, KV-cache handle, deadline, client-cancel flag.
  - Batch size cap (start: 16) + queue with FCFS-with-aging policy (document policy in ADR; SJF variant is a Phase 6 option).
- Backpressure: worker exposes queue depth; gateway admission control rejects (429 + Retry-After) beyond threshold.
- Loadgen: open-loop generator with configurable arrival rate, prompt-length distribution, and per-request TTFT/TPOT recording to CSV.
- Benchmark: sequential vs. continuous batching on identical hardware; produce `benchmarks/report.md` with plots.
- **Exit:** headline number exists (target **≥3×** throughput at concurrency 16 — report measured); p99 TTFT measured at 10/50 concurrency; cancellation evicts sequences from a live batch.

### Phase 3 — Multi-worker + failover (Days 15–19)

- Worker registry in gateway: workers register on boot, heartbeat every 2s, marked unhealthy after 3 misses.
- Routing: least-outstanding-tokens across healthy workers.
- Failover: heartbeat loss → drain routing; in-flight requests with zero tokens sent are retried once elsewhere; others get a clean SSE error event. ADR: why no mid-stream migration.
- Chaos test script: sustained load, `docker kill` one worker, assert zero accepted-request loss and recovery <5s.
- **Exit:** chaos test green in CI (or as a documented manual run with logs committed).

### Phase 4 — Distributed rate limiting + usage + polish (Days 20–26)

- Redis Lua token bucket per API key (capacity, refill rate from the key's tenant plan); local-bucket fallback when Redis is down (ADR: availability vs. accuracy).
- Async usage accounting to Postgres (batched inserts); `/v1/usage` endpoint per key.
- Grafana dashboard: TTFT/TPOT percentiles, tokens/sec, queue depth, batch occupancy, cache/ratelimit counters. Provision via JSON in `deploy/`.
- Demo GIF (screen recording of streaming through the playground page — minimal static HTML hitting the gateway) + Grafana screenshots for README. *(Cloud VM deploy + public URLs: deferred until owner opts in; everything is compose-packaged so this is a later deploy step.)*
- README finalization: diagram, GIF, benchmark table, quickstart, "hardest problems" section.
- **Exit:** all six success criteria in §3 pass. ← MVP COMPLETE, resume-ready.

### Phase 5 (post-MVP) — Prompt caching

- Exact-match prompt cache (hash of normalized request → response) with LRU + TTL; measure hit-rate vs. latency on a replayed workload.

### Phase 6 (post-MVP) — Advanced scheduling

- Priority classes per tenant; aging to prevent starvation; optionally preemption (long request yields at iteration boundary). Benchmark fairness: p99 of short interactive requests with and without a long-request flood.

### Phase 7 (post-MVP) — Pick ONE

- Paged KV-cache management, OR speculative decoding with a draft model, OR multi-model routing, OR k3s deployment with a queue-depth autoscaler.

---

## 9. Benchmarks to publish (README table)

| Metric | How measured | Target |
|---|---|---|
| Throughput multiplier (continuous vs sequential) | loadgen, identical HW, conc=16 | ≥3× (report measured) |
| Aggregate tokens/sec | loadgen sweep, batch 1→32 | report curve |
| TTFT p50/p95/p99 | conc = 10 / 50 / 200 | report; p99 < 5s at conc=50 |
| TPOT (per-token latency) p50/p99 | same runs | report |
| Failover recovery time | chaos test | <5s, zero accepted-request loss |
| Rate-limit enforcement accuracy | 10× burst vs configured limit | >99% |
| Cache hit-rate → latency reduction (Phase 5) | replayed workload | report |
| Cost | tokens per ₹ on the benchmark machine | report vs hosted API pricing |

Methodology notes to include: open-loop vs closed-loop load generation, warm-up exclusion, fixed prompt-length distributions, hardware spec disclosure. **At concurrency 200, admission control will 429 most requests by design — percentiles are reported over accepted requests, and the rejection rate is reported alongside.**

## 10. Interview-defense checklist (owner must be able to answer)

- Why does batching increase throughput? (memory-bandwidth-bound decode; arithmetic intensity)
- Continuous vs static batching — what exactly changes?
- What is the KV cache, why does it dominate memory, what happens without it?
- Why least-outstanding-tokens routing instead of round-robin?
- Why fail-fast admission control instead of queueing everything?
- Token bucket math; why Lua scripts in Redis; what breaks with naive GET/SET?
- Why retry-from-scratch instead of mid-stream migration on worker death?
- SSE vs WebSockets vs gRPC streaming — why SSE for the client edge?
- How does client cancellation propagate through gateway → scheduler → batch?
- Little's Law applied to the queue: predict latency from arrival rate and service rate.
- Graceful shutdown: what happens to in-flight streams on SIGTERM?
- Security: why hash API keys; tenant isolation; what's logged and what must never be.

## 11. Instructions for Claude Code sessions

1. Read this file fully before making changes. Work phase by phase; never skip exit criteria.
2. At the start of each session, state which phase and which task is being worked on.
3. After implementing any component, produce a short "why it's built this way" summary suitable for `docs/` — the owner will be interviewed on this code.
4. Keep the crown jewel (Phase 2 batching engine) as readable as possible: explicit state machine, named invariants in comments, unit tests for insertion/eviction/cancellation edge cases.
5. Write ADRs (`docs/adr/NNN-title.md`) for: streaming protocol choice, no-mid-stream-migration, rate-limiter degradation mode, scheduling policy, worker protocol.
6. Never add features from a later phase while a current phase's exit criteria are unmet. Cut scope before cutting benchmarks or docs.
7. All benchmark numbers in README must come from committed raw CSVs in `benchmarks/` — no hand-typed numbers.

## 12. Resume bullets (template — fill with real measured numbers)

> **Forge-Serve — Distributed LLM Inference Platform** *(Go, Python/llama.cpp, Redis, PostgreSQL, Prometheus, Docker)*
> - Built a self-hosted model-serving platform with an OpenAI-compatible streaming API, continuous-batching scheduler, and multi-worker failover; sustained **X tok/s at Y concurrent streams** with **Z ms p99 TTFT**
> - Designed a continuous batching engine achieving **N× throughput** over sequential serving on identical hardware
> - Implemented Redis-backed distributed rate limiting with **>99% enforcement accuracy** under 10× burst, and chaos-tested failover recovering in **<T s** with zero accepted-request loss

## 13. Amendment log — 2026-07-24 (pre-implementation review)

Changes from the original draft, agreed with the owner before any code was written:

1. **Public demo URL → demo GIF + one-command quickstart** (§3.1, §3.5, Phase 4). Owner deferred cloud VM spend; system stays cloud-ready so deploying later is a deploy step, not a rewrite.
2. **Throughput target 5× → ≥3×, report measured** (§3.2, Phase 2 exit, §9). CPU batching gains are real (amortized weight-memory reads) but 5× at conc=16 on a laptop is a promise, not a measurement. Promise the safe number; report the real one.
3. **Worker↔gateway protocol: HTTP/1.1 chunked streaming** (§4, §5). One stream per request — HTTP/2/gRPC multiplexing buys nothing; simplest debuggable choice wins.
4. **Dropped k6** (§5). The custom Go loadgen is the methodology; a second tool is decoration.
5. **CI never runs inference benchmarks** (§5, Phase 0). Shared runners produce meaningless numbers; CI tests against a stub engine, benchmarks run locally on disclosed hardware.
6. **Dev environment pinned** (§5): native Windows 11 development; Docker Desktop (WSL2 as invisible engine) for infra containers and the compose demo. Hardware disclosed in every benchmark: Ultra 9 185H, 32 GB.
7. **Phase 2 risk named**: continuous batching requires llama-cpp-python's low-level API; the high-level API cannot interleave sequences.
8. **Pacing rule** (§8): owner's time is unpredictable; every phase exit is a resume-usable checkpoint.
9. **Conc-200 benchmark honesty** (§9): admission control 429s most requests by design; report accepted-request percentiles plus rejection rate.
