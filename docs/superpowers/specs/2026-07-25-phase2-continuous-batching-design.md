# Phase 2 — Continuous Batching: Design

Approved by owner 2026-07-25. Scope per FORGE_SERVE_BLUEPRINT.md §8 Phase 2.

## Problem

The Phase 1 worker generates for one request at a time (lock); concurrent
requests queue linearly (measured: 10 concurrent requests finished 9s→24s,
a staircase). A decode step's cost is dominated by reading model weights
from memory — one step for N sequences costs nearly the same as for one.
Continuous batching shares each step across all active requests, admits new
requests at iteration boundaries, and evicts finished ones immediately.

## Decision: own the decode loop (low-level llama.cpp API)

Rejected: high-level API time-slicing (not real batching, no speedup) and
llama.cpp's built-in server (outsources the crown jewel). We drive
`llama_batch` / `llama_decode` / per-sequence KV-cache ops ourselves via
llama-cpp-python's low-level bindings, pinned to an exact version.

## Engine (`worker/engine.py`)

One decode-loop thread owns the model; requests communicate via queues.
Sequence state machine (explicit, invariants commented):

```
WAITING → PREFILL → DECODING → FINISHED
              └──────┴───→ CANCELLED (client gone)
```

Loop iteration = admit → step → evict:
1. **Admit** (FCFS): while a slot is free and waiters exist — assign seq id,
   prefill prompt into KV cache.
2. **Step**: one `llama_batch` with the newest token of every DECODING seq,
   one `llama_decode`, sample next token per seq (temperature; greedy at 0;
   top_p deferred), push to that request's output queue.
3. **Evict**: finished (EOS/max_tokens) or cancelled → free KV range + slot
   immediately; slot reusable next iteration.

Per-seq state: prompt tokens, generated count, sampler params, output queue,
cancel flag, deadline. Knobs (env): `MAX_BATCH` (8), `N_CTX` (16384),
`MAX_QUEUE` (32), per-request length cap.

**Testability:** scheduler logic is isolated behind a `ModelBackend`
protocol (`prefill(seq)`, `step(seqs) → token per seq`). pytest drives it
with a scripted fake backend — admission order, slot reuse, eviction,
cancellation, queue-cap rejection all covered in CI with no model.
`worker/backend_llama.py` implements the protocol for real (all low-level:
model load, tokenize, decode, sample via logits + numpy, detokenize,
KV ops); `worker/kvcache.py` holds seq-id/slot bookkeeping.

**Chat template:** ChatML formatted by our own function (model is pinned to
Qwen2.5 in config); noted in ADR-003.

**Both engines stay:** `ENGINE=sequential|batched` env selects Phase 1 path
or the new engine — the A/B benchmark runs on identical infrastructure.

## Backpressure

Bounded waiting queue (`MAX_QUEUE`). Full → worker responds 429 instantly;
gateway maps worker 429 → client 429 + `Retry-After: 1`. Worker `/stats`
endpoint (queue_depth, active_seqs) — gateway polls it into Prometheus
gauges (queue depth, batch occupancy).

## Loadgen (`loadgen/`, separate Go module)

CLI: closed-loop (`-concurrency N`: N users send→wait→send) for the
headline conc=16 comparison; open-loop (`-rate R`: fire on a timer
regardless of responses) for honest latency percentiles. Streaming requests;
per request one CSV row: start, TTFT, total, tokens, TPOT, status. Flags:
url, key, mode, rate, concurrency, duration, max-tokens, prompt size, out.

## Benchmark (`benchmarks/`)

`make bench` matrix: {sequential, batched} × closed conc {1, 8, 16} +
open-loop rates for TTFT p50/p95/p99 at conc 10 and 50. Python plot script
(pandas+matplotlib) → charts; `report.md` with methodology (open vs closed
loop, warm-up excluded, 3-run median, hardware disclosed: Ultra 9 185H,
32 GB, Windows 11/Docker). Raw CSVs committed.

## Exit criteria

1. Throughput multiplier at conc=16 measured; target ≥3× (report actual).
2. TTFT p99 at conc 10 and 50 in report.
3. Cancellation evicts a sequence from a live batch: pytest (fake backend)
   + observed live (eviction log, occupancy drop).

## ADR + CI

ADR-003: FCFS + bounded queue; aging deferred until priority classes
(Phase 6). CI: add worker pytest job (scheduler tests only). Pin
llama-cpp-python exact version.

## Risks

Low-level bindings are version-sensitive ctypes: first step is probing the
pinned version's actual symbol names inside the container; segfault-class
bugs are expected during bring-up — mitigated by the fake-backend split
(logic proven independently) and by keeping the sequential engine as
fallback.
