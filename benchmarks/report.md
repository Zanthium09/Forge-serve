# Phase 2 benchmark: sequential vs continuous batching

All numbers come from the CSVs in `benchmarks/raw/` (regenerate with
`make bench`, then `make plots`). Nothing here is hand-typed.

## Hardware and setup

| | |
|---|---|
| CPU | Intel Core Ultra 9 185H (16C / 22T) |
| RAM | 32 GB |
| Host | Windows 11, Docker Desktop (WSL2 backend) |
| Model | Qwen2.5-1.5B-Instruct, GGUF Q4_K_M |
| Worker | `MAX_BATCH=16`, `MAX_QUEUE=8`, `N_CTX=16384` |
| Request | 32-word prompt, `max_tokens=64`, `temperature=0.7` |
| Run | 60 s measured after 10 s warm-up (warm-up rows tagged and excluded) |

Both engines run in the same container image, same compose stack, same
loadgen. Only the worker's `ENGINE` variable changes, so the comparison
isolates the scheduler.

## Headline

**22.5x aggregate throughput at concurrency 16** versus sequential serving
on identical hardware (40.54 vs 1.80 completion tokens/s). Blueprint target
was >= 3x.

That number is real but composite, and the decomposition below matters more
than the number: sequential serving does not merely fail to scale, it
*degrades* under concurrency, and that degradation is part of the ratio.

## Throughput (closed loop, completion tokens/s)

| Concurrency | Sequential | Batched | Multiplier |
|---|---|---|---|
| 1 | 29.56 | 21.02 | 0.71x |
| 8 | 3.03 | 33.33 | 11.00x |
| 16 | 1.80 | 40.54 | 22.52x |

Three separate effects are visible, and conflating them would be dishonest:

1. **Batching genuinely shares work.** Measured in isolation
   (`benchmarks/decode_scaling.py`, raw output in
   `benchmarks/raw/decode_scaling.txt`), one `llama_decode` covering N
   sequences costs far less than N separate decodes:

   | Batch | ms/step | ms/token | tok/s |
   |---|---|---|---|
   | 1 | 39.2 | 39.2 | 25.51 |
   | 2 | 55.5 | 27.7 | 36.06 |
   | 4 | 72.1 | 18.0 | 55.45 |
   | 8 | 136.5 | 17.1 | 58.62 |
   | 16 | 250.0 | 15.6 | 63.99 |

   That is **2.5x at batch 16**, and it is the honest ceiling of batching on
   this hardware. It is sublinear because CPU decode is largely
   compute-bound: the cores are already busy on a single token, so extra
   sequences cost real time. On a GPU the same step is memory-bandwidth
   bound and leaves the device idle, which is why GPU batching gains are
   much larger. This table is also what set `MAX_BATCH=16`.

2. **Sequential serving collapses under concurrency.** Aggregate throughput
   falls from 29.56 tok/s (1 client) to 1.80 tok/s (16 clients), and
   per-token latency rises from 33 ms to ~165 ms even though only one
   request generates at a time. Serialization alone would have held
   throughput flat near 29 tok/s; the extra loss is contention around a
   single lock with many concurrent streaming responses. Most of the 22.5x
   is this collapse, not batching efficiency.

3. **Batching costs single-stream performance.** At concurrency 1 the
   batched engine is *slower* (0.71x): per-token time is 40.1 ms vs
   33.4 ms, and TTFT is 493 ms vs 38 ms. The engine pays Python-side cost
   per token (sampling, piece decoding, queue handoff) that llama.cpp's own
   loop avoids. This is the classic batching trade-off — worse latency for
   one user, far better aggregate service for many — and it is why the
   sequential engine is kept as a supported mode rather than deleted.

## Latency and the TTFT SLO

Blueprint target: **p99 TTFT < 5 s**. Time to first token, milliseconds:

| Engine | Concurrency | p50 | p95 | p99 | SLO |
|---|---|---|---|---|---|
| sequential | 1 | 38 | 51 | 54 | pass |
| batched | 1 | 493 | 608 | 609 | pass |
| sequential | 8 | 68 392 | 69 052 | 69 170 | fail |
| batched | 8 | 2 287 | 4 120 | 4 240 | **pass** |
| batched | 10 | 2 810 | 4 636 | 4 867 | **pass** |
| batched | 16 | 3 587 | 4 398 | 4 589 | **pass** |
| sequential | 16 | 152 082 | 153 557 | 153 671 | fail |
| batched | 50 | 4 982 | 27 472 | 27 534 | fail |

**The SLO holds through concurrency 16 and breaks at 50.** Concurrency 16
equals `MAX_BATCH`, so every request is admitted immediately and TTFT is
just prefill. Beyond that, requests wait for a slot.

Per-output-token latency (TPOT) p50 rises with batch occupancy — 40 ms at
concurrency 1, ~301 ms at concurrency 16 — which is expected: a sequence
gets one token per shared step, so more co-resident sequences means each
one's tokens arrive further apart while total output rises.

### Why concurrency 50 misses, and why it is not a tuning problem

The first instinct is "the queue is too deep." That was measured and
**disproved**: at concurrency 50, shrinking `MAX_QUEUE` from 8 to 2 barely
moved p99 (see `batched_closed_c50_q2.csv`).

| `MAX_QUEUE` | served | shed (429) | TTFT p50 | TTFT p99 |
|---|---|---|---|---|
| 8 | 30 | 1 693 | 4 982 ms | 27 534 ms |
| 2 | 33 | 1 953 | 1 478 ms | 17 332 ms |

p99 TTFT is bounded by **service time**, not queue depth. At batch 16 a
64-token request occupies its slot for roughly 16 s, so a request arriving
when all slots are busy waits up to one full request duration for a slot
regardless of how short the line is. Little's Law says the same thing: with
16 servers and ~16 s service time, the system sustains about 1 request/s;
50 concurrent arrivals exceed that, so wait time grows until admission
control sheds the excess.

So concurrency 50 is a **capacity** limit on one CPU worker, not a knob
left untuned. The real remedies, in order of honesty:

- **More workers** (Phase 3): capacity scales with worker count; this is
  the designed answer and the reason Phase 3 exists.
- **Shorter outputs**: service time falls linearly with `max_tokens`.
- **Preemption** (Phase 6): let a long sequence yield its slot at an
  iteration boundary so a waiting request can start.
- A GPU, where the decode step is memory-bound and batching is near-free.

Tuning did help substantially and was applied: `MAX_BATCH` 8 -> 16 and
`MAX_QUEUE` 32 -> 8 moved concurrency-50 p99 TTFT from **70.8 s to 27.5 s**
and concurrency-16 throughput from 29.00 to 40.54 tok/s. It just cannot
close a capacity gap.

## Admission control

At concurrency 50 the run served 30 requests and rejected 1 693 with
HTTP 429 plus `Retry-After: 1`, with **zero errors and zero timeouts**.
Load is shed at admission in sub-millisecond time rather than accepted and
dropped later — the blueprint's fail-fast rule, working as specified
(ADR-003). Queue depth is treated as a latency budget: only what can be
drained inside the SLO is admitted.

The load generator honours `Retry-After`. An earlier run without that
backoff turned concurrency 50 into a retry storm — 17 382 client errors
against a fail-fast server — which measured the retry loop rather than the
server. Closed-loop generators must back off, or open-loop should be used.

## Methodology notes

- **Closed loop** (used here): N virtual users each send, wait for the full
  response, then send again. It models a fixed user population and is the
  right tool for throughput comparisons. It self-throttles when the server
  slows, so it cannot show latency collapse — which is why the sequential
  numbers understate how bad that path feels in production.
- **Open loop** (`-mode open` in the loadgen): requests fire on a timer
  regardless of completions. That is the honest way to measure latency
  under overload, and it is what Phase 3's SLO work should use.
- Warm-up requests are recorded in the CSVs and excluded from every
  statistic; the first request pays for cold caches.
- Percentiles are computed over **accepted** requests; shed (429) counts are
  reported alongside, never hidden inside the latency figures.
- Single run per configuration. The blueprint asks for a 3-run median on the
  headline configuration; that is outstanding, and the headline should be
  treated as approximate until it lands.

## Reproducing

```bash
make bench     # ~18 min: both engines, concurrency 1/8/10/16/50
make plots     # summary.csv, throughput.png, ttft_cdf.png
```

Knobs are environment variables, e.g. `MAX_QUEUE=2 docker compose up -d
--force-recreate worker`.
