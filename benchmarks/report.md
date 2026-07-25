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
| Worker | `MAX_BATCH=8`, `MAX_QUEUE=32`, `N_CTX=4096` |
| Request | 32-word prompt, `max_tokens=64`, `temperature=0.7` |
| Run | 60 s measured after 10 s warm-up (warm-up rows tagged and excluded) |

Both engines run in the same container image, same compose stack, same
loadgen. Only the worker's `ENGINE` variable changes, so the comparison
isolates the scheduler.

## Headline

**16.1x aggregate throughput at concurrency 16** versus sequential serving
on identical hardware (29.00 vs 1.80 completion tokens/s). Blueprint target
was >= 3x.

That number is real but it needs its decomposition, below: sequential
serving does not merely fail to scale, it *degrades* under concurrency, and
that degradation is part of the ratio.

## Throughput (closed loop, completion tokens/s)

| Concurrency | Sequential | Batched | Multiplier |
|---|---|---|---|
| 1 | 29.56 | 21.02 | 0.71x |
| 8 | 3.03 | 36.57 | 12.07x |
| 16 | 1.80 | 29.00 | 16.11x |

Three separate effects are visible here, and conflating them would be
dishonest:

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

   That is **2.5x at batch 16** and it is the honest ceiling of batching on
   this hardware. It is sublinear because CPU decode is largely
   compute-bound: the cores are already busy on a single token, so extra
   sequences cost real time. On a GPU the same step is memory-bandwidth
   bound and leaves the device idle, which is why GPU batching gains are
   much larger.

2. **Sequential serving collapses under concurrency.** Its aggregate
   throughput falls from 29.56 tok/s (1 client) to 1.80 tok/s (16 clients),
   and per-token latency rises from 33 ms to ~165 ms even though only one
   request generates at a time. Serialization alone would have held
   throughput flat near 29 tok/s; the extra loss is contention around a
   single lock with many concurrent streaming responses. Most of the 16.1x
   is this collapse, not batching efficiency.

3. **Batching costs single-stream performance.** At concurrency 1 the
   batched engine is *slower* (0.71x): per-token time is 40.1 ms vs
   33.4 ms, and TTFT is 493 ms vs 38 ms. The engine pays Python-side cost
   per token (sampling, piece decoding, queue handoff) that llama.cpp's own
   loop avoids. This is the classic batching trade-off - worse latency for
   one user, far better aggregate service for many - and it is the reason
   the sequential engine is kept as a supported mode rather than deleted.

## Latency

Time to first token, milliseconds:

| Engine | Concurrency | p50 | p95 | p99 |
|---|---|---|---|---|
| sequential | 1 | 38 | 51 | 54 |
| batched | 1 | 493 | 608 | 609 |
| sequential | 8 | 68 392 | 69 052 | 69 170 |
| batched | 8 | 2 370 | 4 067 | 4 325 |
| batched | 10 | 3 508 | 15 880 | 16 508 |
| batched | 16 | 16 394 | 18 182 | 18 540 |
| sequential | 16 | 152 082 | 153 557 | 153 671 |
| batched | 50 | 69 960 | 70 611 | 70 782 |

Per-output-token latency (TPOT) p50 rises with batch occupancy - 40 ms at
concurrency 1, ~187 ms at concurrency 16 - which is expected: a sequence
gets one token per shared step, so more co-resident sequences means each
one's tokens arrive further apart while total output rises.

**Blueprint target not met:** p99 TTFT < 5 s at concurrency 50. Measured
70.8 s for served requests. Cause is structural: with `MAX_BATCH=8` and
`MAX_QUEUE=32`, an admitted request can sit behind four full batch turns,
and each turn is ~11 s of generation at 64 tokens. The decode-scaling table
shows the lever - batch 16 delivers 64 tok/s versus batch 8's 58.6 tok/s -
so raising `MAX_BATCH` (memory permitting) both raises throughput and
shortens the queue. Left as a tuning follow-up rather than a silent
re-run at a friendlier setting.

## Admission control

At concurrency 50, the run served 24 requests and rejected 656 with
HTTP 429 plus `Retry-After: 1`, with **zero errors and zero timeouts**.
Load is shed at admission in sub-millisecond time rather than accepted and
dropped later - the blueprint's fail-fast rule, working as specified
(ADR-003).

The load generator honours `Retry-After`. An earlier run without that
backoff turned concurrency 50 into a retry storm - 17 382 client errors
against a fail-fast server - which measured the retry loop rather than the
server. Closed-loop generators must back off, or open-loop should be used
instead.

## Methodology notes

- **Closed loop** (used here): N virtual users each send, wait for the full
  response, then send again. It models a fixed user population and is the
  right tool for throughput comparisons. It self-throttles when the server
  slows, so it cannot show latency collapse - which is why the sequential
  numbers understate how bad that path feels in production.
- **Open loop** (`-mode open` in the loadgen): requests fire on a timer
  regardless of completions. That is the honest way to measure latency
  under overload, and it is what Phase 3's SLO work should use.
- Warm-up requests are recorded in the CSVs and excluded from every
  statistic; the first request pays for cold caches.
- Single run per configuration. The blueprint asks for a 3-run median on
  the headline configuration; that is outstanding work, and the headline
  should be treated as approximate until it lands.

## Reproducing

```bash
make bench     # ~18 min: both engines, concurrency 1/8/10/16/50
make plots     # summary.csv, throughput.png, ttft_cdf.png
```
