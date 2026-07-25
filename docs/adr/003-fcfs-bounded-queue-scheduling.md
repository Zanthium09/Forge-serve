# ADR-003: FCFS admission with a bounded queue

Status: accepted (Phase 2, 2026-07-25)

## Context

The continuous batching engine has `MAX_BATCH` slots. When all slots are
busy, arriving requests must wait somewhere, and the waiting line needs a
policy and a limit. Candidates considered:

- FCFS (first come, first served) with a bounded queue.
- SJF (shortest job first) — admit the request predicted to finish soonest.
- Unbounded queue with no admission control.

## Decision

FCFS admission from a bounded waiting queue (`MAX_QUEUE`, default 32).
When the queue is full the worker answers 429 immediately; the gateway
maps that to a client 429 with `Retry-After: 1`.

## Rationale

**FCFS over SJF.** SJF raises average throughput when job lengths are
known, but we do not know output length in advance — `max_tokens` is an
upper bound, not a prediction, and requests routinely stop early at EOS.
Estimating length would need a predictor whose errors cause starvation for
long requests. FCFS is predictable, trivially fair, and its behaviour is
easy to defend. Blueprint Phase 6 revisits scheduling with priority
classes; aging belongs there, not here — with pure FCFS and no priorities,
starvation is impossible by construction, so aging would be dead code.

**Bounded queue over unbounded.** An unbounded queue converts overload
into latency: requests are accepted, wait minutes, and time out at the
client after the server has already spent resources on them. Little's Law
makes this concrete — with arrival rate λ above service rate μ, queue
length grows without bound and so does wait time. Rejecting at admission
costs one cheap 429 and lets the client decide (retry, back off, shed).
This is the blueprint's "fail fast, never accept-then-drop" rule.

**Where cancellation fits.** A cancelled request is removed from the
waiting line immediately (swept at the top of every iteration), never
prefilled, and if already decoding it is evicted at the next iteration
boundary and its slot freed. Wasted work is bounded by one decode step.

## Consequences

- A long request can hold a slot for a long time; short requests behind it
  wait. Acceptable at Phase 2 scale; Phase 6's priority classes and
  optional preemption address it if benchmarks show it hurts p99.
- `MAX_QUEUE` is a **latency budget, not a buffer size**. A slot frees
  roughly every (service time / `MAX_BATCH`), so the last waiter in a queue
  of N pays about N times that interval before its first token. Size the
  queue to what can be drained inside the TTFT SLO; anything deeper is a
  promise the server cannot keep and is rejected at admission instead.
  Defaults are `MAX_BATCH=16` (chosen from measured decode scaling) and
  `MAX_QUEUE=8`.
- Queue depth cannot rescue an over-capacity system. Measured at
  concurrency 50 (`benchmarks/report.md`): shrinking `MAX_QUEUE` from 8 to
  2 moved p99 TTFT only 27.5 s -> 17.3 s, because p99 is bounded by service
  time — an arrival waits up to one full request duration for a slot no
  matter how short the line. Capacity beyond that comes from more workers
  (Phase 3) or preemption (Phase 6), not from tuning.

## Related simplifications (recorded here so they are not silently lost)

- **ChatML is hardcoded** in `worker/chatml.py`. Owning the decode loop
  means owning prompt formatting; the served model is pinned to Qwen2.5 in
  config and compose. Serving a second model family requires a template
  per model — a Phase 7 multi-model concern.
- **Sampling is temperature-only** (softmax over logits, argmax at
  temperature 0). `top_p`/`top_k` are omitted until a measured need; the
  OpenAI schema accepts them but the worker ignores them today.
