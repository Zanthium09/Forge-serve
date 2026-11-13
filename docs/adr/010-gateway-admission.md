# ADR-010: Admission control moves into the gateway

Status: accepted (production gateway, 2026-11-12)

## Context
Our own worker rejected work when its queue was full (ADR-003). vLLM never
rejects: it queues indefinitely, which turns overload into slow responses.

## Decision
Each backend has two limits from config: `max_waiting` (engine-reported
queue length, used only when the engine exposes it) and `max_inflight` (the
gateway's own count of requests in flight to that backend, default 64).
When every healthy backend of a model is at a limit, the gateway answers
429 + `Retry-After`, or uses the model's fallback (ADR-014). Spec §6.4.

## Rationale
- `max_inflight` works with every engine and is not 2 s stale like the poll.
- `max_waiting` is the fleet-wide signal when several gateway copies run.

## Consequences
With several gateway copies, `max_inflight` applies per copy. Documented.
