# ADR-005: Retry from scratch, never migrate a stream mid-response

Status: accepted (Phase 3, 2026-07-25)

## Context

When a worker dies while the gateway is relaying its output, the gateway
must decide what the client sees. Options:

1. Migrate: re-issue the request to another worker and continue the stream.
2. Retry from scratch, but only when nothing has been sent yet.
3. Always fail the request.

## Decision

Option 2. A request that has emitted **zero tokens** to the client is
retried once on a different healthy worker. A request that has already
streamed at least one token is **not** retried; the client receives a
terminal SSE error event (`{"error":{"type":"worker_lost"}}`) and the
stream ends.

Non-stream (`stream:false`) requests buffer the whole response inside the
gateway, so nothing has reached the client until it is complete — those are
always retryable, and are.

## Rationale

- **Migration would require moving KV-cache state** between processes.
  The cache is the model's memory of this conversation; another worker has
  no idea what the first one had generated. Serialising and shipping it is
  possible in principle (vLLM-style prefix caching, paged attention) but is
  a large subsystem, and on CPU the transfer would cost more than the
  regeneration.
- **Re-generating without migration is worse than failing.** Sampling is
  stochastic: the second worker produces *different* text. The client would
  receive "The capital of Fra" followed by a fresh "France's capital is
  Paris" — a corrupt response that no client can detect or repair.
- **Zero tokens sent is a safe boundary.** Nothing is observable to the
  client yet, so a retry is indistinguishable from the request having been
  routed to the second worker in the first place. This is the same
  idempotency reasoning that governs HTTP request retries generally.
- **An explicit error beats a silent truncation.** Ending the stream
  without a marker is indistinguishable from a short completion; clients
  would treat a failure as a valid answer. The error event makes the
  failure machine-detectable.

## Consequences

- Long responses are the most expensive to lose, and they are exactly the
  ones that cannot be retried. Accepted: correctness over convenience.
- "Zero accepted-request loss" therefore means: every accepted request
  either completes, is retried and completes, or receives an explicit
  error. It does not mean every request produces a full answer.
- Clients should treat `worker_lost` as retryable at the application level,
  where re-asking the question is safe and visible to the user.
