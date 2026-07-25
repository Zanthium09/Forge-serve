# ADR-006: Rate limiter degrades to local buckets when Redis is down

Status: accepted (Phase 4, 2026-07-26)

## Context

Per-key token buckets live in Redis so that several gateway replicas share
one view of a tenant's quota. Redis is therefore on the request path. When
it is unreachable, the limiter must choose:

1. **Fail closed** — reject every request (quota is unknowable, so deny).
2. **Fail open** — allow every request (ignore limits until Redis returns).
3. **Degrade** — enforce the same quota in an in-process bucket per replica.

## Decision

Option 3. Each gateway keeps a local token bucket per key and uses it
whenever a Redis call fails. Every request retries Redis first, so
recovery is automatic and needs no separate health check. The degraded
state is exported as `forge_limiter_local_fallback_total`.

## Rationale

- **Fail closed turns a Redis blip into a total outage.** The rate limiter
  exists to protect the service from abuse, not to be a second thing that
  can take it down. An availability dependency on Redis for *serving* is
  worse than the accuracy it buys.
- **Fail open is an invitation.** An attacker who can degrade Redis (or
  simply wait for a restart) gets unlimited quota. Load shedding must not
  have an off switch reachable from the network.
- **Local buckets keep the shape of the policy.** A single replica still
  enforces burst and refill exactly; only the *aggregate* across replicas
  drifts.

## Consequences

- **Accuracy degrades predictably**: with N gateway replicas in fallback,
  a key can spend up to N times its configured rate. That is the explicit
  trade — availability over strict accuracy — and it is bounded and
  measurable, not unbounded.
- At the current single-replica deployment the fallback is *exactly*
  accurate; the drift only appears when the gateway is scaled out, which
  is also when Redis is most likely to be highly available anyway.
- Buckets do not survive a gateway restart in fallback mode. A restart
  refills every local bucket, which is a small, one-off over-allowance.
- The blueprint's ">99% enforcement accuracy under 10x burst" target is
  measured with Redis healthy; the fallback path is a separate, documented
  mode, not the number being claimed.
