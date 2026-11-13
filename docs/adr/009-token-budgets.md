# ADR-009: Token budgets: check before, count after, skip when Redis is down

Status: accepted (production gateway, 2026-11-12)

## Context
Daily/monthly token budgets per user and monthly caps per team need a fast
counter on the request path. A request's size is unknown until it ends.

## Decision
Counters live in Redis (`forge:tok:...`, UTC windows). The check happens
before forwarding, against usage so far; the request's tokens are added
after it ends. A missing counter is rebuilt from Postgres `usage_events`.
If Redis is unavailable, budget checks are skipped (allowed) and
`forge_budget_check_skipped_total` is incremented. Spec §7.3.

## Rationale
- Pre-reserving `max_tokens` would reject requests that would have fit:
  most requests stop far below their maximum.
- Postgres keeps every usage row, so a Redis outage pauses enforcement but
  loses no data: the same availability-over-accuracy choice as ADR-006.

## Consequences
A single request may overshoot a budget by its own size. During a Redis
outage budgets are not enforced; the metric makes that visible.
