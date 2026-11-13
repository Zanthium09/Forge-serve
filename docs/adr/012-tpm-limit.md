# ADR-012: Tokens-per-minute limit: allow while positive, debit after

Status: accepted (production gateway, 2026-11-12)

## Context
Counting requests ignores that one request can be 10 tokens or 10 000. GPU
cost is in tokens, and a request's size is only known when it ends.

## Decision
A second per-user bucket with capacity and per-minute refill equal to
`tokens_per_minute`. A request is admitted only while the bucket is above
zero; when it ends, prompt + completion tokens are debited and the bucket
may go negative, blocking the user until refill catches up. Same Lua
atomicity and Redis-down local fallback as ADR-006. Spec §7.2.

## Rationale
A large request is never cut off mid-way, and the long-run rate still holds.
Reserving `max_tokens` up front would reject users who are below their limit.

## Consequences
A burst can exceed the per-minute figure once; the following requests pay
for it.
