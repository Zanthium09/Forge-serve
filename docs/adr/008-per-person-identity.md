# ADR-008: One identity per person; limits keyed by user, not key

Status: accepted (production gateway, 2026-11-12)

## Context
Inference engines have no per-person keys. Usage must be tracked and access
decided per person. A person may need several keys (laptop, server, CI).

## Decision
Keys belong to users; users optionally belong to teams. The request-rate
bucket, TPM bucket and budgets are keyed by **user id**, so all of a
person's keys share one allowance. Spec §7.1-§7.2.

## Rationale
- Keying by key would let anyone multiply their quota by minting keys.
- Usage reports answer "who", not "which key"; the key id is still recorded.

## Consequences
Revoking one key does not reset the person's allowance. Disabling the user
blocks all their keys at once.
