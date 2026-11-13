# ADR-011: One YAML config file; secrets in the environment

Status: accepted (production gateway, 2026-11-12)

## Context
Models, backends, admission limits, fallback and priority support no longer
fit in a couple of environment variables.

## Decision
`forge.yaml` (path from `FORGE_CONFIG`) describes listen address, auth cache
TTL, body size limit, user defaults, and models with their backends.
Secrets (database URL, Redis URL, TLS key, backend keys) come only from
environment variables. `WORKER_URLS` keeps working as a shorthand for one
`forge` model. Parsed with `gopkg.in/yaml.v3`. Spec §6.5.

## Rationale
Operators expect YAML, at the cost of one small dependency. Keeping secrets
out of the file lets the file be committed and reviewed.

## Consequences
Config changes need a restart (no hot reload).
