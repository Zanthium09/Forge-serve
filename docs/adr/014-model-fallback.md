# ADR-014: Model fallback at admission time, one hop, with opt-out

Status: accepted (production gateway, 2026-11-12)

## Context
With two GPU servers, some models run on only one. When that server is full
or down, some callers prefer a smaller model's answer to an error.

## Decision
A model may name a `fallback`. It is used only at admission, when the
requested model's pool is full or has no healthy backend, never after a
request has started. One hop only. The user must be allowed to use the
fallback model. Header `X-Forge-No-Fallback: 1` opts out. The gateway sets
`X-Forge-Model-Fallback` and records usage against the served model, with
the requested model alongside. Spec §6.7.

## Rationale
Admission time is the only point where switching models cannot interfere
with the retry rule (ADR-005). One hop prevents chains and loops.

## Consequences
A fallback model may lack features the request uses; it then fails at the
engine like any unsupported request.
