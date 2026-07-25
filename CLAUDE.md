# CLAUDE.md

Read `FORGE_SERVE_BLUEPRINT.md` fully before making any changes — it is the master spec (architecture, fixed tech stack, build phases, exit criteria, and session rules in its §11).

Current phase: **Phase 2 — Continuous batching** (Phase 1 completed 2026-07-25; exit criteria verified: real Qwen tokens streamed end-to-end via compose with auth, OpenAI Python SDK worked unmodified in stream + non-stream modes, 10 concurrent requests all completed without deadlock).

Rules that override defaults:
- Work phase by phase; never start a later phase while current exit criteria are unmet.
- State the current phase and task at the start of each session.
- Every non-obvious design choice gets a *why* comment or an ADR in `docs/adr/`.
- Benchmark numbers in README must come from committed CSVs in `benchmarks/` — never hand-typed.
