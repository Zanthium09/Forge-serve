# CLAUDE.md

Read `FORGE_SERVE_BLUEPRINT.md` fully before making any changes — it is the master spec (architecture, fixed tech stack, build phases, exit criteria, and session rules in its §11).

Current phase: **Phase 0 — Skeleton** (not started).

Rules that override defaults:
- Work phase by phase; never start a later phase while current exit criteria are unmet.
- State the current phase and task at the start of each session.
- Every non-obvious design choice gets a *why* comment or an ADR in `docs/adr/`.
- Benchmark numbers in README must come from committed CSVs in `benchmarks/` — never hand-typed.
