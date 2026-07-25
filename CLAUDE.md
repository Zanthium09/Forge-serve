# CLAUDE.md

Read `FORGE_SERVE_BLUEPRINT.md` fully before making any changes — it is the master spec (architecture, fixed tech stack, build phases, exit criteria, and session rules in its §11).

Current phase: **Phase 3 — Multi-worker + failover** (Phase 2 completed 2026-07-25; exit criteria verified: 16.1x throughput at concurrency 16 vs sequential — decomposed in `benchmarks/report.md`, batching's own ceiling on this CPU is 2.5x; TTFT p99 reported at concurrency 10/16/50; cancellation evicts from a live batch, proven by unit test and observed live).

Known follow-ups carried out of Phase 2 (see `benchmarks/report.md`):
- p99 TTFT at concurrency 50 misses the blueprint's <5s target; lever is `MAX_BATCH` (decode scaling shows batch 16 beats batch 8).
- Headline is a single run; blueprint asks for a 3-run median.
- Batched engine is ~30% slower than sequential at concurrency 1 (Python per-token overhead).

Rules that override defaults:
- Work phase by phase; never start a later phase while current exit criteria are unmet.
- State the current phase and task at the start of each session.
- Every non-obvious design choice gets a *why* comment or an ADR in `docs/adr/`.
- Benchmark numbers in README must come from committed CSVs in `benchmarks/` — never hand-typed.
