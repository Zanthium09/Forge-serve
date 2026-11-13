# CLAUDE.md

Read `FORGE_SERVE_BLUEPRINT.md` fully before making any changes — it is the master spec (architecture, fixed tech stack, build phases, exit criteria, and session rules in its §11).

Current phase: **Production gateway, Steps 2–3** — plan `docs/superpowers/plans/2026-11-12-production-gateway-steps-2-3.md`, spec `docs/superpowers/specs/2026-11-10-production-gateway-design.md` (blueprint §14, ADR-007..015). MVP (Phases 0-4) completed 2026-07-26; all six §3 success criteria verified — see `README.md` and `benchmarks/`.

Known follow-ups carried out of Phase 2 (see `benchmarks/report.md`):
- p99 TTFT misses the <5s SLO at concurrency 50 (27.5s). Established by measurement to be a **capacity** limit, not a tuning gap: p99 there is bounded by service time, and shrinking `MAX_QUEUE` 8->2 barely moved it. Phase 3's multiple workers are the designed fix; revisit this number once routing lands.
- Headline is a single run; blueprint asks for a 3-run median.
- Batched engine is ~30% slower than sequential at concurrency 1 (Python per-token overhead).

Rules that override defaults:
- Work phase by phase; never start a later phase while current exit criteria are unmet.
- State the current phase and task at the start of each session.
- Every non-obvious design choice gets a *why* comment or an ADR in `docs/adr/`.
- Benchmark numbers in README must come from committed CSVs in `benchmarks/` — never hand-typed.
