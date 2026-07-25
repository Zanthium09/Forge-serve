# Forge-Serve

Self-hosted, distributed LLM inference gateway for open-weight models — an
OpenAI-compatible streaming API with continuous batching, routing, rate
limiting, failover, and observability. Built from scratch; see
[FORGE_SERVE_BLUEPRINT.md](FORGE_SERVE_BLUEPRINT.md).

**Status: Phase 0 — skeleton.**

## Quickstart

```bash
docker compose up --build
```

Then in another terminal:

```bash
curl.exe -N -X POST http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -d "{\"stream\":true}"
```

You should see fake tokens stream at 20 tok/s (real model arrives in Phase 1).
