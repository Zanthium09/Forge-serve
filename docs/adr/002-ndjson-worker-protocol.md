# ADR-002: NDJSON over chunked HTTP for gateway ↔ worker streaming

Status: accepted (Phase 1, 2026-07-25)

## Context
The worker streams tokens to the gateway. Candidates: SSE, NDJSON over
chunked HTTP/1.1, gRPC server streaming.

## Decision
NDJSON (one JSON object per line) over HTTP/1.1 chunked transfer.

## Rationale
- Internal hop between two services we own: no browsers, no reconnection
  semantics, no proxies — SSE's framing (`data:` prefixes, event ids) is
  dead weight here.
- NDJSON is one json.dumps per line in Python and one json.Unmarshal per
  line in Go; trivially debuggable with curl.
- gRPC adds codegen, TLS/ALPN considerations and a heavier debug loop for
  no measurable win at Phase 1 scale. Revisit only with a measured need
  (per blueprint §5).

## Consequences
One stream per request (no multiplexing) — fine, the gateway opens one
worker request per client request. Final NDJSON line carries usage counts.
