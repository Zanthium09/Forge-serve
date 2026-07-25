# ADR-001: SSE for the client edge

Status: accepted (Phase 1, 2026-07-25)

## Context
Clients need live token streams. Candidates: SSE, WebSockets, gRPC streaming.

## Decision
Server-Sent Events on /v1/chat/completions.

## Rationale
- OpenAI compatibility is a hard requirement, and OpenAI streams SSE — any
  deviation breaks unmodified SDKs.
- Traffic is one-directional after the request (server → client). WebSockets'
  bidirectionality buys nothing and costs an upgrade handshake and stateful
  connection handling.
- SSE is plain HTTP: proxies, load balancers, curl all work untouched.
- gRPC streaming would force every browser/script client through a proxy.

## Consequences
Client → server mid-stream signals are limited to connection close (which we
use as cancellation). Acceptable: it is the OpenAI semantic too.
