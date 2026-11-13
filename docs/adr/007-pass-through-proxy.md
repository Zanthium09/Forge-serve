# ADR-007: Forward the client's body instead of rebuilding requests

Status: accepted (production gateway, 2026-11-12)

## Context
The MVP gateway decoded each request into a small struct and built a new
body for the worker. Every OpenAI field it did not model (`top_p`, `stop`,
`tools`, `response_format`, `seed`, ...) was silently dropped. In front of
vLLM, that breaks clients that worked when calling vLLM directly.

## Decision
The gateway forwards the original JSON body. It parses only `model`,
`stream`, `max_tokens` and `stream_options`, and makes only these edits:
rewrite `model` to the pool's upstream name; set
`stream_options.include_usage` when streaming (hiding the resulting usage
chunk if the client did not ask for it); remove any client-supplied
`priority`, replacing it with the user's effective priority for backends
that support it (ADR-013). Spec §6.2.

## Rationale
- Drop-in compatibility: anything that worked against the engine works
  through the gateway, including fields invented after this code was written.
- Field semantics belong to the engine; the gateway validates only what it
  needs (JSON object, model, the endpoint's required field, size limit).

## Consequences
JSON key order may change (values do not). Engine errors reach the client
unchanged as backend 4xx responses, which are never retried.
