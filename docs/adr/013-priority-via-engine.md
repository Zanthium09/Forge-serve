# ADR-013: Priority is decided by the gateway and enforced by the engine

Status: accepted (production gateway, 2026-11-12)

## Context
On shared GPUs, interactive people should go ahead of bulk scripts. The
engine can reorder its queue (vLLM `--scheduling-policy priority`) but does
not know who is asking; the gateway knows who, but does not own the queue.

## Decision
Each user and team has an optional integer priority (config default
otherwise). The gateway always strips a client-supplied `priority` and sets
the user's effective value only for backends marked
`supports_priority: true`. Other engines and our own worker ignore it.
Spec §7.5.

## Rationale
Neither side can do this alone, and delegating avoids building a second
scheduler in the gateway. A client must never choose its own priority.

## Consequences
Priority affects only the engine's queue, not gateway admission or routing.
The meaning of the number follows the engine, verified against real vLLM.
