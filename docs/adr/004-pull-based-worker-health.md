# ADR-004: Pull-based worker health instead of push heartbeats

Status: accepted (Phase 3, 2026-07-25)

## Context

The blueprint (§8 Phase 3) specifies "workers register on boot, heartbeat
every 2s, marked unhealthy after 3 misses" — a push model. Deviating from
the blueprint requires an ADR (§5), hence this record.

## Decision

The gateway is configured with a worker list (`WORKER_URLS`) and **polls**
each worker's `/stats` every 2s. Three consecutive failed polls mark a
worker unhealthy; one successful poll restores it. There is no registration
endpoint and no heartbeat from workers.

## Rationale

- **One signal, not two.** The gateway already polls `/stats` for queue
  depth and batch occupancy, which is exactly the data
  least-outstanding-tokens routing needs. Pull makes liveness a free
  by-product of a call we already make; push would add a second mechanism
  that can disagree with the first.
- **No inbound surface on the gateway.** A registration endpoint accepts
  writes from the network and needs authentication of its own, or any
  process that can reach the gateway can inject a fake worker into the
  routing table.
- **Workers stay ignorant of the control plane.** A pushed heartbeat means
  every worker must know the gateway's address and retry policy. Pulling
  keeps workers as dumb inference servers, which is also what makes them
  trivially replaceable in Phase 7.
- **The failure semantics are identical.** Three misses at a 2s interval
  ejects a dead worker within ~6s either way, and recovery is symmetric.

## Consequences

- **Workers must be known in advance.** Autoscaling a worker fleet requires
  service discovery (DNS round-robin, a k3s Service, or Consul) rather than
  self-registration. That is the correct place for this concern and is a
  Phase 7 (k3s) problem; at compose scale a static list is honest.
- Health granularity is bounded by the poll interval; a worker that dies
  immediately after a successful poll is discovered up to 6s later. Actual
  in-flight requests do not wait for that: they fail fast on the broken
  connection and trigger failover (ADR-005), so the poll interval only
  affects routing of *new* requests.
