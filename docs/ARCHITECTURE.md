# Architecture

## System intent

Automation System Runtime is a worker-side industrial control runtime. Its
central invariant is that local field control remains local: a master, broker
or cross-node coordinator may distribute intent and collect evidence, but it
does not become the synchronous decision point for a node-owned control loop.

The implementation is functional and context-oriented. State and capabilities
are passed explicitly through dictionaries and queues; the project deliberately
does not use classes, decorators, lambdas or type annotations.

## Responsibility boundaries

| Boundary | Owns | May execute automation | May write hardware |
|---|---|---:|---:|
| Master/control plane | Registry, relay, mirror and audit | No | No |
| Local worker | Node shard, local rules, FSM and safety | Yes | Its local controllers only |
| Cross worker | One isolated cross shard and fact cache | Yes | Never directly |
| Controller owner | One controller connection and I/O lifecycle | No independent rules | Yes, after ownership and safety checks |
| SEM | Authorization and atomic configuration mutation | No | Authorizes actuator/config plans |

## Local control path

```text
Field sensor
  -> protocol adapter / connection owner
  -> controller thread
  -> central runtime value store
  -> priority event router
  -> local rule engine and finite-state machine
  -> automation engine
  -> SEM safety policy
  -> controller-owned actuator queue
  -> fieldbus write
  -> readback confirmation
```

The MQTT transport is not a synchronization point in this path. A transport
outage is represented as a degraded external state while local ownership and
automation remain available.

## Configuration and authority

Active node configuration uses a flat, entity-keyed V9 storage contract.
Cross configuration uses a separate V8 contract. V5 is isolated below
`_config/config_data/_legacy/` and accepted only through an explicit migration
path.

All accepted mutations converge on one node-local commit boundary:

```text
External proposal -> MQTT/relay -> canonical scope operations --+
                                                               |
Local JSON edit -> file WAL -> stable selector diff ------------+
                                                               v
                                  SEM authorization + safety policy
                                                               |
                                                               v
                       durable journal + scope head + mirror outbox
                                                               |
                                                               v
                             atomic runtime activation and rollout
```

The normative persisted identity is:

```text
(authority_epoch, scope_revision, execution_epoch, fingerprint)
```

The epoch/revision values are authority; fingerprint equality alone never
authorizes a rollback, owner change or stale replay.

### Commit semantics

- A proposal binds the expected authority epoch, base revision and base
  fingerprint.
- Safety and schema validation run before activation.
- Journal, scope head, snapshot and mirror-outbox records are persisted before
  the new runtime state becomes visible.
- A repeated message ID uses the stored result and does not commit twice.
- The worker publishes its new head asynchronously; an exact ACK permits
  outbox compaction.
- A master mirror is never silently materialized as the local source of truth.

## Distributed runtime

`src/core/distributed_coordinator.py` implements proposal, registry, mirror and
cross-node protocols. MQTT messages use a versioned envelope, topic scoping,
expiry, size limits, deduplication and optional HMAC protection. Distributed
state is persisted in SQLite through the operation and authority stores.

The control plane can authenticate, validate and relay operations. It does not
allocate a worker's next node revision and it does not commit the worker's
hardware-owned shard.

## Cross-node execution

The cross runtime consumes validated local and remote facts, evaluates isolated
cross rules and produces UUID-only actuator intent. It has no connection
adapter and stores no bus address, unit ID or host data.

```text
remote/local facts
  -> ownership and freshness validation
  -> cross fact cache
  -> cross rule/FSM/automation
  -> UUID-only actuator intent
  -> owner worker
  -> canonical local address resolution
  -> safety + fencing
  -> hardware write + readback
```

Freshness, future-clock skew, quality, sequence and ownership are validated
fail closed. Cross operation is intended for coordination on a seconds-scale;
hard real-time or safety-critical loops remain local or hardwired.

## Concurrency model

- One connection owner serializes access to each protocol connection.
- Controller workers isolate local controller state and queues.
- The event router uses priority-aware queues and configurable batching.
- The generic thread pool supports priorities, timeouts, cancellation and
  controlled shutdown.
- Mutable shared records are protected by explicit locks.
- Runtime sensor values use a centralized store with writer-token ownership.
- Cross state is single-writer within its cross worker.

The design targets free-threaded CPython 3.14t for parallel CPU work while
remaining regression-testable on CPython 3.12. Free-threading improves the
opportunity for parallel execution; it does not itself establish deterministic
latency.

## Persistence model

| Data | Mechanism | Purpose |
|---|---|---|
| Authority heads and transition state | SQLite | Reject stale ownership and recover controlled transfer/restore. |
| Distributed operation journal | SQLite | Preserve proposal, ACK and uncertain-operation state. |
| Mirror outbox | SQLite | Retry worker-to-master convergence after reconnect/restart. |
| Runtime sensor values | JSON-backed asynchronous store | Preserve the latest local values and monotonic sequences. |
| File changes | File WAL and receipts | Bind baseline, diff, commit, journal and mirror identity. |
| Node/cross configuration | Canonical JSON | Human-reviewable, fingerprinted contract input. |

## Security boundaries

- TLS 1.2+ is the default MQTT transport requirement.
- Distributed profiles require a private CA, client certificate, private key
  mode `0600` and an exact worker URI SAN.
- Credentials and HMAC secrets are resolved from environment variables.
- Retained commands are rejected.
- Site, node, command, principal, age, expiry, base revision and packet size
  are validated.
- External commits and actuator tasks have separate allowlists and safety
  policies.
- Cross intent cannot carry raw bus addressing.

These mechanisms reduce risk but do not make the runtime a certified safety
system or a complete OT-security control by themselves.

## Lifecycle

Startup is staged so that the data center, event router and SEM exist before
controller and MQTT work begins. The worker preflight checks cache policy,
profile invariants, required dependencies and distributed identity before the
runtime starts.

Shutdown first rejects or drains new external commands, then stops controller
and cross owners, flushes persistence and MQTT, stops SEM, and finally stops
the event router. Shutdown failures are isolated so that later cleanup stages
still run.

## Extension points

- Adapter factories under `src/adapter/`.
- Protocol and persistence contracts under
  `src/library/_generic_system_contract_library/`.
- Event routes and queue policies under `src/core/_event_router.py` and YAML.
- Safety policy under `src/library/_config_safety_policy.py` and profile data.
- Runtime profile overlays in `_config/application/app_config.yaml`.

Any extension must preserve hardware ownership, the SEM single-writer rule,
canonical storage contracts, monotonic fencing and explicit failure behavior.
