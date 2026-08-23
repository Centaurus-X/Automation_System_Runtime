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

## Component and event-flow map

The public overview contains two native Mermaid views that remain collapsed by
default: the [internal component source](diagrams/runtime-internal-components.mmd)
and the [external communication source](diagrams/runtime-external-communications.mmd).
They were reconstructed against the runtime entry points rather than inferred
from product terminology alone.

| Runtime concern | Implemented responsibility | Primary source anchors |
|---|---|---|
| Bootstrap and lifecycle | Builds shared queues and the value store, then starts the Data Center, Event Router, SEM, MQTT, Distributed Coordinator, file sync, Thread Management and Cross Worker in dependency order. | `src/system/bootstrap_runtime_interface.py`, `src/system/bootstrap_system_interface.py` |
| Event ingress and routing | Drains `queue_event_send` in bounded batches, orders events in the heap-based Priority Event Broker, performs deterministic primary dispatch and only then executes broadcast mirrors. | `src/core/_event_router.py`, `src/library/_event_interface.py` |
| Local orchestration | Owns the management, controller-ingress and pool-result threads; coalesces pending sensor work by sensor identity and rejects or retries stale results against the active config revision. | `src/core/thread_management.py` |
| Parallel execution | Executes prioritized Rule Engine, Automation, FSM and Cross jobs with configurable worker limits, queue and execution timeouts, cancellation and controlled shutdown. Results return through one dedicated result queue. | `src/library/_generic_thread_pool.py` |
| Rule, FSM and automation state | Evaluates detached facts, serializes FSM work per process instance and preserves per-sensor Automation state behind explicit locks. Rule action plans return to SEM; actuator tasks enter the actuator safety boundary. | `src/library/_generic_rule_engine.py`, `src/library/_generic_finite_state_machine.py`, `src/library/_automation_engine.py` |
| Controller ownership | Gives each controller an isolated worker queue while a connection-owner thread serializes one protocol connection. Sensor commits precede `SENSOR_VALUE_UPDATE`; actuator success requires write plus readback evidence. | `src/core/_worker_thread.py`, `src/core/_controller_thread.py`, `src/adapter/_connection_thread.py` |
| Adapter boundary | Resolves protocol factories for Modbus TCP, S7, OPC UA, EtherNet/IP, KNX/IP, PROFINET RT and PROFIBUS gateways without moving ownership into the adapter. | `src/adapter/_connection_registry.py`, `src/adapter/fieldbus/`, `src/adapter/plc/` |
| Configuration authority | Converts local file changes or authenticated proposals to canonical scope operations, applies schema and safety policy, commits through the SEM single writer and synchronously rolls the accepted revision into runtime owners. | `src/core/config_file_sync.py`, `src/core/state_event_management.py`, `src/library/_generic_data_center.py` |
| Cross execution | Keeps one single-writer Cross thread and fact cache; pool jobs receive copies and return results by queue. Only UUID actuator intent leaves the Cross runtime. | `src/core/_cross_worker_thread.py`, `src/adapter/_cross_io_adapter.py` |
| External convergence | Validates MQTT envelopes, identity, age, expiry, HMAC and deduplication; persists authority, proposal, mirror and actuator-operation state before retryable asynchronous exchange. | `src/interfaces/mqtt_interface.py`, `src/core/distributed_coordinator.py`, `src/library/_distributed_operation_store.py` |

The diagrams distinguish two safety-relevant write paths that are easy to
conflate. SEM is the single writer for configuration and Rule Engine action
plans. Hardware tasks are instead canonicalized at the owner Worker and pass
ownership, revision, fencing, allowlist, limit and interlock checks before they
enter the controller-owned queue. Neither path gives the Master, MQTT broker,
Cross Worker or protocol adapter independent field-write authority.

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
