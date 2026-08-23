# Performance and capacity model

## What the release can claim

The architecture removes master/MQTT round trips from the node-local control
critical path, isolates controller connection owners, supports batched I/O and
uses configurable queues, event-router workers and execution pools. The release
also includes CPU/memory probes, internal metrics and a free-threading
microbenchmark.

The release does **not** contain a reproducible, hardware-independent end-to-end
latency, jitter or throughput benchmark. Consequently, no universal numerical
performance claim is made.

## Critical paths

### Local sensor-to-actuator path

```text
field read
-> controller acquisition
-> value-store commit
-> event routing
-> rule/FSM evaluation
-> automation plan
-> safety authorization
-> field write
-> optional readback
```

This is the path to measure for control latency. Network transport to the
master is intentionally not part of it.

### Configuration path

```text
proposal/file diff
-> schema and policy checks
-> durable journal/outbox write
-> atomic heap activation
-> affected-controller rollout
-> asynchronous mirror convergence
```

Durability adds filesystem and SQLite latency by design. It should be measured
separately from control-cycle latency.

### Cross-node path

```text
remote sensor acquisition
-> MQTT delivery
-> fact validation/cache
-> cross evaluation
-> routed actuator intent
-> owner-side safety/write/readback
```

This path depends on broker, network, clock quality and two nodes. It is suited
to coordination on a seconds-scale, not hard real-time safety control.

## Tuning controls

| Area | Representative controls | Trade-off |
|---|---|---|
| Controller polling | `poll_interval_s`, batch timeout | Freshness versus fieldbus/controller load. |
| Event routing | worker count, priority mode, batch maximum | Parallelism and burst handling versus scheduling overhead. |
| Thread pool | min/max workers, task timeout | Concurrency versus CPU and memory contention. |
| Queues | queue size and ingress timeout | Burst absorption versus memory and backpressure behavior. |
| Persistence | runtime value persistence and journal policy | Recovery evidence versus storage latency/wear. |
| MQTT | QoS, reconnect timing and packet limits | Delivery assurance versus transport overhead. |
| Metrics | CPU/memory probe and tracing | Observability versus measurement overhead. |

The shipped queue maximum is `0` (unbounded) for several paths. This prevents
premature message loss but means capacity planning must include worst-case
producer/consumer imbalance. A production deployment should commission bounded
queues only together with explicit overload and drop/retry semantics.

## Target-system benchmark plan

Record the following for each selected hardware/profile combination:

1. Interpreter build, GIL state, CPU topology, memory and kernel.
2. Protocol backend and exact dependency versions.
3. Controller/simulator model, network path and register/tag batch size.
4. Profile, config fingerprint and enabled persistence/metrics options.
5. Warm-up period and sample count.
6. Median, p95, p99 and maximum latency for each critical path.
7. Jitter, missed deadlines, queue high-water marks and error/retry counts.
8. CPU, RSS, disk I/O and network utilization.
9. Broker outage, reconnect, controller timeout and shutdown behavior.

Suggested acceptance tables:

| Metric | Idle | Nominal load | Peak load | Outage/recovery | Target |
|---|---:|---:|---:|---:|---:|
| Sensor-to-actuator p99 | TBD | TBD | TBD | TBD | project-specific |
| Controller cycle deadline misses | TBD | TBD | TBD | TBD | project-specific |
| Event queue high-water mark | TBD | TBD | TBD | TBD | project-specific |
| Config commit p99 | TBD | TBD | TBD | TBD | project-specific |
| Mirror convergence p99 | TBD | TBD | TBD | TBD | project-specific |
| RSS | TBD | TBD | TBD | TBD | project-specific |

Do not compare free-threaded and conventional Python runs unless they use the
same workload, dependency set, CPU affinity, warm-up and measurement method.
`_tools/gil-free_benchmark.py` is a CPU microbenchmark only; it does not model
fieldbus timing or end-to-end runtime behavior.
