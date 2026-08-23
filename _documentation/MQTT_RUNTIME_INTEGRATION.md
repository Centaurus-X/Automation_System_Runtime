# MQTT runtime integration

## Scope

The MQTT interface is an optional external transport boundary for one runtime
node. It publishes sensor/config/runtime events and accepts authenticated
configuration commands. It does not write the config heap itself: every write
uses the existing State Event Management (SEM) single-writer capability and is
acknowledged only after atomic commit and synchronous Thread Management rollout.

```text
Controller Worker
  -> central sensor value store
  -> SENSOR_VALUE_UPDATE
  -> Event Router -> Thread Management -> MQTT outbox -> TLS broker

TLS broker -> MQTT command queue -> MQTT command dispatcher
  -> SEM single writer -> Data Center copy-on-write commit
  -> affected local controller rollout -> MQTT ACK
```

MQTT is enabled in the distributed `worker_simulation` and `worker_fn02`
deployment profiles and disabled in the explicit `standalone`, `local_dev` and
`unit_test` profiles.
The standalone local Sensor -> RuleEngine/FSM -> Automation -> Actuator path is
independent of the Control Plane.

## Topic layout

For prefix `automation/v1`, site `site-main` and node `fn-01`:

```text
automation/v1/site-main/nodes/fn-01/status               retained
automation/v1/site-main/nodes/fn-01/heartbeat
automation/v1/site-main/nodes/fn-01/registration         retained
automation/v1/site-main/nodes/fn-01/config/state         retained metadata
automation/v1/site-main/events/<event_type>/fn-01
automation/v1/site-main/nodes/fn-01/commands/#            inbound
automation/v1/site-main/broadcast/commands/#              inbound
automation/v1/site-main/nodes/fn-01/acks/<message_id>
```

Sensor and config-change events use QoS 1 by default. Status uses a retained
online message and a retained Last Will (`offline`). Retained commands are
rejected to prevent an old write from being replayed after reconnect.

## Command envelope

```json
{
  "protocol": "automation.mqtt.v1",
  "kind": "command",
  "message_id": "globally-unique-id",
  "site_id": "site-main",
  "source_node": "operator-console",
  "target_node": "fn-01",
  "created_at_s": 1783716000.0,
  "expires_at_s": 1783716030.0,
  "principal": "operator-a",
  "command": "config.patch",
  "payload": {
    "path": "process_states.0.parameters.target_temp_c",
    "value": 555.0,
    "mode": "set",
    "base_revision": 42
  }
}
```

Supported handlers:

- `config.get`: full detached config snapshot.
- `config.shard.get`: one controller UUID shard.
- `config.patch` / `config.update`: partial mutation.
- `config.append` / `config.delete`: available but disabled in the default ACL.
- `config.replace`: full verified replacement, disabled in the default ACL.
- `config.sync`: force a rollout of the current revision.
- `runtime.status`: lightweight runtime health response.

`append`, `delete`, arbitrary paths and full replacement require explicit
enablement in both `mqtt_interface.commands.allowed` and the SEM external safety
gate. All resulting device configs still pass the Generic Type Library schema
verification before the heap is swapped.

## Security controls

- TLS 1.2 or newer is the default and unencrypted transport requires an explicit
  unsafe override.
- Server certificate verification and hostname verification are enabled.
- Private CA and mutual-TLS files are configured under `_config/certs`.
- The distributed profile requires a private CA, a client certificate, a
  matching private key, mode `0600` on that key and the exact Worker URI SAN.
- Username/password and optional HMAC secrets are read from environment
  variables; production secrets do not belong in YAML.
- Broker ACLs should grant each node read access only to its own command topic
  and write access only to its own status/event/ACK topics.
- Optional HMAC-SHA256 protects the complete canonical JSON command envelope.
- Target site/node, command allowlist, expiry, maximum age, clock skew,
  `base_revision`, message deduplication and maximum packet size are validated.
- An ACK is emitted only after commit and rollout. Repeated `message_id` values
  return the cached ACK without a second mutation.

Command publishers should retry the same `message_id` when the application ACK
does not arrive before their timeout. MQTT QoS 1 protects broker delivery;
`base_revision` plus the node's dedupe cache makes an application retry safe.

## Optional safety gates

`state_event_management.safety.enabled` is intentionally `false` until plant
limits and interlocks are commissioned. When enabled, the nested gates provide:

- dynamic RuleEngine `writable_fields` enforcement;
- `required_mode_id` enforcement;
- parameter ranges and allowed values;
- declarative config/state/live-sensor interlocks;
- action-plan revision checks and deduplication;
- external operation, patch-mode, path-prefix and principal policies;
- actuator value limits, live interlocks and config-revision checks.

Each plan is checked completely before mutation. One unsafe action rejects the
whole plan, so no partial config change can occur.

## Lifecycle

Startup order:

```text
Data Center -> Event Router -> SEM -> prepare MQTT capabilities
-> Thread Management -> initial rollout -> start MQTT -> value persistence
```

Shutdown first rejects new MQTT commands and drains an in-flight command. It
then stops controller/connection owners and the pool, flushes/stops MQTT, stops
SEM and persistence, and finally stops the Event Router.

`run_worker.sh` performs a side-effect-free preflight before this lifecycle
begins. An invalid identity fails immediately with a concise remediation. If
the identity is valid but the broker is temporarily unavailable, the Worker
starts local control in a visible degraded transport state and reconnects.

## Authority boundary

The Worker remains fieldbus owner and local automation authority. Global
configuration, registry and external command routing belong to the Master.
Worker config proposals are journaled in local SQLite and converge only through
MQTT application ACK plus authoritative replication. The Worker never accesses
PostgreSQL.
