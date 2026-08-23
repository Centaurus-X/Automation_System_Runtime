# -*- coding: utf-8 -*-

# src/core/_cross_worker_thread.py


"""
=============================================================================
ISOLATED CROSS WORKER
=============================================================================

The cross worker owns the logical, non-fieldbus execution domain of one
Worker node.  It has its own ingress queue, RuleEngine/FSM snapshots,
automation state and independently versioned config cell.  Short business
jobs use the existing central GenericThreadPool; completed jobs are returned
to this component through ``dispatch_cross_pool_result``.

The component never opens a hardware adapter.  Sensor facts are accepted by
``_cross_io_adapter`` and actuator intents are routed either to the normal
local controller dispatcher or to the distributed actuator relay.

The implementation deliberately follows the project's functional style:
plain dictionaries, explicit locks, deterministic copies and no shared
mutable state with the legacy local pipeline.
=============================================================================
"""


import copy
import json
import logging
import threading
import time

from functools import partial
from queue import Empty, Full, Queue

from src.adapter import _cross_io_adapter as cross_io
from src.core._automation_thread import run_automation_cycle
from src.library import _generic_rule_engine as rule_engine
from src.library import _generic_type_library as type_library
from src.library._generic_system_contract_library.config_sharding_contracts import (
    classify_config_scopes,
    validate_cross_execution_shard,
)
from src.library._cross_snapshot_store import (
    create_cross_snapshot_capabilities,
    create_cross_snapshot_store,
)
from src.library._generic_finite_state_machine import (
    fsm_machines_from_config,
    fsm_restore,
    fsm_send,
    fsm_snapshot,
)
from src.library._state_shard_manager import build_automation_projection


LOGGER = logging.getLogger(__name__)

CONTEXT = {
    "library_name": "cross_worker_thread",
    "library_version": "2.0.0",
    "options": {
        "enabled": False,
        "node_id": "",
        "domain_id": "cross-default",
        "queue_maxsize": 1024,
        "ingress_timeout_s": 0.25,
        "join_timeout_s": 5.0,
        "apply_timeout_s": 5.0,
        "job_timeout_s": 30.0,
        "job_queue_timeout_s": 10.0,
        "pipeline_priority": 150.0,
        "fsm_priority": 250.0,
        "max_signal_age_s": 15.0,
        "snapshot_path": "",
    },
}


def _is_dict(value):
    return isinstance(value, dict)


def _safe_bool(value, default):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(default)


def _safe_int(value, default, minimum=0):
    try:
        result = int(value)
    except Exception:
        return int(default)
    return result if result >= minimum else int(default)


def _safe_float(value, default, minimum=0.0):
    try:
        result = float(value)
    except Exception:
        return float(default)
    return result if result >= minimum else float(default)


def _thread_alive(thread):
    try:
        return bool(thread and thread.is_alive())
    except Exception:
        return False


def _increment_stat(context, key, amount=1):
    with context["lock"]:
        context["statistics"][key] += int(amount)


def _resolved_options(options):
    supplied = copy.deepcopy(options) if _is_dict(options) else {}
    result = dict(CONTEXT["options"])
    result.update(supplied)
    result["enabled"] = _safe_bool(result.get("enabled"), False)
    result["node_id"] = str(result.get("node_id") or "").strip()
    result["domain_id"] = str(
        result.get("domain_id") or "cross-default").strip()
    result["queue_maxsize"] = _safe_int(
        result.get("queue_maxsize"), 1024, 1)
    for key in (
        "ingress_timeout_s",
        "join_timeout_s",
        "apply_timeout_s",
        "job_timeout_s",
        "job_queue_timeout_s",
        "pipeline_priority",
        "fsm_priority",
    ):
        result[key] = _safe_float(
            result.get(key), CONTEXT["options"][key], 0.01)
    result["max_signal_age_s"] = _safe_float(
        result.get("max_signal_age_s"), 15.0, 0.01)
    result["snapshot_path"] = str(
        result.get("snapshot_path") or "").strip()
    if result["enabled"] and not result["node_id"]:
        raise ValueError("cross worker requires node_id")
    return result


def _config_fingerprint(config):
    return str(type_library.config_fingerprint(config) or "")


def _meta_row(config):
    rows = config.get("meta") if _is_dict(config) else None
    if isinstance(rows, list) and rows and _is_dict(rows[0]):
        return rows[0]
    return {}


def _validate_cross_package(config, options):
    report = validate_cross_execution_shard(config, options["node_id"])
    if not report.get("ok"):
        return {
            "ok": False,
            "code": "cross_shard",
            "error": "; ".join(report.get("errors") or ()),
        }
    meta = _meta_row(config)
    domain_id = str(
        meta.get("domain_id") or meta.get("cross_domain_id") or "").strip()
    if domain_id and domain_id != options["domain_id"]:
        return {
            "ok": False,
            "code": "domain",
            "error": "cross shard domain differs from Cross Worker domain",
        }
    return {"ok": True}


def _create_snapshot_api(options):
    path = options.get("snapshot_path")
    if not path:
        return None
    store = create_cross_snapshot_store(path)
    return create_cross_snapshot_capabilities(store)


def _select_initial_package(options, supplied, snapshot_api):
    package = copy.deepcopy(supplied) if _is_dict(supplied) else {}
    recovered = None
    if snapshot_api is None:
        return package, recovered
    loaded = snapshot_api["load"]()
    if not loaded.get("ok"):
        raise RuntimeError(loaded.get("error") or "Cross snapshot is invalid")
    record = loaded.get("record")
    if not _is_dict(record):
        return package, recovered
    saved_revision = _safe_int(record.get("revision"), -1, -1)
    supplied_revision = _safe_int(package.get("revision"), -1, -1)
    saved_epoch = _safe_int(record.get("execution_epoch"), -1, -1)
    supplied_epoch = _safe_int(package.get("execution_epoch"), 0, 0)
    saved_fingerprint = str(record.get("fingerprint") or "")
    supplied_fingerprint = str(package.get("fingerprint") or "")
    if (saved_epoch, saved_revision) < (supplied_epoch, supplied_revision):
        return package, recovered
    if (
        (saved_epoch, saved_revision) == (supplied_epoch, supplied_revision)
        and supplied_fingerprint
        and saved_fingerprint != supplied_fingerprint
    ):
        raise RuntimeError(
            "Cross snapshot conflicts with configured initial authority head")
    package = {
        "config": copy.deepcopy(record.get("authority_config") or {}),
        "revision": saved_revision,
        "execution_epoch": saved_epoch,
        "execution_active": bool(record.get("execution_active", True)),
        "fingerprint": saved_fingerprint,
    }
    recovered = copy.deepcopy(record)
    return package, recovered


def _controller_owners(config, supplied=None):
    owners = copy.deepcopy(supplied) if _is_dict(supplied) else {}
    for row in config.get("site_controller_map") or []:
        if not _is_dict(row) or row.get("enabled") is False:
            continue
        if str(row.get("routing_role") or "owner") != "owner":
            continue
        identity = str(row.get("controller_uuid") or "").strip()
        node_id = str(row.get("node_id") or "").strip()
        if identity and node_id:
            owners[identity] = node_id
    return owners


def _controller_ids(config):
    result = []
    for row in config.get("controllers") or []:
        if not _is_dict(row):
            continue
        try:
            result.append(int(row.get("controller_id")))
        except Exception:
            continue
    return tuple(sorted(set(result)))


def _required_sensor_keys(config):
    result = set()

    def _walk(node):
        if isinstance(node, list):
            for item in node:
                _walk(item)
            return
        if not _is_dict(node):
            return
        for gate in ("all", "any", "none", "xor"):
            if gate in node:
                _walk(node.get(gate))
                return
        if "not" in node:
            _walk(node.get("not"))
            return
        if str(node.get("src") or "").lower() != "sensor":
            return
        identity = str(node.get("controller_uuid") or "").strip()
        sensor_id = node.get("sensor_id")
        if identity and sensor_id is not None:
            result.add((identity, str(sensor_id)))

    for rule in config.get("rules") or []:
        if not _is_dict(rule) or rule.get("enabled") is False:
            continue
        _walk(rule.get("when"))
        _walk(rule.get("until"))
    for state in config.get("process_states") or []:
        if not _is_dict(state):
            continue
        refs = state.get("io_refs") if _is_dict(state.get("io_refs")) else {}
        for ref in refs.get("sensors") or []:
            if not _is_dict(ref):
                continue
            identity = str(ref.get("controller_uuid") or "").strip()
            sensor_id = ref.get("sensor_id")
            if identity and sensor_id is not None:
                result.add((identity, str(sensor_id)))
    return tuple(sorted(result))


def _actuator_controller_uuids(config):
    result = set()
    for state in config.get("process_states") or []:
        if not _is_dict(state):
            continue
        refs = state.get("io_refs") if _is_dict(state.get("io_refs")) else {}
        for ref in refs.get("actuators") or []:
            if _is_dict(ref) and ref.get("controller_uuid"):
                result.add(str(ref.get("controller_uuid")))
    for group in config.get("actuators") or []:
        if _is_dict(group) and group.get("controller_uuid"):
            result.add(str(group.get("controller_uuid")))
    return tuple(sorted(result))


def _state_map(config):
    result = {}
    for row in config.get("process_states") or []:
        if _is_dict(row) and row.get("state_id") is not None:
            result[row.get("state_id")] = copy.deepcopy(row)
    return result


def _states_to_rows(config, states):
    rows = []
    seen = set()
    for source in config.get("process_states") or []:
        if not _is_dict(source):
            continue
        state_id = source.get("state_id")
        value = states.get(state_id, states.get(str(state_id), source))
        rows.append(copy.deepcopy(value if _is_dict(value) else source))
        seen.add(str(state_id))
    for state_id in sorted(states, key=str):
        if str(state_id) not in seen and _is_dict(states[state_id]):
            rows.append(copy.deepcopy(states[state_id]))
    return rows


def _create_rule_engine(config, runtime=None, max_signal_age_s=None):
    engine = rule_engine.create_engine_context()
    dataset = rule_engine.load_dataset(config)
    binding = rule_engine.engine_bind_dataset(engine, dataset)
    if _is_dict(runtime):
        rule_engine.engine_import_runtime(engine, runtime)
    if max_signal_age_s is not None:
        with engine["lock"]:
            engine["config"]["max_signal_age_s"] = float(max_signal_age_s)
    return engine, binding


def _create_fsm(config):
    try:
        return {"ok": True, "machines": fsm_machines_from_config(config),
                "error": None}
    except Exception as exc:
        LOGGER.error("Cross FSM projection failed: %s", exc)
        return {"ok": False, "machines": {}, "error": str(exc)}


def _restore_fsm(config, runtime):
    created = _create_fsm(config)
    snapshots = runtime if _is_dict(runtime) else {}
    for name in sorted(created.get("machines") or {}):
        snapshot = snapshots.get(name)
        if not _is_dict(snapshot):
            continue
        try:
            created["machines"][name] = fsm_restore(
                created["machines"][name]["spec"], snapshot)
        except Exception:
            LOGGER.exception("Cross FSM restore failed: %s", name)
            created["ok"] = False
            created["error"] = "FSM restore failed for " + str(name)
    return created


def _fsm_runtime(context):
    return {
        name: fsm_snapshot(machine)
        for name, machine in sorted(
            (context["fsm"].get("machines") or {}).items())
    }


def _fact_entries(facts):
    result = []
    sensors = facts.get("sensors") if _is_dict(facts) else {}
    for controller_uuid in sorted(sensors or {}):
        rows = sensors.get(controller_uuid)
        if not _is_dict(rows):
            continue
        for sensor_id in sorted(rows, key=str):
            entry = rows.get(sensor_id)
            if _is_dict(entry) and "value" in entry:
                result.append((str(controller_uuid), sensor_id, entry))
    return result


def _deterministic_task_id(domain_id, revision, execution_epoch, trigger, task, sequence):
    material = {
        "domain_id": domain_id,
        "revision": revision,
        "execution_epoch": execution_epoch,
        "trigger": copy.deepcopy(trigger.get("event_key") or {}),
        "controller_uuid": task.get("controller_uuid"),
        "actuator_id": task.get("actuator_id"),
        "control_value": task.get("control_value"),
        "sequence": sequence,
    }
    digest = type_library.fingerprint(material)
    return "cross-{}-{}".format(domain_id, digest[:32])


def _prepare_task(task, options, revision, fingerprint, execution_epoch, activation_proof, trigger, sequence):
    prepared = copy.deepcopy(task)
    metadata = prepared.get("meta") if _is_dict(prepared.get("meta")) else {}
    metadata = copy.deepcopy(metadata)
    metadata.update({
        "execution_scope": "cross",
        "cross_domain_id": options["domain_id"],
        "cross_owner_node_id": options["node_id"],
        "cross_revision": revision,
        "cross_fingerprint": fingerprint,
        "execution_epoch": execution_epoch,
    })
    if _is_dict(activation_proof):
        metadata["registry_transition"] = copy.deepcopy(activation_proof)
    prepared["meta"] = metadata
    prepared["execution_epoch"] = execution_epoch
    prepared["source"] = "cross_automation:{}".format(options["domain_id"])
    prepared["task_uuid"] = _deterministic_task_id(
        options["domain_id"], revision, execution_epoch, trigger, prepared, sequence)
    # A Cross task is an intent.  Hardware fields supplied by a projection are
    # explicitly removed; the owning Worker resolves them from its local shard.
    for key in ("address", "bus_type", "function_type", "unit", "controller_id"):
        prepared.pop(key, None)
    return prepared


def _task_sort_key(item):
    return (
        str(item.get("controller_uuid") or ""),
        str(item.get("actuator_id") or ""),
        str(item.get("control_value") or ""),
    )


def _task_target_key(item):
    return (
        str(item.get("controller_uuid") or ""),
        str(item.get("actuator_id") or ""),
    )


def _task_value_key(item):
    value = item.get("control_value")
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )


def _deduplicate_tasks(tasks):
    selected = {}
    conflicts = set()
    for task in sorted(tasks, key=_task_sort_key):
        target = _task_target_key(task)
        previous = selected.get(target)
        if previous is None:
            selected[target] = copy.deepcopy(task)
            continue
        if _task_value_key(previous) != _task_value_key(task):
            conflicts.add(target)
    for target in conflicts:
        selected.pop(target, None)
    ordered = [selected[target] for target in sorted(selected)]
    return ordered, tuple(sorted(conflicts))


def _run_cross_pipeline_job(config, engine_runtime, persistent_state, facts,
                            options, revision, fingerprint, execution_epoch,
                            activation_proof, trigger):
    working = copy.deepcopy(config)
    engine, binding = _create_rule_engine(
        working, runtime=engine_runtime,
        max_signal_age_s=options.get("max_signal_age_s"),
    )
    rule_facts = copy.deepcopy(facts)
    rule_facts["states"] = _state_map(working)
    rule_facts["now"] = float(trigger.get("now") or time.time())
    decision = rule_engine.evaluate_rules(
        engine, rule_facts, now=rule_facts["now"])
    target = {"states": _state_map(working)}
    rule_engine.execute_actions(engine, decision, target=target)
    working["process_states"] = _states_to_rows(working, target["states"])

    controller_ids = _controller_ids(working)
    slots = copy.deepcopy(persistent_state or {})
    tasks = []
    for controller_uuid, sensor_id, entry in _fact_entries(facts):
        projection = build_automation_projection(
            working, controller_uuid, sensor_id)
        if not projection:
            continue
        identity = "{}:{}".format(controller_uuid, sensor_id)
        thread_info = {
            "job_id": "cross:{}:{}".format(
                options.get("domain_id"), identity),
            "sensor_id": projection["sensor_id"],
            "persistent_state": copy.deepcopy(slots.get(identity) or {}),
            "node_id": options.get("node_id"),
            "allowed_controller_ids": controller_ids,
            "controller_scope_explicit": True,
        }
        sensor_data = {
            "controller_uuid": controller_uuid,
            "sensor_id": projection["sensor_id"],
            "value": copy.deepcopy(entry.get("value")),
            "timestamp": entry.get("ts"),
            "quality": entry.get("quality"),
            "sequence": entry.get("sequence"),
        }
        produced = run_automation_cycle(
            projection["config"], sensor_data, thread_info)
        slots[identity] = copy.deepcopy(
            thread_info.get("persistent_state") or {})
        for task in produced if isinstance(produced, list) else []:
            if _is_dict(task):
                tasks.append(task)

    tasks, conflicts = _deduplicate_tasks(tasks)
    prepared_tasks = []
    for sequence, task in enumerate(tasks, 1):
        prepared_tasks.append(_prepare_task(
            task, options, revision, fingerprint, execution_epoch,
            activation_proof, trigger, sequence))
    return {
        "revision": revision,
        "fingerprint": fingerprint,
        "execution_epoch": execution_epoch,
        "config": working,
        "engine_runtime": rule_engine.engine_export_runtime(engine),
        "persistent_state": slots,
        "decision": decision,
        "binding": binding,
        "tasks": prepared_tasks,
        "task_conflicts": conflicts,
    }


def _statistics():
    return {
        "events_received": 0,
        "facts_accepted": 0,
        "facts_rejected": 0,
        "facts_coalesced": 0,
        "jobs_submitted": 0,
        "jobs_completed": 0,
        "jobs_failed": 0,
        "stale_results": 0,
        "tasks_local": 0,
        "tasks_remote": 0,
        "tasks_rejected": 0,
        "config_applied": 0,
        "config_rejected": 0,
        "execution_epoch_rejected": 0,
        "execution_freezes": 0,
        "fsm_events": 0,
        "fsm_jobs": 0,
        "fsm_updates": 0,
        "fsm_errors": 0,
        "state_publications": 0,
        "snapshot_errors": 0,
    }


def _adapter_options(options, config, owners, local_controllers):
    required = [
        {"controller_uuid": controller_uuid, "sensor_id": sensor_id}
        for controller_uuid, sensor_id in _required_sensor_keys(config)
    ]
    return {
        "node_id": options["node_id"],
        "domain_id": options["domain_id"],
        "max_signal_age_s": options["max_signal_age_s"],
        "controller_owners": owners,
        "required_sensor_keys": required,
        "target_controller_uuids": _actuator_controller_uuids(config),
        "local_controller_uuids": tuple(sorted(local_controllers)),
    }


def _make_adapter(options, config, owners, local_controllers, **kwargs):
    adapter_options = _adapter_options(
        options, config, owners, local_controllers)
    return cross_io.create_cross_io_adapter(
        options=adapter_options,
        ownership=owners,
        required_facts=adapter_options["required_sensor_keys"],
        local_controller_uuids=tuple(sorted(local_controllers)),
        local_dispatcher=kwargs.get("local_dispatcher"),
        remote_dispatcher=kwargs.get("remote_dispatcher"),
    )


def _snapshot_record(context):
    with context["lock"]:
        return {
            "revision": context["revision"],
            "fingerprint": context["fingerprint"],
            "execution_epoch": context["execution_epoch"],
            "execution_active": context["execution_active"],
            "authority_config": copy.deepcopy(context["authority_config"]),
            "runtime_config": copy.deepcopy(context["config"]),
            "engine_runtime": rule_engine.engine_export_runtime(
                context["rule_engine"]),
            "persistent_state": copy.deepcopy(context["persistent_state"]),
            "fact_cache": context["adapter"]["export_fact_cache"](),
            "fsm_runtime": _fsm_runtime(context),
            "pipeline_sequence": context["pipeline_sequence"],
            "freeze_intent": copy.deepcopy(context.get("freeze_intent")),
            "activation_proof": copy.deepcopy(
                context.get("activation_proof")),
        }


def _persist_snapshot(context):
    snapshot_api = context.get("snapshot_store")
    if snapshot_api is None:
        return {"ok": True, "status": "disabled"}
    report = snapshot_api["persist"](_snapshot_record(context))
    if not report.get("ok"):
        _increment_stat(context, "snapshot_errors")
        context["last_error"] = copy.deepcopy(report)
    return report


def create_cross_worker(**kwargs):
    options = _resolved_options(kwargs.get("options"))
    supplied = kwargs.get("package") if _is_dict(kwargs.get("package")) else {}
    if not supplied and _is_dict(kwargs.get("cross_config")):
        supplied = {"config": copy.deepcopy(kwargs.get("cross_config"))}
    snapshot_api = _create_snapshot_api(options)
    package, recovered = _select_initial_package(
        options, supplied, snapshot_api)
    config = copy.deepcopy(kwargs.get("cross_config") or {})
    if _is_dict(package.get("config")):
        config = copy.deepcopy(package["config"])
    if _is_dict(config) and config:
        representation = (
            "runtime"
            if type_library.get_config_io_layout(config) == "grouped"
            else "storage"
        )
        config = type_library.normalize_cross_v8_for_runtime(
            config, representation=representation)
    revision = _safe_int(package.get("revision"), 0, 0)
    fingerprint = str(package.get("fingerprint") or _config_fingerprint(config))
    execution_epoch = _safe_int(package.get("execution_epoch"), 0, 0)
    execution_active = bool(package.get("execution_active", True))
    if fingerprint != _config_fingerprint(config):
        raise ValueError("cross shard fingerprint differs")
    validation = _validate_cross_package(config, options)
    if options["enabled"] and not validation.get("ok"):
        raise ValueError(validation.get("error") or "cross shard is invalid")
    meta = _meta_row(config)
    configured_owner = str(
        meta.get("owner_node_id") or meta.get("node_id") or "").strip()
    if options["enabled"] and configured_owner not in ("", options["node_id"]):
        raise ValueError("cross shard owner differs from Worker node")
    owners = _controller_owners(config, kwargs.get("controller_owners"))
    local_controllers = {
        identity for identity, owner in owners.items()
        if owner == options["node_id"]
    }
    pool = kwargs.get("pool") or {}
    if options["enabled"] and not callable(pool.get("submit_job")):
        raise ValueError("enabled cross worker requires central thread pool")
    runtime_config = (
        copy.deepcopy(recovered.get("runtime_config"))
        if _is_dict(recovered) and _is_dict(recovered.get("runtime_config"))
        else copy.deepcopy(config)
    )
    adapter = _make_adapter(
        options, runtime_config, owners, local_controllers,
        local_dispatcher=kwargs.get("local_dispatcher"),
        remote_dispatcher=kwargs.get("remote_dispatcher"),
    )
    if _is_dict(recovered):
        adapter["import_fact_cache"](recovered.get("fact_cache") or [])
    engine, binding = _create_rule_engine(
        runtime_config,
        runtime=(recovered or {}).get("engine_runtime"),
        max_signal_age_s=options["max_signal_age_s"],
    )
    context = {
        "module": CONTEXT["library_name"],
        "version": CONTEXT["library_version"],
        "state": "created" if options["enabled"] else "disabled",
        "options": options,
        "pool": pool,
        "queue": Queue(maxsize=options["queue_maxsize"]),
        "stop_event": threading.Event(),
        "thread": None,
        "lock": threading.RLock(),
        "config": runtime_config,
        "authority_config": copy.deepcopy(config),
        "revision": revision,
        "fingerprint": fingerprint,
        "execution_epoch": execution_epoch,
        "execution_active": execution_active,
        "freeze_intent": copy.deepcopy((recovered or {}).get("freeze_intent")),
        "activation_proof": copy.deepcopy(
            (recovered or {}).get("activation_proof")
            or package.get("activation_proof")),
        "owners": owners,
        "local_controller_uuids": tuple(sorted(local_controllers)),
        "adapter": adapter,
        "rule_engine": engine,
        "rule_binding": binding,
        "fsm": _restore_fsm(
            runtime_config, (recovered or {}).get("fsm_runtime")),
        "fsm_active": set(),
        "fsm_pending": {},
        "persistent_state": copy.deepcopy(
            (recovered or {}).get("persistent_state") or {}),
        "pipeline_active": False,
        "pipeline_sequence": _safe_int(
            (recovered or {}).get("pipeline_sequence"), 0, 0),
        "pending": {},
        "snapshot_store": snapshot_api,
        "state_publisher": kwargs.get("state_publisher"),
        "statistics": _statistics(),
        "last_error": None,
        "last_decision": None,
    }
    persisted = _persist_snapshot(context)
    if options["enabled"] and not persisted.get("ok"):
        raise RuntimeError(
            persisted.get("error") or "Cross snapshot persistence failed")
    return context


def _queue_item(context, item):
    if context["stop_event"].is_set():
        return False
    waiter = item.get("waiter") if _is_dict(item) else None
    prepared = dict(item) if _is_dict(item) else {}
    prepared.pop("waiter", None)
    prepared = copy.deepcopy(prepared)
    if waiter is not None:
        prepared["waiter"] = waiter
    try:
        context["queue"].put_nowait(prepared)
        return True
    except Full:
        _increment_stat(context, "facts_rejected")
        return False


def dispatch_cross_event(context, event, **kwargs):
    if not _is_dict(context) or not _is_dict(event):
        return False
    return _queue_item(context, {
        "kind": "sensor_event",
        "event": event,
        "source_node": kwargs.get("source_node"),
        "received_at_s": kwargs.get("received_at_s", time.time()),
    })


def dispatch_cross_pool_result(context, result):
    if not _is_dict(context) or not _is_dict(result):
        return False
    metadata = result.get("meta") if _is_dict(result.get("meta")) else {}
    if metadata.get("kind") not in ("cross_pipeline", "cross_fsm"):
        return False
    if str(metadata.get("domain_id") or "") != context["options"]["domain_id"]:
        return False
    return _queue_item(context, {"kind": "pool_result", "result": result})


def _event_payload(event):
    if not _is_dict(event):
        return {}
    payload = event.get("payload")
    if _is_dict(payload) and _is_dict(payload.get("payload")):
        payload = payload.get("payload")
    return payload if _is_dict(payload) else event


def _event_identity(event):
    payload = _event_payload(event)
    return "{}:{}".format(
        str(payload.get("controller_uuid") or ""),
        str(payload.get("sensor_id")),
    )


def _event_key(event):
    payload = _event_payload(event)
    sequence = payload.get("sequence")
    if sequence is None:
        sequence = payload.get("sensor_store_sequence")
    return {
        "controller_uuid": str(payload.get("controller_uuid") or ""),
        "sensor_id": str(payload.get("sensor_id")),
        "sequence": copy.deepcopy(sequence),
        "timestamp": copy.deepcopy(payload.get("timestamp")),
        "value": copy.deepcopy(payload.get("value")),
    }


def _schedule_pipeline(context, trigger):
    with context["lock"]:
        if not context["execution_active"]:
            return False
        if context["pipeline_active"]:
            identity = _event_identity(trigger.get("event") or trigger)
            replaced = identity in context["pending"]
            context["pending"][identity] = copy.deepcopy(trigger)
            if replaced:
                _increment_stat(context, "facts_coalesced")
            return True
        context["pipeline_active"] = True
        context["pipeline_sequence"] += 1
        sequence = context["pipeline_sequence"]
        config = copy.deepcopy(context["config"])
        revision = context["revision"]
        fingerprint = context["fingerprint"]
        execution_epoch = context["execution_epoch"]
        activation_proof = copy.deepcopy(
            context.get("activation_proof"))
        engine_runtime = rule_engine.engine_export_runtime(
            context["rule_engine"])
        persistent = copy.deepcopy(context["persistent_state"])
    facts = context["adapter"]["get_fact_snapshot"]()
    trigger_meta = {
        "identity": _event_identity(trigger.get("event") or trigger),
        "sequence": sequence,
        "now": trigger.get("received_at_s", time.time()),
        "event_key": _event_key(trigger.get("event") or trigger),
    }
    try:
        job_id = context["pool"]["submit_job"]({
            "fn": _run_cross_pipeline_job,
            "priority": context["options"]["pipeline_priority"],
            "timeout_s": context["options"]["job_timeout_s"],
            "queue_timeout_s": context["options"]["job_queue_timeout_s"],
            "args": (
                config, engine_runtime, persistent, facts,
                copy.deepcopy(context["options"]), revision, fingerprint,
                execution_epoch,
                activation_proof,
                trigger_meta,
            ),
            "meta": {
                "kind": "cross_pipeline",
                "domain_id": context["options"]["domain_id"],
                "revision": revision,
                "fingerprint": fingerprint,
                "execution_epoch": execution_epoch,
                "pipeline_sequence": sequence,
            },
        })
    except Exception as exc:
        job_id = ""
        with context["lock"]:
            context["last_error"] = {
                "code": "pool_submit",
                "error": str(exc),
            }
        LOGGER.exception("Cross pipeline submission failed")
    if job_id:
        _increment_stat(context, "jobs_submitted")
        return True
    with context["lock"]:
        context["pipeline_active"] = False
    _increment_stat(context, "jobs_failed")
    return False


def _schedule_pending(context):
    with context["lock"]:
        if context["pipeline_active"] or not context["pending"]:
            return False
        identity = sorted(context["pending"])[0]
        trigger = context["pending"].pop(identity)
    return _schedule_pipeline(context, trigger)


def _handle_sensor_event(context, item):
    with context["lock"]:
        if not context["execution_active"]:
            _increment_stat(context, "facts_rejected")
            return False
    _increment_stat(context, "events_received")
    event = item.get("event") if _is_dict(item.get("event")) else {}
    report = context["adapter"]["ingest_sensor_event"](
        event,
        source_node_id=item.get("source_node"),
        now_s=item.get("received_at_s"),
    )
    accepted = bool(report.get("ok")) if _is_dict(report) else bool(report)
    if not accepted:
        _increment_stat(context, "facts_rejected")
        return False
    _increment_stat(context, "facts_accepted")
    return _schedule_pipeline(context, item)


def _publish_state(context, value):
    publisher = context.get("state_publisher")
    if not callable(publisher):
        return False
    payload = {
        "event_type": "CROSS_STATE_UPDATE",
        "payload": {
            "owner_node_id": context["options"]["node_id"],
            "domain_id": context["options"]["domain_id"],
            "cross_revision": context["revision"],
            "cross_fingerprint": context["fingerprint"],
            "execution_epoch": context["execution_epoch"],
            "process_states": copy.deepcopy(
                value.get("config", {}).get("process_states") or []),
        },
        "source": "cross_worker",
        "target": "external",
    }
    try:
        accepted = bool(publisher(payload))
    except Exception:
        LOGGER.exception("Cross state publication failed")
        accepted = False
    if accepted:
        _increment_stat(context, "state_publications")
    return accepted


def _handle_pool_result(context, item):
    result = item.get("result") if _is_dict(item.get("result")) else {}
    metadata = result.get("meta") if _is_dict(result.get("meta")) else {}
    if metadata.get("kind") == "cross_fsm":
        return _handle_cross_fsm_result(context, result)
    with context["lock"]:
        current_revision = context["revision"]
        current_fingerprint = context["fingerprint"]
        current_execution_epoch = context["execution_epoch"]
        execution_active = context["execution_active"]
    if (
        metadata.get("revision") != current_revision
        or str(metadata.get("fingerprint") or "") != current_fingerprint
        or int(metadata.get("execution_epoch", -1)) != current_execution_epoch
        or not execution_active
    ):
        _increment_stat(context, "stale_results")
        with context["lock"]:
            context["pipeline_active"] = False
        _schedule_pending(context)
        return False
    if not result.get("ok") or not _is_dict(result.get("result")):
        _increment_stat(context, "jobs_failed")
        context["last_error"] = {
            "error": result.get("error"),
            "traceback": result.get("traceback"),
        }
        with context["lock"]:
            context["pipeline_active"] = False
        _schedule_pending(context)
        return False
    value = result["result"]
    with context["lock"]:
        context["config"] = copy.deepcopy(value.get("config") or context["config"])
        context["persistent_state"] = copy.deepcopy(
            value.get("persistent_state") or {})
        engine, binding = _create_rule_engine(
            context["config"], runtime=value.get("engine_runtime"),
            max_signal_age_s=context["options"]["max_signal_age_s"],
        )
        context["rule_engine"] = engine
        context["rule_binding"] = binding
        context["last_decision"] = copy.deepcopy(value.get("decision"))
        context["pipeline_active"] = False
    conflicts = value.get("task_conflicts") or ()
    _increment_stat(context, "tasks_rejected", len(conflicts))
    for task in value.get("tasks") or []:
        report = context["adapter"]["route_actuator_task"](task)
        if _is_dict(report) and report.get("ok"):
            route = str(report.get("route") or "")
            key = "tasks_local" if route == "local" else "tasks_remote"
            _increment_stat(context, key)
        elif report is True:
            _increment_stat(context, "tasks_local")
        else:
            _increment_stat(context, "tasks_rejected")
    _increment_stat(context, "jobs_completed")
    _persist_snapshot(context)
    _publish_state(context, value)
    _schedule_pending(context)
    return True


def _classification_fingerprint(config):
    report = classify_config_scopes(config)
    return type_library.fingerprint(report)


def _execution_error(context, accepted):
    _increment_stat(context, "execution_epoch_rejected")
    return {
        "ok": False,
        "code": "execution_epoch_stale",
        "error": "cross execution epoch differs",
        "accepted_execution_epoch": int(accepted),
    }


def _activation_proof_valid(context, package, current_epoch, received_epoch):
    proof = package.get("activation_proof")
    if not _is_dict(proof) or received_epoch != current_epoch + 1:
        return False
    return (
        proof.get("old_owner_frozen_ack") is True
        and proof.get("registry_persisted") is True
        and str(proof.get("new_owner_node_id") or "") == context["options"]["node_id"]
        and int(proof.get("execution_epoch", -1)) == received_epoch
        and bool(str(proof.get("registry_intent_id") or ""))
    )


def _apply_package_now(context, package):
    config = package.get("config") if _is_dict(package.get("config")) else None
    if config is None:
        return {"ok": False, "code": "config", "error": "missing cross config"}
    revision = _safe_int(package.get("revision"), -1, -1)
    execution_epoch = _safe_int(package.get("execution_epoch"), -1, -1)
    fingerprint = str(package.get("fingerprint") or "")
    actual = _config_fingerprint(config)
    if not fingerprint or fingerprint != actual:
        return {"ok": False, "code": "fingerprint",
                "error": "cross config fingerprint differs"}
    supplied_classification = str(
        package.get("classification_fingerprint") or "")
    local_classification = _classification_fingerprint(config)
    if not supplied_classification or supplied_classification != local_classification:
        return {
            "ok": False,
            "code": "classification_mismatch",
            "error": "owner classification differs from sender",
            "classification_fingerprint": local_classification,
        }
    validation = _validate_cross_package(config, context["options"])
    if not validation.get("ok"):
        return validation
    owner = str(
        _meta_row(config).get("owner_node_id")
        or _meta_row(config).get("node_id") or "").strip()
    if owner and owner != context["options"]["node_id"]:
        return {"ok": False, "code": "owner",
                "error": "cross shard owner differs from Worker node"}
    with context["lock"]:
        current_epoch = context["execution_epoch"]
        epoch_advance = execution_epoch > current_epoch
        if execution_epoch < current_epoch:
            return _execution_error(context, current_epoch)
        if epoch_advance and not _activation_proof_valid(
            context, package, current_epoch, execution_epoch
        ):
            return _execution_error(context, current_epoch)
        if not context["execution_active"] and not epoch_advance:
            return _execution_error(context, current_epoch + 1)
        accepted_activation_proof = (
            copy.deepcopy(package.get("activation_proof"))
            if epoch_advance
            else copy.deepcopy(context.get("activation_proof"))
        )
        if revision < context["revision"]:
            return {"ok": False, "code": "stale_revision",
                    "revision": context["revision"]}
        if revision == context["revision"] and fingerprint != context["fingerprint"]:
            return {"ok": False, "code": "revision_conflict",
                    "error": "same cross revision has another fingerprint"}
        already_current = (
            revision == context["revision"]
            and fingerprint == context["fingerprint"]
            and not epoch_advance
        )
        if already_current:
            return {
                "ok": True,
                "status": "already_current",
                "revision": revision,
                "execution_epoch": execution_epoch,
                "fingerprint": fingerprint,
            }
        owners = _controller_owners(config, package.get("controller_owners"))
        local_controllers = {
            identity for identity, node_id in owners.items()
            if node_id == context["options"]["node_id"]
        }
        engine, binding = _create_rule_engine(
            config, max_signal_age_s=context["options"]["max_signal_age_s"])
        snapshot_api = context.get("snapshot_store")
        if snapshot_api is not None:
            persisted = snapshot_api["persist"]({
                "revision": revision,
                "execution_epoch": execution_epoch,
                "execution_active": True,
                "fingerprint": fingerprint,
                "authority_config": copy.deepcopy(config),
                "runtime_config": copy.deepcopy(config),
                "engine_runtime": rule_engine.engine_export_runtime(engine),
                "persistent_state": {},
                "fact_cache": [],
                "fsm_runtime": {},
                "pipeline_sequence": context["pipeline_sequence"],
                "freeze_intent": None,
                "activation_proof": copy.deepcopy(
                    accepted_activation_proof),
            })
            if not persisted.get("ok"):
                _increment_stat(context, "snapshot_errors")
                return persisted
        context["config"] = copy.deepcopy(config)
        context["authority_config"] = copy.deepcopy(config)
        context["revision"] = revision
        context["execution_epoch"] = execution_epoch
        context["execution_active"] = True
        context["freeze_intent"] = None
        context["activation_proof"] = copy.deepcopy(
            accepted_activation_proof)
        context["fingerprint"] = fingerprint
        context["owners"] = owners
        context["local_controller_uuids"] = tuple(sorted(local_controllers))
        context["rule_engine"] = engine
        context["rule_binding"] = binding
        context["fsm"] = _create_fsm(config)
        context["fsm_active"].clear()
        context["fsm_pending"].clear()
        context["persistent_state"] = {}
        context["pending"].clear()
        context["adapter"] = _make_adapter(
            context["options"], config, owners, local_controllers,
            local_dispatcher=context["adapter"]["context"].get(
                "local_dispatcher"),
            remote_dispatcher=context["adapter"]["context"].get(
                "remote_dispatcher"),
        )
    _increment_stat(context, "config_applied")
    return {
        "ok": True,
        "status": "owner_activated" if epoch_advance else "applied",
        "revision": revision,
        "execution_epoch": execution_epoch,
        "classification_fingerprint": local_classification,
        "fingerprint": fingerprint,
    }


def freeze_cross_execution(context, **kwargs):
    """Persistently freeze the old owner before Registry advances the epoch."""
    expected = _safe_int(kwargs.get("expected_execution_epoch"), -1, -1)
    target = _safe_int(kwargs.get("target_execution_epoch"), -1, -1)
    freeze_token = str(kwargs.get("freeze_token") or "")
    registry_intent_id = str(kwargs.get("registry_intent_id") or "")
    if not freeze_token or not registry_intent_id:
        return {"ok": False, "code": "invalid_payload",
                "error": "freeze token and Registry intent are required"}
    with context["lock"]:
        current = context["execution_epoch"]
        if expected != current or target != current + 1:
            return _execution_error(context, current)
        existing = context.get("freeze_intent") or {}
        if not context["execution_active"]:
            if existing.get("freeze_token") != freeze_token:
                return {"ok": False, "code": "scope_locked",
                        "error": "another Cross freeze is active"}
        context["execution_active"] = False
        context["pending"].clear()
        context["fsm_pending"].clear()
        context["freeze_intent"] = {
            "freeze_token": freeze_token,
            "registry_intent_id": registry_intent_id,
            "expected_execution_epoch": expected,
            "target_execution_epoch": target,
        }
        persisted = _persist_snapshot(context)
        if not persisted.get("ok"):
            context["execution_active"] = True
            context["freeze_intent"] = None
            return persisted
        _increment_stat(context, "execution_freezes")
        return {
            "ok": True,
            "status": "frozen",
            "old_owner_frozen_ack": True,
            "owner_node_id": context["options"]["node_id"],
            "execution_epoch": current,
            "target_execution_epoch": target,
            "fingerprint": context["fingerprint"],
            "freeze_token": freeze_token,
            "registry_intent_id": registry_intent_id,
        }


def apply_cross_config(context, package, **kwargs):
    if not _is_dict(context) or not _is_dict(package):
        return {"ok": False, "code": "arguments", "error": "invalid arguments"}
    if not _thread_alive(context.get("thread")):
        result = _apply_package_now(context, copy.deepcopy(package))
        if not result.get("ok"):
            _increment_stat(context, "config_rejected")
        return result
    waiter = {"event": threading.Event(), "result": None}
    written = _queue_item(context, {
        "kind": "config_apply", "package": package, "waiter": waiter})
    if not written:
        return {"ok": False, "code": "queue_full", "error": "cross queue full"}
    timeout = _safe_float(
        kwargs.get("timeout_s"), context["options"]["apply_timeout_s"], 0.01)
    if not waiter["event"].wait(timeout):
        return {"ok": False, "code": "timeout", "error": "cross apply timed out"}
    return copy.deepcopy(waiter.get("result") or {
        "ok": False, "code": "result", "error": "missing apply result"})


def _handle_config_apply(context, item):
    waiter = item.get("waiter") if _is_dict(item.get("waiter")) else None
    result = _apply_package_now(context, item.get("package") or {})
    if not result.get("ok"):
        _increment_stat(context, "config_rejected")
    if waiter is not None:
        waiter["result"] = copy.deepcopy(result)
        waiter["event"].set()
    return result


def _run_cross_fsm_job(machine, item):
    return fsm_send(
        machine,
        item.get("event_name"),
        copy.deepcopy(item.get("data") or {}),
        priority=int(item.get("priority", 0)),
    )


def _submit_cross_fsm_job(context, item):
    name = str(item.get("process_name") or "")
    with context["lock"]:
        if not context["execution_active"]:
            return False
        machine = copy.deepcopy(
            (context["fsm"].get("machines") or {}).get(name))
        revision = context["revision"]
        fingerprint = context["fingerprint"]
        execution_epoch = context["execution_epoch"]
    if not _is_dict(machine):
        return False
    try:
        job_id = context["pool"]["submit_job"]({
            "fn": _run_cross_fsm_job,
            "priority": (
                context["options"]["fsm_priority"]
                + _safe_int(item.get("priority"), 0, 0)
            ),
            "timeout_s": context["options"]["job_timeout_s"],
            "queue_timeout_s": context["options"]["job_queue_timeout_s"],
            "args": (machine, copy.deepcopy(item)),
            "meta": {
                "kind": "cross_fsm",
                "domain_id": context["options"]["domain_id"],
                "process_name": name,
                "revision": revision,
                "execution_epoch": execution_epoch,
                "fingerprint": fingerprint,
            },
        })
    except Exception:
        LOGGER.exception("Cross FSM submission failed")
        job_id = ""
    if job_id:
        _increment_stat(context, "fsm_jobs")
        return True
    return False


def _schedule_next_cross_fsm(context, name):
    with context["lock"]:
        pending = context["fsm_pending"].get(name) or []
        if name in context["fsm_active"] or not pending:
            return False
        item = pending.pop(0)
        if not pending:
            context["fsm_pending"].pop(name, None)
        context["fsm_active"].add(name)
    if _submit_cross_fsm_job(context, item):
        return True
    with context["lock"]:
        context["fsm_active"].discard(name)
    _increment_stat(context, "fsm_errors")
    return False


def _finish_cross_fsm(context, name):
    with context["lock"]:
        context["fsm_active"].discard(name)
    _schedule_next_cross_fsm(context, name)


def _publish_fsm_state(context, name, machine):
    publisher = context.get("state_publisher")
    if not callable(publisher):
        return False
    event = {
        "event_type": "CROSS_FSM_STATE_UPDATE",
        "payload": {
            "owner_node_id": context["options"]["node_id"],
            "domain_id": context["options"]["domain_id"],
            "cross_revision": context["revision"],
            "execution_epoch": context["execution_epoch"],
            "process_name": name,
            "snapshot": fsm_snapshot(machine),
        },
        "source": "cross_worker",
        "target": "external",
    }
    try:
        return bool(publisher(event))
    except Exception:
        LOGGER.exception("Cross FSM state publication failed")
        return False


def _handle_cross_fsm_result(context, result):
    metadata = result.get("meta") if _is_dict(result.get("meta")) else {}
    name = str(metadata.get("process_name") or "")
    with context["lock"]:
        current_revision = context["revision"]
        current_fingerprint = context["fingerprint"]
        current_execution_epoch = context["execution_epoch"]
        execution_active = context["execution_active"]
    if (
        metadata.get("revision") != current_revision
        or str(metadata.get("fingerprint") or "") != current_fingerprint
        or int(metadata.get("execution_epoch", -1)) != current_execution_epoch
        or not execution_active
        or not result.get("ok")
        or not _is_dict(result.get("result"))
    ):
        _increment_stat(context, "fsm_errors")
        _finish_cross_fsm(context, name)
        return False
    machine = copy.deepcopy(result["result"])
    with context["lock"]:
        if name not in (context["fsm"].get("machines") or {}):
            _increment_stat(context, "fsm_errors")
            context["fsm_active"].discard(name)
            return False
        context["fsm"]["machines"][name] = machine
    _increment_stat(context, "fsm_updates")
    _persist_snapshot(context)
    _publish_fsm_state(context, name, machine)
    _finish_cross_fsm(context, name)
    return True


def _handle_fsm_event(context, item):
    payload = item.get("payload") if _is_dict(item.get("payload")) else {}
    name = str(payload.get("process_name") or "")
    event_name = str(payload.get("event_name") or "")
    with context["lock"]:
        machine = (context["fsm"].get("machines") or {}).get(name)
    if not _is_dict(machine) or not event_name:
        _increment_stat(context, "fsm_errors")
        return False
    prepared = {
        "process_name": name,
        "event_name": event_name,
        "data": copy.deepcopy(payload.get("data") or {}),
        "priority": int(payload.get("priority", 0)),
    }
    with context["lock"]:
        context["fsm_pending"].setdefault(name, []).append(prepared)
    _increment_stat(context, "fsm_events")
    return _schedule_next_cross_fsm(context, name)


def dispatch_cross_fsm_event(context, payload):
    return _queue_item(context, {"kind": "fsm_event", "payload": payload})


def _worker_loop(context):
    with context["lock"]:
        context["state"] = "running"
    handlers = {
        "sensor_event": _handle_sensor_event,
        "pool_result": _handle_pool_result,
        "config_apply": _handle_config_apply,
        "fsm_event": _handle_fsm_event,
    }
    while not context["stop_event"].is_set():
        try:
            item = context["queue"].get(
                timeout=context["options"]["ingress_timeout_s"])
        except Empty:
            continue
        try:
            if item.get("kind") == "stop":
                break
            handler = handlers.get(item.get("kind"))
            if callable(handler):
                try:
                    handler(context, item)
                except Exception as exc:
                    with context["lock"]:
                        context["last_error"] = {
                            "code": "handler",
                            "kind": item.get("kind"),
                            "error": str(exc),
                        }
                    LOGGER.exception(
                        "Cross Worker handler failed: %s", item.get("kind"))
        finally:
            context["queue"].task_done()
    with context["lock"]:
        context["state"] = "stopped"


def start_cross_worker(context):
    if not _is_dict(context) or not context["options"]["enabled"]:
        if _is_dict(context):
            context["state"] = "disabled"
        return None
    with context["lock"]:
        if _thread_alive(context.get("thread")):
            return context["thread"]
        context["stop_event"].clear()
        thread = threading.Thread(
            target=partial(_worker_loop, context),
            name="CrossWorker-{}-{}".format(
                context["options"]["node_id"],
                context["options"]["domain_id"],
            ),
            daemon=False,
        )
        context["thread"] = thread
        context["state"] = "starting"
        thread.start()
        return thread


def stop_cross_worker(context, **kwargs):
    if not _is_dict(context):
        return {"state": "missing"}
    with context["lock"]:
        if context["state"] == "disabled":
            return get_cross_worker_snapshot(context)
        context["state"] = "stopping"
        context["stop_event"].set()
    try:
        context["queue"].put_nowait({"kind": "stop"})
    except Full:
        pass
    thread = context.get("thread")
    timeout = _safe_float(
        kwargs.get("join_timeout_s"), context["options"]["join_timeout_s"], 0.01)
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=timeout)
    with context["lock"]:
        context["state"] = "stuck" if _thread_alive(thread) else "stopped"
    return get_cross_worker_snapshot(context)


def get_cross_worker_snapshot(context):
    if not _is_dict(context):
        return {"state": "missing", "enabled": False}
    with context["lock"]:
        machines = context["fsm"].get("machines") or {}
        fsm_states = {
            name: fsm_snapshot(machine)
            for name, machine in sorted(machines.items())
        }
        result = {
            "module": context["module"],
            "version": context["version"],
            "state": context["state"],
            "enabled": context["options"]["enabled"],
            "thread_alive": _thread_alive(context.get("thread")),
            "node_id": context["options"]["node_id"],
            "domain_id": context["options"]["domain_id"],
            "revision": context["revision"],
            "fingerprint": context["fingerprint"],
            "execution_epoch": context["execution_epoch"],
            "execution_active": context["execution_active"],
            "authority_config": copy.deepcopy(context["authority_config"]),
            "pipeline_active": context["pipeline_active"],
            "pipeline_sequence": context["pipeline_sequence"],
            "pending_count": len(context["pending"]),
            "freeze_intent": copy.deepcopy(context.get("freeze_intent")),
            "activation_proof": copy.deepcopy(
                context.get("activation_proof")),
            "rule_binding": copy.deepcopy(context["rule_binding"]),
            "fsm": {"ok": context["fsm"].get("ok"),
                    "error": context["fsm"].get("error"),
                    "states": fsm_states},
            "statistics": dict(context["statistics"]),
            "last_error": copy.deepcopy(context["last_error"]),
            "last_decision": copy.deepcopy(context["last_decision"]),
            "fsm_active": tuple(sorted(context["fsm_active"])),
            "fsm_pending": sum(
                len(items) for items in context["fsm_pending"].values()),
        }
    result["io"] = context["adapter"]["snapshot"]()
    snapshot_api = context.get("snapshot_store")
    result["snapshot_store"] = (
        snapshot_api["status"]()
        if snapshot_api is not None
        else {"enabled": False}
    )
    return result


def create_cross_capabilities(context):
    return {
        "dispatch_event": partial(dispatch_cross_event, context),
        "dispatch_pool_result": partial(dispatch_cross_pool_result, context),
        "dispatch_fsm_event": partial(dispatch_cross_fsm_event, context),
        "apply_config": partial(apply_cross_config, context),
        "freeze_execution": partial(freeze_cross_execution, context),
        "snapshot": partial(get_cross_worker_snapshot, context),
    }


