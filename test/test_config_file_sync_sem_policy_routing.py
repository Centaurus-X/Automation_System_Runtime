# -*- coding: utf-8 -*-

"""Real SEM routing test for operator file edits and safety boundaries."""

import copy
import json
import os
import sys
import tempfile
import threading

from queue import Queue


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core import config_file_sync as config_file_sync_module
from src.core.config_file_sync import (
    _process_candidate,
    create_config_file_sync,
)
from src.core.state_event_management import (
    register_post_commit_handler,
    request_config_action_plan,
    request_config_replace,
    request_config_scope_operations,
    start_state_event_management,
    stop_state_event_management,
)
from src.library import _generic_data_center as data_center
from src.library import _generic_type_library as type_library


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data",
    "device_state_data_v9_node_fn-01.json")


def _state(config, state_id):
    return next(
        row for row in config.get("process_states") or []
        if str(row.get("state_id")) == str(state_id))


def _write(path, config):
    storage = type_library.project_config_for_storage(config)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(
            storage, handle, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"))


def _snapshot(center):
    found, value = data_center.heap_snapshot(center)
    if not found:
        raise RuntimeError("test DataCenter snapshot is unavailable")
    return value


def _sem_options():
    return {
        "ingress_timeout_s": 0.02,
        "request_timeout_s": 3.0,
        "persistence_enabled": False,
        "safety": {
            "enabled": True,
            "rule_actions": {
                "enabled": True,
                "require_base_revision": True,
                "enforce_writable_fields": True,
                "enforce_required_mode": True,
                "parameter_limits": {
                    "target_temp_c": {"min": 0.0, "max": 800.0},
                },
            },
            "external_commits": {
                "enabled": True,
                "require_base_revision": True,
                "allowed_operations": ["scope_operations"],
                "allowed_principals": ["worker_file_sync"],
                "allow_arbitrary_paths": True,
            },
        },
    }


def _run_test():
    with tempfile.TemporaryDirectory(
            prefix="config-file-real-sem-routing-") as directory:
        config_path = os.path.join(directory, "config.json")
        wal_path = os.path.join(directory, "proposal-wal.json")
        with open(SOURCE_CONFIG, "r", encoding="utf-8") as handle:
            storage = json.load(handle)
        baseline = type_library.normalize_config_for_runtime(
            storage, representation="storage")
        _write(config_path, baseline)

        center = data_center.create_data_center_context(
            name="config-file-real-sem-routing",
            json_path=config_path,
            allow_legacy_import=False,
        )
        assert data_center.load_json_into_heap(center)["status"] == "OK"
        runtime = {}
        thread = start_state_event_management(
            data_center=center,
            queues={
                "queue_event_state": Queue(),
                "queue_event_send": Queue(),
            },
            stop_event=threading.Event(),
            options=_sem_options(),
            runtime=runtime,
        )
        sem = runtime["context"]
        distributed = {
            "authority_epoch": 0,
            "scope_revision": 0,
            "scope_fingerprint": type_library.config_fingerprint(baseline),
            "scope_calls": 0,
            "mirrors": [],
        }

        def post_commit(_event, result):
            if not isinstance(result, dict) or not result.get("ok"):
                return {"ok": True, "status": "local_commit_not_applied"}
            if not result.get("changed"):
                return {
                    "ok": True,
                    "status": "unchanged",
                    "authority_epoch": distributed["authority_epoch"],
                    "scope_revision": distributed["scope_revision"],
                    "fingerprint": distributed["scope_fingerprint"],
                }
            distributed["scope_revision"] += 1
            distributed["scope_fingerprint"] = type_library.config_fingerprint(
                _snapshot(center))
            mirror_id = "mirror:fn-01:{}:{}".format(
                distributed["scope_revision"],
                distributed["scope_fingerprint"][:12])
            distributed["mirrors"].append(mirror_id)
            return {
                "ok": True,
                "code": "accepted",
                "status": "node_authoritative_committed",
                "authority_epoch": distributed["authority_epoch"],
                "scope_revision": distributed["scope_revision"],
                "fingerprint": distributed["scope_fingerprint"],
                "mirror_id": mirror_id,
            }

        register_post_commit_handler(sem, post_commit)

        def snapshot():
            return _snapshot(center)

        def revision():
            return data_center.get_revision(center)

        def replace(config, **kwargs):
            return request_config_replace(sem, config, **kwargs)

        def persist_json(**kwargs):
            return data_center.save_heap_to_json(center, **kwargs)

        def request_actions(actions, **kwargs):
            return request_config_action_plan(sem, actions, **kwargs)

        def request_scope(operations, **kwargs):
            distributed["scope_calls"] += 1
            assert kwargs.get("scope") == "node:fn-01"
            assert kwargs.get("base_authority_epoch") \
                == distributed["authority_epoch"]
            assert kwargs.get("base_scope_revision") \
                == distributed["scope_revision"]
            request_options = dict(kwargs)
            request_options["base_revision"] = revision()
            return request_config_scope_operations(
                sem, operations, **request_options)

        def distributed_snapshot():
            return {
                "authority_epoch": distributed["authority_epoch"],
                "node_scope_revision": distributed["scope_revision"],
                "node_shard_fingerprint": distributed["scope_fingerprint"],
            }

        api = create_config_file_sync(
            options={
                "enabled": True,
                "node_id": "fn-01",
                "config_path": config_path,
                "wal_path": wal_path,
                "proposal_timeout_s": 1.0,
            },
            config_capabilities={
                "get_snapshot": snapshot,
                "get_revision": revision,
                "request_replace": replace,
                "persist_json": persist_json,
            },
            distributed_api={
                "request_action_plan": request_actions,
                "request_scope_operations": request_scope,
                "snapshot": distributed_snapshot,
            },
        )
        context = api["context"]

        candidate = _snapshot(center)
        _state(candidate, 21)["parameters"]["target_temp_c"] = 400.0
        _write(config_path, candidate)
        warnings = []

        def capture_warning(message, *args):
            warnings.append(message % args if args else message)

        original_warning = config_file_sync_module.LOGGER.warning
        config_file_sync_module.LOGGER.warning = capture_warning
        try:
            assert _process_candidate(
                context, candidate,
                type_library.config_fingerprint(candidate))
        finally:
            config_file_sync_module.LOGGER.warning = original_warning
        assert context["wal"]["last_operation"]["mode"] == "node_local"
        assert context["wal"]["state"] == "IDLE"
        proof = context["wal"]["last_operation"]
        authority = proof["authority_commit"]
        assert proof["commit_kind"] == "scope_operations"
        assert authority["authority_mode"] == "node_local"
        assert authority["scope_revision"] == 1
        assert authority["mirror_id"] == distributed["mirrors"][0]
        assert distributed["scope_calls"] == 1
        assert len(warnings) == 1
        assert "required_mode" in warnings[0]
        assert "Worker node authority" in warnings[0]
        assert context["statistics"]["rollbacks"] == 0
        assert context["statistics"]["node_local"] == 1
        assert _state(_snapshot(center), 21)["parameters"][
            "target_temp_c"] == 400.0

        unsafe = _snapshot(center)
        _state(unsafe, 21)["parameters"]["target_temp_c"] = 1800.0
        _write(config_path, unsafe)
        try:
            _process_candidate(
                context, unsafe,
                type_library.config_fingerprint(unsafe))
        except RuntimeError as exc:
            assert "parameter_limit" in str(exc)
        else:
            raise AssertionError("unsafe parameter limit was routed to Master")
        assert distributed["scope_calls"] == 1
        assert context["statistics"]["rollbacks"] == 1
        assert context["wal"]["state"] == "IDLE"
        assert _state(_snapshot(center), 21)["parameters"][
            "target_temp_c"] == 400.0

        stopped = stop_state_event_management(sem, join_timeout_s=3.0)
        assert stopped["state"] == "stopped"
        assert not thread.is_alive()


if __name__ == "__main__":
    _run_test()
    print(
        "PASS real SEM file edit commits through node-local authority; "
        "parameter_limit remains enforced; mirror receipt is durable")


