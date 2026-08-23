# -*- coding: utf-8 -*-

"""Crash-safe JSON persistence provenance and exact snapshot fingerprints."""

import json
import os
import shutil
import sys
import tempfile


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library import _generic_data_center as data_center
from src.library import _json_formator as json_formator
from src.library import _generic_type_library as type_library


SOURCE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data", "_legacy",
    "device_state_data_v5_multi_worker_norm.json",
)
PRIMARY_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data",
    "device_state_data_v9_node_fn-01.json",
)


def _reverse_object_fields(value):
    if isinstance(value, dict):
        return {
            key: _reverse_object_fields(value[key])
            for key in reversed(tuple(value))
        }
    if isinstance(value, list):
        return [_reverse_object_fields(item) for item in value]
    return value


def _entity_order(rows, id_field):
    return tuple(row.get(id_field) for row in rows)


def main():
    with tempfile.TemporaryDirectory(prefix="config-persistence-receipt-") as directory:
        config_path = os.path.join(directory, "config.json")
        receipt_path = os.path.join(directory, "receipt.json")
        shutil.copyfile(SOURCE_CONFIG, config_path)
        context = data_center.create_data_center_context(
            name="receipt-test",
            json_path=config_path,
            json_receipt_path=receipt_path,
            sort_mode="none",
            preserve_config_identity_on_save=True,
            allow_legacy_import=True,
            legacy_import_profile="lab_v2",
        )
        assert data_center.load_json_into_heap(context)["status"] == "OK"
        found, loaded = data_center.heap_snapshot(context)
        assert found and len(loaded["actuators"]) > 1
        assert data_center.request_write_commit(
            context,
            "actuators",
            list(reversed(loaded["actuators"])),
            source="receipt-render-order",
        )["status"] == "OK"
        assert data_center.request_write_commit(
            context,
            "process_states[0].parameters.target_temp_c",
            555.0,
            source="receipt-test",
        )["status"] == "OK"

        original_atomic_write = data_center.atomic_write_text
        raced = {"done": False}

        def racing_atomic_write(path, text, encoding):
            if os.path.abspath(path) == os.path.abspath(config_path) and not raced["done"]:
                raced["done"] = True
                # The persisted snapshot was already captured. Move live state
                # forward while file IO is in progress to prove that the save
                # report/receipt describe the bytes written, not the newer heap.
                result = data_center.request_write_commit(
                    context,
                    "process_states[0].parameters.target_temp_c",
                    556.0,
                    source="receipt-race",
                )
                assert result["status"] == "OK"
            return original_atomic_write(path, text, encoding)

        data_center.atomic_write_text = racing_atomic_write
        try:
            report = data_center.save_heap_to_json(context)
        finally:
            data_center.atomic_write_text = original_atomic_write

        assert report["status"] == "OK", report
        persisted, verification = type_library.load_config_file(config_path)
        assert persisted is not None and verification["status"] == "OK"
        persisted_fingerprint = type_library.config_fingerprint(persisted)
        assert report["details"]["fingerprint"] == persisted_fingerprint
        assert data_center.get_fingerprint(context) != persisted_fingerprint

        with open(receipt_path, "r", encoding="utf-8") as handle:
            receipt = json.load(handle)
        assert receipt["version"] == 1
        assert receipt["state"] == "COMMITTED"
        assert receipt["target_path"] == os.path.abspath(config_path)
        assert receipt["target_fingerprint"] == persisted_fingerprint
        assert int(receipt["revision"]) == int(report["details"]["revision"])
        assert data_center.describe_data_center(context)["statistics"][
            "json_receipts"] == 2

    with tempfile.TemporaryDirectory(prefix="config-identity-guard-") as directory:
        config_path = os.path.join(directory, "config.json")
        receipt_path = os.path.join(directory, "receipt.json")
        shutil.copyfile(SOURCE_CONFIG, config_path)
        with open(config_path, "rb") as handle:
            original_bytes = handle.read()
        context = data_center.create_data_center_context(
            name="identity-guard-test",
            json_path=config_path,
            json_receipt_path=receipt_path,
            sort_mode="numeric_pk",
            preserve_config_identity_on_save=True,
            allow_legacy_import=True,
            legacy_import_profile="lab_v2",
        )
        assert data_center.load_json_into_heap(context)["status"] == "OK"
        found, loaded = data_center.heap_snapshot(context)
        assert found and len(loaded["actuators"]) > 1
        assert data_center.request_write_commit(
            context,
            "actuators",
            list(reversed(loaded["actuators"])),
            source="identity-guard-test",
        )["status"] == "OK"

        report = data_center.save_heap_to_json(context)
        assert report["status"] == "ERROR"
        assert report["message"] == (
            "save rejected: rendered JSON changes config identity")
        assert report["details"]["snapshot_fingerprint"] != report[
            "details"]["rendered_fingerprint"]
        with open(config_path, "rb") as handle:
            assert handle.read() == original_bytes
        assert not os.path.exists(receipt_path)

    with tempfile.TemporaryDirectory(prefix="config-external-edit-guard-") as directory:
        config_path = os.path.join(directory, "config.json")
        receipt_path = os.path.join(directory, "receipt.json")
        shutil.copyfile(PRIMARY_CONFIG, config_path)
        context = data_center.create_data_center_context(
            name="external-edit-guard-test",
            json_path=config_path,
            json_receipt_path=receipt_path,
            sort_mode="none",
            preserve_config_identity_on_save=True,
        )
        assert data_center.load_json_into_heap(context)["status"] == "OK"
        found, loaded = data_center.heap_snapshot(context)
        assert found
        state_index = next(
            index for index, row in enumerate(loaded["process_states"])
            if int(row.get("state_id") or 0) == 1)
        parameter_path = (
            "process_states[{}].parameters.target_temp_c".format(state_index))
        assert data_center.request_write_commit(
            context, parameter_path, 612.0,
            source="external-edit-guard-internal",
        )["status"] == "OK"

        with open(config_path, "r", encoding="utf-8") as handle:
            external = json.load(handle)
        verification = type_library.verify_storage_config_data(external)
        assert verification["status"] == "OK"
        external_state = next(
            row for row in external["process_states"]
            if int(row.get("state_id") or 0) == 1)
        external_state.setdefault("parameters", {})["target_temp_c"] = 613.0
        external_fingerprint = type_library.config_fingerprint(external)
        data_center.atomic_write_text(
            config_path, type_library.canonical_dumps(external), "utf-8")

        deferred = data_center.save_heap_to_json(context)
        assert deferred["status"] == "ERROR"
        assert deferred["message"] == (
            "save deferred: external config edit is pending"), deferred
        assert deferred["details"]["external_fingerprint"] == (
            external_fingerprint)
        with open(config_path, "r", encoding="utf-8") as handle:
            visible = json.load(handle)
        verification = type_library.verify_storage_config_data(visible)
        assert verification["status"] == "OK"
        assert type_library.config_fingerprint(visible) == external_fingerprint
        assert not os.path.exists(receipt_path)

        # The file-sync WAL may explicitly authorize this exact candidate
        # fingerprint after the live heap has already advanced independently.
        # Ordinary persistence must still defer, while the exact known
        # candidate may be replaced by the current authoritative heap.
        assert data_center.request_write_commit(
            context, parameter_path, 614.0,
            source="external-edit-guard-live-advance",
        )["status"] == "OK"
        live_fingerprint = data_center.get_fingerprint(context)
        still_deferred = data_center.save_heap_to_json(context)
        assert still_deferred["status"] == "ERROR"
        assert still_deferred["details"]["external_fingerprint"] == (
            external_fingerprint)
        accepted = data_center.save_heap_to_json(
            context,
            expected_external_fingerprint=external_fingerprint,
        )
        assert accepted["status"] == "OK"
        assert accepted["details"]["fingerprint"] == live_fingerprint

        # A stale authorization never permits overwriting a later, unrelated
        # operator edit.
        with open(config_path, "r", encoding="utf-8") as handle:
            unrelated = json.load(handle)
        unrelated_state = next(
            row for row in unrelated["process_states"]
            if int(row.get("state_id") or 0) == 1)
        unrelated_state.setdefault("parameters", {})[
            "target_temp_c"] = 615.0
        unrelated_fingerprint = type_library.config_fingerprint(unrelated)
        data_center.atomic_write_text(
            config_path, type_library.canonical_dumps(unrelated), "utf-8")
        rejected = data_center.save_heap_to_json(
            context,
            expected_external_fingerprint=external_fingerprint,
        )
        assert rejected["status"] == "ERROR"
        assert rejected["details"]["external_fingerprint"] == (
            unrelated_fingerprint)

    with tempfile.TemporaryDirectory(prefix="config-readable-v9-") as directory:
        config_path = os.path.join(directory, "config.json")
        receipt_path = os.path.join(directory, "receipt.json")
        with open(PRIMARY_CONFIG, "r", encoding="utf-8") as handle:
            primary = json.load(handle)
        master_ordered = _reverse_object_fields(primary)
        expected_fingerprint = type_library.config_fingerprint(master_ordered)
        expected_actuator_order = _entity_order(
            master_ordered["actuators"], "actuator_id")
        expected_state_order = _entity_order(
            master_ordered["process_states"], "state_id")
        with open(config_path, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(master_ordered, handle, ensure_ascii=False)
            handle.write("\n")

        context = data_center.create_data_center_context(
            name="readable-v9-test",
            json_path=config_path,
            json_receipt_path=receipt_path,
            sort_mode="fingerprint_safe",
            preserve_config_identity_on_save=True,
        )
        assert data_center.load_json_into_heap(context)["status"] == "OK"
        report = data_center.save_heap_to_json(context)
        assert report["status"] == "OK", report
        with open(config_path, "r", encoding="utf-8") as handle:
            persisted = json.load(handle)

        expected_sections = [
            key for key in json_formator.TOP_LEVEL_ORDER if key in persisted]
        assert list(persisted) == expected_sections
        actuator = next(
            row for row in persisted["actuators"]
            if int(row.get("actuator_id") or 0) == 5)
        assert list(actuator)[:3] == [
            "local_controller_id", "actuator_id", "profile_id"]
        state = next(
            row for row in persisted["process_states"]
            if int(row.get("state_id") or 0) == 21)
        assert list(state)[0] == "state_id"
        assert list(state)[-1] == "parameters"
        assert _entity_order(
            persisted["actuators"], "actuator_id") == expected_actuator_order
        assert _entity_order(
            persisted["process_states"], "state_id") == expected_state_order
        assert type_library.config_fingerprint(
            persisted) == expected_fingerprint
        assert report["details"]["fingerprint"] == expected_fingerprint

    print(
        "PASS crash-safe config persistence receipt, exact saved fingerprint, "
        "identity guard, external-edit lost-update guard, and canonical "
        "fingerprint-safe V9 readability")


if __name__ == "__main__":
    main()
