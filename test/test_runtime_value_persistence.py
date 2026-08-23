# -*- coding: utf-8 -*-

"""Asynchronous central sensor snapshot persistence smoke test."""

import json
import os
import sys
import tempfile
import time


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._runtime_value_store import (
    claim_controller_writer,
    close_runtime_value_store,
    create_runtime_value_store,
    create_sensor_value_persistence,
    create_writer_token,
    start_sensor_value_persistence,
    stop_sensor_value_persistence,
    write_sensor_value,
)


def run_test():
    directory = tempfile.mkdtemp(prefix="runtime-value-persistence-")
    file_path = os.path.join(directory, "sensor_values.json")
    store = create_runtime_value_store(name="persistence-test")
    controller_uuid = "62671112-766c-59ed-b0ff-30a49d48f747"
    token = create_writer_token("persistence-test")
    assert claim_controller_writer(store, controller_uuid, token)["ok"]
    persistence = create_sensor_value_persistence(
        store, file_path, flush_interval_s=0.02, batch_size=16)
    start_sensor_value_persistence(persistence)
    assert write_sensor_value(
        store,
        controller_uuid=controller_uuid,
        sensor_id=1,
        value=12.5,
        quality="good",
        writer_token=token,
    )["ok"]
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and not os.path.exists(file_path):
        time.sleep(0.02)
    snapshot = stop_sensor_value_persistence(persistence, timeout_s=3.0)
    assert snapshot["thread_alive"] is False
    with open(file_path, "r", encoding="utf-8") as handle:
        persisted = json.load(handle)
    entry = persisted["controllers"][controller_uuid]["values"]["1"]
    assert entry["value"] == 12.5
    assert entry["quality"] == "good"
    assert close_runtime_value_store(store)
    print("PASS asynchronous sensor-value persistence")


if __name__ == "__main__":
    run_test()
