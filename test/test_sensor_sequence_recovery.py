# -*- coding: utf-8 -*-

"""Crash-safe sensor sequence reservation and Master-floor recovery."""

import os
import sys
import tempfile


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._runtime_value_store import (
    advance_global_sequence_floor,
    claim_controller_writer,
    close_runtime_value_store,
    create_runtime_value_store,
    create_writer_token,
    release_controller_writer,
    write_sensor_value,
)


CONTROLLER_UUID = "62671112-766c-59ed-b0ff-30a49d48f747"


def _write_one(store, sensor_id):
    token = create_writer_token("sequence-test")
    assert claim_controller_writer(store, CONTROLLER_UUID, token)["ok"]
    result = write_sensor_value(
        store, controller_uuid=CONTROLLER_UUID, sensor_id=sensor_id,
        value=123, writer_token=token)
    assert result["ok"]
    sequence = int(result["entry"]["sequence"])
    assert release_controller_writer(store, CONTROLLER_UUID, token)["ok"]
    return sequence


def main():
    with tempfile.TemporaryDirectory(prefix="sensor-sequence-") as directory:
        path = os.path.join(directory, "sequence.json")
        first = create_runtime_value_store(
            sequence_state_path=path, sequence_reservation_size=8)
        assert first["global_sequence"] == 0
        assert first["sequence_reserved_until"] == 8
        assert _write_one(first, 1) == 1
        close_runtime_value_store(first)

        restarted = create_runtime_value_store(
            sequence_state_path=path, sequence_reservation_size=8)
        assert restarted["global_sequence"] == 8
        assert restarted["sequence_reserved_until"] == 16
        assert _write_one(restarted, 1) == 9
        advanced = advance_global_sequence_floor(restarted, 100)
        assert advanced["ok"] and advanced["changed"]
        assert advanced["global_sequence"] == 100
        assert advanced["reserved_until"] == 108
        assert _write_one(restarted, 2) == 101
        close_runtime_value_store(restarted)

        third = create_runtime_value_store(
            sequence_state_path=path, sequence_reservation_size=8)
        assert third["global_sequence"] == 108
        assert _write_one(third, 1) == 109
        close_runtime_value_store(third)
    print("PASS crash-safe sensor sequence recovery")


if __name__ == "__main__":
    main()
