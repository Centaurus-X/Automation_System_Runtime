#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Regression checks for Worker release archive safety boundaries."""

import shutil
import sys
import tempfile

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from _tools import build_release_archive as builder


def test_shell_carriage_return_is_rejected():
    root = Path(tempfile.mkdtemp(prefix="worker-release-builder-"))
    try:
        script = root / "run.sh"
        script.write_bytes(b"#!/bin/sh\r\nprintf 'unsafe'\r\n")
        try:
            builder.reject_unsafe_tree(root)
        except RuntimeError as error:
            detail = str(error)
            assert "run.sh" in detail
            assert "carriage return" in detail
        else:
            raise AssertionError("shell script with carriage returns was accepted")
    finally:
        shutil.rmtree(root)


def main():
    test_shell_carriage_return_is_rejected()
    print("PASS Worker release builder rejects shell carriage returns")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())