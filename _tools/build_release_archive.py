#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Build and independently verify the deterministic Worker release ZIP."""

import argparse
import hashlib
import os
import re
import stat
import tempfile
import zipfile

from functools import partial
from pathlib import Path, PurePosixPath


EXPECTED_VERSION = "automation_system_v0101"
ZIP_TIMESTAMP = (2026, 7, 25, 0, 0, 0)
TRANSIENT_DIRS = frozenset((
    "__pycache__", ".git", ".pytest_cache", ".venv", "venv",
))
TRANSIENT_SUFFIXES = (".pyc", ".pyo", ".log")
SENSITIVE_SUFFIXES = (".key", ".pem", ".p12", ".pfx")
MANIFEST_LINE = re.compile(r"([0-9a-f]{64})  [.]\/([A-Za-z0-9._\/-]+)")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(partial(stream.read, 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _path_sort_key(item):
    return item.as_posix().encode("utf-8")


def _relative_files(root, omit_manifest=False):
    result = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if any(part in TRANSIENT_DIRS for part in relative.parts):
            continue
        if path.is_symlink():
            raise RuntimeError("symbolic links are forbidden: " + relative.as_posix())
        if not path.is_file():
            continue
        if omit_manifest and relative == Path("SHA256SUMS"):
            continue
        result.append(relative)
    return sorted(result, key=_path_sort_key)


def reject_unsafe_tree(root):
    failures = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        name = relative.as_posix()
        if path.is_symlink():
            failures.append(name + " (symbolic link)")
            continue
        if any(part in TRANSIENT_DIRS for part in relative.parts):
            failures.append(name)
            continue
        if not path.is_file():
            continue
        if path.suffix.lower() in TRANSIENT_SUFFIXES + SENSITIVE_SUFFIXES:
            failures.append(name)
        if path.suffix.lower() == ".sh" and b"\r" in path.read_bytes():
            failures.append(name + " (carriage return in shell script)")
        if relative.parts[:2] == ("_config", "certs") and path.name != "README.md":
            failures.append(name)
        if relative.parts and relative.parts[0] in ("logs", "_runtime"):
            failures.append(name)
    if failures:
        raise RuntimeError(
            "runtime, cache or sensitive files remain in Worker release:\n  "
            + "\n  ".join(sorted(set(failures))[:40])
        )


def write_manifest(root):
    lines = [
        "{}  ./{}".format(sha256(root / relative), relative.as_posix())
        for relative in _relative_files(root, omit_manifest=True)
    ]
    manifest = root / "SHA256SUMS"
    temporary = manifest.with_name(manifest.name + ".tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    os.replace(temporary, manifest)
    return manifest


def verify_manifest(root):
    manifest = root / "SHA256SUMS"
    seen = set()
    for number, line in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
        match = MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise RuntimeError("malformed SHA256SUMS line {}".format(number))
        relative = PurePosixPath(match.group(2))
        if any(part in ("", ".", "..") for part in relative.parts):
            raise RuntimeError("unsafe manifest path: " + relative.as_posix())
        name = relative.as_posix()
        if name in seen:
            raise RuntimeError("duplicate manifest path: " + name)
        seen.add(name)
        path = root.joinpath(*relative.parts)
        if not path.is_file() or path.is_symlink():
            raise RuntimeError("manifest file is absent or unsafe: " + name)
        if sha256(path) != match.group(1):
            raise RuntimeError("manifest hash mismatch: " + name)
    expected = {item.as_posix() for item in _relative_files(root, omit_manifest=True)}
    if seen != expected:
        raise RuntimeError("SHA256SUMS does not cover the release exactly")


def _zip_info(name, directory=False):
    info = zipfile.ZipInfo(name, ZIP_TIMESTAMP)
    info.create_system = 3
    info.flag_bits |= 0x800
    executable = directory or name.endswith(".sh") or name.endswith(".py")
    mode = 0o755 if executable else 0o644
    kind = stat.S_IFDIR if directory else stat.S_IFREG
    info.external_attr = (kind | mode) << 16
    if directory:
        info.external_attr |= 0x10
    info.compress_type = zipfile.ZIP_DEFLATED
    return info


def build_zip(root, destination):
    top = root.name
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.unlink(missing_ok=True)
    directories = sorted(
        (path.relative_to(root) for path in root.rglob("*") if path.is_dir()),
        key=_path_sort_key,
    )
    try:
        with zipfile.ZipFile(
            temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9,
        ) as archive:
            archive.writestr(_zip_info(top + "/", directory=True), b"")
            for relative in directories:
                if any(part in TRANSIENT_DIRS for part in relative.parts):
                    continue
                archive.writestr(
                    _zip_info(top + "/" + relative.as_posix() + "/", directory=True),
                    b"",
                )
            for relative in _relative_files(root):
                archive.writestr(
                    _zip_info(top + "/" + relative.as_posix()),
                    (root / relative).read_bytes(),
                )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def verify_zip(root, destination):
    expected = {root.name + "/" + item.as_posix() for item in _relative_files(root)}
    actual = set()
    with zipfile.ZipFile(destination) as archive:
        if archive.testzip() is not None:
            raise RuntimeError("Worker ZIP CRC validation failed")
        for info in archive.infolist():
            path = PurePosixPath(info.filename)
            if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
                raise RuntimeError("unsafe Worker ZIP member: " + info.filename)
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise RuntimeError("symbolic link in Worker ZIP: " + info.filename)
            if not info.is_dir():
                actual.add(info.filename)
        if actual != expected:
            raise RuntimeError("Worker ZIP file set differs from release tree")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    version = (root / "VERSION").read_text(encoding="utf-8").strip()
    if root.name != EXPECTED_VERSION or version != EXPECTED_VERSION:
        raise RuntimeError("Worker directory/VERSION must both be " + EXPECTED_VERSION)
    reject_unsafe_tree(root)
    write_manifest(root)
    verify_manifest(root)
    destination = (arguments.output or root.parent / (EXPECTED_VERSION + ".zip")).resolve()
    if destination == root or root in destination.parents:
        raise RuntimeError("Worker ZIP destination must be outside the release tree")
    build_zip(root, destination)
    verify_zip(root, destination)
    sidecar = destination.with_name(destination.name + ".sha256")
    sidecar.write_text(
        "{}  {}\n".format(sha256(destination), destination.name),
        encoding="utf-8", newline="\n",
    )
    print("PASS Worker release ZIP: " + str(destination))
    print("SHA-256: " + sha256(destination))
    print("Manifest SHA-256: " + sha256(root / "SHA256SUMS"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        print("ERROR: " + str(error), file=__import__("sys").stderr)
        raise SystemExit(1) from error
