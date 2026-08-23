# -*- coding: utf-8 -*-

# /tools/__tree_finder.py


"""
Ohne --all und ohne --max-depth zeigt es genau die komplette Verzeichnisstruktur – genau wie der klassische tree-Befehl,
nur mit den üblichen „Rausfiltern“ für Müll- und versteckte Ordner. Wenn du wirklich alles sehen willst, reicht einfach:
"""

import argparse
import ctypes
import os
import sys
from functools import partial


NOISE_DIRS = frozenset({
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
})


FILE_ATTRIBUTE_HIDDEN = 0x2
INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF


def normalize_root(path):
    return os.path.normpath(os.path.abspath(path))


def is_windows():
    return os.name == "nt"


def get_windows_file_attributes(path):
    try:
        get_file_attributes = ctypes.windll.kernel32.GetFileAttributesW
    except AttributeError:
        return None

    get_file_attributes.argtypes = [ctypes.c_wchar_p]
    get_file_attributes.restype = ctypes.c_uint32

    attrs = get_file_attributes(path)
    if attrs == INVALID_FILE_ATTRIBUTES:
        return None
    return attrs


def has_windows_hidden_attribute(path):
    if not is_windows():
        return False

    attrs = get_windows_file_attributes(path)
    if attrs is None:
        return False

    return bool(attrs & FILE_ATTRIBUTE_HIDDEN)


def is_hidden_name(name):
    return name.startswith(".")


def is_hidden_path(path):
    name = os.path.basename(path.rstrip("\\/"))
    if is_hidden_name(name):
        return True

    if has_windows_hidden_attribute(path):
        return True

    return False


def should_skip_entry(path, name, show_all):
    if name in NOISE_DIRS:
        return True

    if not show_all and is_hidden_path(path):
        return True

    return False


def safe_scandir(path):
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                yield entry
    except PermissionError:
        return
    except FileNotFoundError:
        return
    except OSError:
        return


def sort_entries(entries):
    def sort_key(entry):
        return os.fsencode(entry.name)

    return sorted(entries, key=sort_key)


def format_line(depth, name, is_dir_like):
    indent = "│   " * max(depth - 1, 0)
    suffix = "/" if is_dir_like else ""
    return f"{indent}├─ {name}{suffix}"


def is_dir_like(entry):
    try:
        if entry.is_dir(follow_symlinks=False):
            return True

        if entry.is_symlink():
            return entry.is_dir(follow_symlinks=True)
    except OSError:
        return False

    return False


def walk_tree(root, show_all, max_depth):
    yield root + os.sep

    def _walk(current_path, depth):
        if max_depth is not None and depth > max_depth:
            return

        entries = []
        for entry in safe_scandir(current_path):
            entry_path = entry.path
            entry_name = entry.name

            if should_skip_entry(entry_path, entry_name, show_all):
                continue

            entries.append(entry)

        for entry in sort_entries(entries):
            entry_path = entry.path
            entry_name = entry.name
            dir_like = is_dir_like(entry)

            yield format_line(depth, entry_name, dir_like)

            if dir_like:
                is_symlink_dir = False
                try:
                    is_symlink_dir = entry.is_symlink()
                except OSError:
                    is_symlink_dir = False

                if not is_symlink_dir:
                    yield from _walk(entry_path, depth + 1)

    yield from _walk(root, 1)


def write_output(lines, out_file):
    if out_file:
        with open(out_file, "w", encoding="utf-8", newline="\n") as handle:
            for line in lines:
                handle.write(line)
                handle.write("\n")
        return

    for line in lines:
        print(line)


def positive_int(value):
    try:
        parsed = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("muss eine ganze Zahl sein") from exc

    if parsed < 0:
        raise argparse.ArgumentTypeError("muss >= 0 sein")

    return parsed


def build_parser():
    parser = argparse.ArgumentParser(
        description="Vollständige Verzeichnisstruktur inkl. Dateien anzeigen."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Wurzelverzeichnis (Standard: aktuelles Verzeichnis)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="auch versteckte Dateien/Ordner anzeigen"
    )
    parser.add_argument(
        "--max-depth",
        type=positive_int,
        default=None,
        help="maximale Tiefe relativ zu ROOT"
    )
    parser.add_argument(
        "--out",
        default="",
        help="Ausgabe in Datei schreiben"
    )
    return parser


def reconfigure_stdio():
    targets = [sys.stdout, sys.stderr]
    for stream in targets:
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass


def main():
    reconfigure_stdio()

    parser = build_parser()
    args = parser.parse_args()

    root = normalize_root(args.root)

    if not os.path.isdir(root):
        print(f"ERROR: ROOT ist kein Verzeichnis: {args.root}", file=sys.stderr)
        return 2

    generate_lines = partial(
        walk_tree,
        root,
        args.all,
        args.max_depth,
    )

    try:
        write_output(generate_lines(), args.out)
    except OSError as exc:
        print(f"ERROR: Ausgabe fehlgeschlagen: {exc}", file=sys.stderr)
        return 2

    if args.out:
        print(f"OK: geschrieben nach {args.out}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())