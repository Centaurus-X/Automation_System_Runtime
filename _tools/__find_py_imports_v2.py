# -*- coding: utf-8 -*-

# /tools/__find_py_imports_v2.py


"""
Find python module imports
"""


import argparse
import ast
import functools
import json
import os
import re
import sys
import sysconfig
import tokenize
from datetime import datetime, timezone
from pathlib import Path

try:
    from importlib import metadata as importlib_metadata
except ImportError:  # pragma: no cover
    importlib_metadata = None


DEFAULT_IGNORED_DIRS = (
    ".git",
    ".hg",
    ".svn",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "env",
    "site-packages",
    "venv",
)

STDLIB_SECTION_PREFIX = "# stdlib:"
UNRESOLVED_SECTION_PREFIX = "# unresolved-import:"


def normalize_name(name) :
    return re.sub(r"[-_.]+", "-", name).strip().lower()


def path_is_relative_to(path, other) :
    try:
        path.relative_to(other)
        return True
    except ValueError:
        return False


def read_source(path) :
    with tokenize.open(path) as handle:
        return handle.read()


def write_text(path, content) :
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def split_csv_values(values) :
    items = []
    for value in values:
        for chunk in str(value).split(","):
            cleaned = chunk.strip()
            if cleaned:
                items.append(cleaned)
    return items


def build_source_roots(project_root, raw_source_roots) :
    roots = []
    candidates = []

    if raw_source_roots:
        for item in raw_source_roots:
            candidate = Path(item)
            if not candidate.is_absolute():
                candidate = project_root / candidate
            candidates.append(candidate.resolve())
    else:
        candidates.append(project_root)
        src_root = (project_root / "src").resolve()
        if src_root.exists() and src_root.is_dir():
            candidates.append(src_root)

    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        if candidate.exists() and candidate.is_dir():
            roots.append(candidate)
            seen.add(candidate)

    if not roots:
        roots.append(project_root)

    roots.sort(key=source_root_sort_key, reverse=True)
    return roots


def source_root_sort_key(path) :
    return (len(path.parts), str(path))


def iter_py_files(project_root, ignored_dirs) :
    for current_root, dirnames, filenames in os.walk(project_root):
        pruned_dirs = []
        for dirname in dirnames:
            if dirname in ignored_dirs:
                continue
            pruned_dirs.append(dirname)
        dirnames[:] = sorted(pruned_dirs)

        current_path = Path(current_root)
        for filename in sorted(filenames):
            if not filename.endswith(".py"):
                continue
            path = current_path / filename
            if path.is_file():
                yield path


def choose_source_root(path, source_roots) :
    for root in source_roots:
        if path_is_relative_to(path, root):
            return root
    return source_roots[-1]


def path_to_module_name(path, source_roots) :
    source_root = choose_source_root(path, source_roots)
    relative_parts = list(path.relative_to(source_root).parts)
    if not relative_parts:
        return ""

    filename = relative_parts.pop()
    stem = Path(filename).stem

    if stem == "__init__":
        module_parts = relative_parts
    else:
        module_parts = relative_parts + [stem]

    cleaned_parts = [part for part in module_parts if part]
    return ".".join(cleaned_parts)


def build_local_module_index(py_files, source_roots) :
    local_modules = set()
    local_roots = set()
    module_by_path = {}

    for path in py_files:
        module_name = path_to_module_name(path, source_roots)
        module_by_path[path] = module_name
        if not module_name:
            continue

        parts = module_name.split(".")
        prefix_parts = []
        for part in parts:
            prefix_parts.append(part)
            local_modules.add(".".join(prefix_parts))

        local_roots.add(parts[0])

    return local_modules, local_roots, module_by_path


def get_current_package(module_name, is_package_file) :
    if not module_name:
        return ""

    if is_package_file:
        return module_name

    if "." not in module_name:
        return ""

    return module_name.rsplit(".", 1)[0]


def resolve_relative_module(current_package, level, module) :
    if level <= 0:
        return module or ""

    base_parts = [part for part in current_package.split(".") if part]
    trim_count = level - 1

    if trim_count > 0:
        if trim_count >= len(base_parts):
            base_parts = []
        else:
            base_parts = base_parts[: len(base_parts) - trim_count]

    if module:
        base_parts.extend([part for part in module.split(".") if part])

    return ".".join(base_parts)


def collect_import_helpers(tree) :
    importlib_aliases = set()
    import_module_names = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "importlib":
                    importlib_aliases.add(alias.asname or "importlib")
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module == "importlib":
                for alias in node.names:
                    if alias.name == "import_module":
                        import_module_names.add(alias.asname or "import_module")

    return importlib_aliases, import_module_names


def get_string_constant(node) :
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.strip() or None
    return None


def is_import_module_call(node, importlib_aliases, import_module_names) :
    func = node.func

    if isinstance(func, ast.Name):
        if func.id == "__import__":
            return True
        if func.id in import_module_names:
            return True

    if isinstance(func, ast.Attribute) and func.attr == "import_module":
        value = func.value
        if isinstance(value, ast.Name) and value.id in importlib_aliases:
            return True

    return False


def build_runtime_module_checker() :
    cache = {}

    def check(module_name) :
        import importlib.util

        if module_name in cache:
            return cache[module_name]

        try:
            spec = importlib.util.find_spec(module_name)
            cache[module_name] = spec is not None
        except Exception:
            cache[module_name] = False

        return cache[module_name]

    return functools.partial(check)


def extract_import_references(
    source,
    filename,
    module_name,
    local_modules,
    detect_dynamic_imports,
    runtime_module_exists,
) :
    references = set()
    tree = ast.parse(source, filename=filename)
    is_package_file = Path(filename).name == "__init__.py"
    current_package = get_current_package(module_name, is_package_file)
    importlib_aliases, import_module_names = collect_import_helpers(tree)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name:
                    references.add(alias.name)

        elif isinstance(node, ast.ImportFrom):
            if node.level > 0:
                base_module = resolve_relative_module(current_package, int(node.level), node.module)
            else:
                base_module = node.module or ""

            if base_module:
                references.add(base_module)

            for alias in node.names:
                if alias.name == "*":
                    continue

                if base_module:
                    candidate = base_module + "." + alias.name
                else:
                    candidate = alias.name

                if candidate in local_modules:
                    references.add(candidate)
                    continue

                if runtime_module_exists(candidate):
                    references.add(candidate)

        elif detect_dynamic_imports and isinstance(node, ast.Call):
            if not node.args:
                continue
            if not is_import_module_call(node, importlib_aliases, import_module_names):
                continue

            module_string = get_string_constant(node.args[0])
            if module_string:
                references.add(module_string)

    return references


def build_stdlib_module_set() :
    stdlib_modules = set(sys.builtin_module_names)

    names = getattr(sys, "stdlib_module_names", None)
    if names:
        stdlib_modules.update(set(names))

    return stdlib_modules


def build_stdlib_paths() :
    paths = []
    for key in ("stdlib", "platstdlib"):
        value = sysconfig.get_paths().get(key)
        if not value:
            continue
        path = Path(value).resolve()
        if path not in paths:
            paths.append(path)
    return paths


def build_stdlib_checker() :
    stdlib_modules = build_stdlib_module_set()
    stdlib_paths = build_stdlib_paths()
    cache = {}
    runtime_module_exists = build_runtime_module_checker()

    def is_stdlib(module_name) :
        import importlib.util

        if module_name in cache:
            return cache[module_name]

        if module_name in stdlib_modules:
            cache[module_name] = True
            return True

        if not runtime_module_exists(module_name):
            cache[module_name] = False
            return False

        try:
            spec = importlib.util.find_spec(module_name)
        except Exception:
            cache[module_name] = False
            return False

        if spec is None:
            cache[module_name] = False
            return False

        origin = getattr(spec, "origin", None)
        if origin in ("built-in", "frozen"):
            cache[module_name] = True
            return True

        search_locations = getattr(spec, "submodule_search_locations", None)
        if origin is None:
            if search_locations:
                for location in search_locations:
                    try:
                        location_path = Path(location).resolve()
                    except Exception:
                        continue

                    for stdlib_path in stdlib_paths:
                        if path_is_relative_to(location_path, stdlib_path):
                            location_text = str(location_path).replace("\\", "/").lower()
                            if "/site-packages/" in location_text or "/dist-packages/" in location_text:
                                continue
                            cache[module_name] = True
                            return True

            cache[module_name] = False
            return False

        try:
            origin_path = Path(origin).resolve()
        except Exception:
            cache[module_name] = False
            return False

        for stdlib_path in stdlib_paths:
            if path_is_relative_to(origin_path, stdlib_path):
                origin_text = str(origin_path).replace("\\", "/").lower()
                if "/site-packages/" in origin_text or "/dist-packages/" in origin_text:
                    continue
                cache[module_name] = True
                return True

        cache[module_name] = False
        return False

    return functools.partial(is_stdlib)


def load_package_map(path) :
    mapping = {}
    if path is None:
        return mapping
    if not path.exists():
        raise FileNotFoundError("Package-Map-Datei nicht gefunden: {0}".format(path))

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Package-Map muss ein JSON-Objekt sein.")

    for key, value in raw.items():
        cleaned_key = str(key).strip()
        if not cleaned_key:
            continue

        if isinstance(value, list):
            values = [str(item).strip() for item in value if str(item).strip()]
        else:
            values = [str(value).strip()] if str(value).strip() else []

        if values:
            mapping[cleaned_key] = values

    return mapping


def get_distribution_name_from_dist(dist) :
    name = None
    try:
        metadata_obj = getattr(dist, "metadata", None)
        if metadata_obj is not None:
            name = metadata_obj.get("Name")
    except Exception:
        name = None

    if name:
        return str(name).strip()

    try:
        name = getattr(dist, "name", None)
    except Exception:
        name = None

    if name:
        return str(name).strip()

    return None


def get_top_level_names_from_distribution(dist) :
    names = set()

    try:
        text = dist.read_text("top_level.txt")
    except Exception:
        text = None

    if text:
        for line in text.splitlines():
            cleaned = line.strip()
            if cleaned and cleaned not in names:
                names.add(cleaned)

    if names:
        return names

    try:
        files = dist.files or []
    except Exception:
        files = []

    for item in files:
        text = str(item).replace("\\", "/")
        if not text:
            continue

        if "/" not in text and not text.endswith(".py"):
            continue

        first = text.split("/", 1)[0]
        if not first or first.endswith(".dist-info") or first.endswith(".egg-info") or first.endswith(".data"):
            continue

        if first.endswith(".py"):
            names.add(Path(first).stem)
            continue

        names.add(first)

    return names


def build_import_to_distribution_map() :
    mapping = {}

    if importlib_metadata is None:
        return mapping

    packages_distributions = getattr(importlib_metadata, "packages_distributions", None)
    if packages_distributions is not None:
        raw = packages_distributions()
        for import_name, dist_names in raw.items():
            key = str(import_name).strip()
            if not key:
                continue
            mapping.setdefault(key, set()).update(
                str(item).strip() for item in dist_names if str(item).strip()
            )
        return mapping

    for dist in importlib_metadata.distributions():
        dist_name = get_distribution_name_from_dist(dist)
        if not dist_name:
            continue

        for import_name in get_top_level_names_from_distribution(dist):
            mapping.setdefault(import_name, set()).add(dist_name)

    return mapping


def build_distribution_file_cache() :
    cache = {}

    def get_files(dist_name) :
        if dist_name in cache:
            return cache[dist_name]

        file_names = set()

        if importlib_metadata is None:
            cache[dist_name] = file_names
            return file_names

        try:
            dist = importlib_metadata.distribution(dist_name)
        except Exception:
            cache[dist_name] = file_names
            return file_names

        try:
            files = dist.files or []
        except Exception:
            files = []

        for item in files:
            file_names.add(str(item).replace("\\", "/"))

        cache[dist_name] = file_names
        return file_names

    return functools.partial(get_files)


def build_distribution_version_getter() :
    cache = {}

    def get_version(dist_name) :
        if dist_name in cache:
            return cache[dist_name]

        if importlib_metadata is None:
            cache[dist_name] = None
            return None

        try:
            version_value = importlib_metadata.version(dist_name)
        except Exception:
            version_value = None

        cache[dist_name] = version_value
        return version_value

    return functools.partial(get_version)


def resolve_manual_distribution_mapping(package_map, module_name) :
    if module_name in package_map:
        return list(package_map[module_name])

    root_name = module_name.split(".", 1)[0]
    if root_name in package_map:
        return list(package_map[root_name])

    return []


def build_similarity_keys(module_name) :
    parts = [part for part in module_name.split(".") if part]
    if not parts:
        return []

    keys = []
    joined = normalize_name("-".join(parts))
    if joined:
        keys.append(joined)

    for index in range(len(parts)):
        suffix = normalize_name("-".join(parts[index:]))
        if suffix and suffix not in keys:
            keys.append(suffix)

    tail = normalize_name(parts[-1])
    if tail and tail not in keys:
        keys.append(tail)

    return keys


def resolve_namespace_distributions(
    module_name,
    candidates,
    get_distribution_files,
) :
    module_path = module_name.replace(".", "/")
    module_paths = [
        module_path,
        module_path + ".py",
        module_path + "/__init__.py",
    ]

    exact_hits = []
    prefix_hits = []

    for candidate in candidates:
        files = get_distribution_files(candidate)
        if not files:
            continue

        for file_name in files:
            if file_name in module_paths:
                exact_hits.append(candidate)
                break

        if candidate in exact_hits:
            continue

        for prefix in module_paths:
            normalized_prefix = prefix.rstrip("/")
            if not normalized_prefix:
                continue

            if normalized_prefix.endswith(".py"):
                directory_prefix = normalized_prefix.rsplit("/", 1)[0] if "/" in normalized_prefix else ""
                if directory_prefix and any(item.startswith(directory_prefix + "/") for item in files):
                    prefix_hits.append(candidate)
                    break
                continue

            if any(item.startswith(normalized_prefix + "/") for item in files):
                prefix_hits.append(candidate)
                break

    if len(exact_hits) == 1:
        return exact_hits

    if len(exact_hits) > 1:
        return sorted(set(exact_hits), key=normalize_name)

    if len(prefix_hits) == 1:
        return prefix_hits

    if len(prefix_hits) > 1:
        return sorted(set(prefix_hits), key=normalize_name)

    similarity_hits = []
    similarity_keys = build_similarity_keys(module_name)

    for candidate in candidates:
        normalized_candidate = normalize_name(candidate)
        for key in similarity_keys:
            if normalized_candidate == key:
                similarity_hits.append(candidate)
                break

    if len(similarity_hits) == 1:
        return similarity_hits

    return []


def resolve_distributions_for_module(
    module_name,
    import_to_distribution,
    package_map,
    get_distribution_files,
) :
    manual = resolve_manual_distribution_mapping(package_map, module_name)
    if manual:
        return sorted(set(manual), key=normalize_name), None

    root_name = module_name.split(".", 1)[0]
    candidates = sorted(import_to_distribution.get(root_name, set()), key=normalize_name)

    if not candidates:
        return [], "keine-zuordnung"

    if len(candidates) == 1:
        return candidates, None

    resolved = resolve_namespace_distributions(module_name, candidates, get_distribution_files)
    if resolved:
        return resolved, None

    return [], "mehrdeutiger-namespace-import"


def pin_distribution(dist_name, get_version, pin_versions) :
    if not pin_versions:
        return dist_name

    version_value = get_version(dist_name)
    if not version_value:
        return dist_name

    return "{0}=={1}".format(dist_name, version_value)


def parse_error_sort_key(item) :
    return str(item[0])


def collect_import_inventory(
    project_root,
    source_roots,
    ignored_dirs,
    detect_dynamic_imports,
    package_map,
    pin_versions,
) :
    py_files = list(iter_py_files(project_root, ignored_dirs))
    local_modules, local_roots, module_by_path = build_local_module_index(py_files, source_roots)

    runtime_module_exists = build_runtime_module_checker()
    is_stdlib = build_stdlib_checker()
    import_to_distribution = build_import_to_distribution_map()
    get_distribution_files = build_distribution_file_cache()
    get_version = build_distribution_version_getter()

    own_modules = set()
    stdlib_modules = set()
    third_party_modules = set()
    parse_errors = []

    for py_file in py_files:
        try:
            source = read_source(py_file)
            references = extract_import_references(
                source=source,
                filename=str(py_file),
                module_name=module_by_path.get(py_file, ""),
                local_modules=local_modules,
                detect_dynamic_imports=detect_dynamic_imports,
                runtime_module_exists=runtime_module_exists,
            )
        except (OSError, SyntaxError, UnicodeError) as exc:
            parse_errors.append((py_file, "{0}: {1}".format(type(exc).__name__, exc)))
            continue

        for reference in references:
            root_name = reference.split(".", 1)[0]
            if root_name in local_roots:
                own_modules.add(reference)
                continue

            if is_stdlib(root_name):
                stdlib_modules.add(reference)
                continue

            third_party_modules.add(reference)

    resolved_distributions = {}
    unresolved_imports = {}

    for module_name in sorted(third_party_modules):
        dist_names, reason = resolve_distributions_for_module(
            module_name=module_name,
            import_to_distribution=import_to_distribution,
            package_map=package_map,
            get_distribution_files=get_distribution_files,
        )

        if not dist_names:
            unresolved_imports[module_name] = reason or "unbekannt"
            continue

        for dist_name in dist_names:
            normalized_dist_name = normalize_name(dist_name)
            if normalized_dist_name in resolved_distributions:
                continue
            resolved_distributions[normalized_dist_name] = pin_distribution(
                dist_name=dist_name,
                get_version=get_version,
                pin_versions=pin_versions,
            )

    return {
        "project_root": project_root,
        "source_roots": list(source_roots),
        "py_files": py_files,
        "own_modules": sorted(own_modules),
        "stdlib_modules": sorted(stdlib_modules),
        "third_party_modules": sorted(third_party_modules),
        "resolved_requirements": sorted(resolved_distributions.values(), key=normalize_name),
        "unresolved_imports": unresolved_imports,
        "parse_errors": sorted(parse_errors, key=parse_error_sort_key),
    }


def render_list_section(title, values) :
    lines = ["[{0}]".format(title)]
    if not values:
        lines.append("-")
        return lines

    lines.extend(values)
    return lines


def render_report(inventory) :
    project_root = inventory["project_root"]
    source_roots = inventory["source_roots"]
    py_files = inventory["py_files"]
    own_modules = inventory["own_modules"]
    stdlib_modules = inventory["stdlib_modules"]
    third_party_modules = inventory["third_party_modules"]
    resolved_requirements = inventory["resolved_requirements"]
    unresolved_imports = inventory["unresolved_imports"]
    parse_errors = inventory["parse_errors"]

    lines = []
    append = lines.append
    extend = lines.extend

    append("# Import-Analyse")
    append("timestamp_utc = {0}".format(datetime.now(timezone.utc).replace(microsecond=0).isoformat()))
    append("project_root = {0}".format(project_root))
    append("source_roots = {0}".format(", ".join(str(item) for item in source_roots)))
    append("scanned_python_files = {0}".format(len(py_files)))
    append("own_modules = {0}".format(len(own_modules)))
    append("stdlib_modules = {0}".format(len(stdlib_modules)))
    append("third_party_modules = {0}".format(len(third_party_modules)))
    append("resolved_requirements = {0}".format(len(resolved_requirements)))
    append("unresolved_imports = {0}".format(len(unresolved_imports)))
    append("parse_errors = {0}".format(len(parse_errors)))
    append("")

    extend(render_list_section("EIGENE MODULE", own_modules))
    append("")
    extend(render_list_section("STANDARD PYTHON MODULE", stdlib_modules))
    append("")
    extend(render_list_section("FREMDE / DRITTANBIETER MODULE", third_party_modules))
    append("")
    extend(render_list_section("REQUIREMENTS (AUFGELOEST)", resolved_requirements))
    append("")

    append("[NICHT AUFGELOESTE FREMDE IMPORTE]")
    if unresolved_imports:
        for module_name in sorted(unresolved_imports):
            append("{0} :: {1}".format(module_name, unresolved_imports[module_name]))
    else:
        append("-")
    append("")

    append("[PARSE-FEHLER]")
    if parse_errors:
        for path, message in parse_errors:
            append("{0} :: {1}".format(path, message))
    else:
        append("-")

    append("")
    return "\n".join(lines)


def render_requirements(inventory) :
    project_root = inventory["project_root"]
    stdlib_modules = inventory["stdlib_modules"]
    resolved_requirements = inventory["resolved_requirements"]
    unresolved_imports = inventory["unresolved_imports"]

    lines = []
    append = lines.append

    append("# Automatically generated by __find_py_imports_v2.py")
    append("# project-root: {0}".format(project_root))
    append("# python-executable: {0}".format(sys.executable))
    append("# python-version: {0}".format(sys.version.split()[0]))
    append("# generated-at-utc: {0}".format(datetime.now(timezone.utc).replace(microsecond=0).isoformat()))
    append("")

    append("# Standard library imports - ignored by __auto_install_package_v2.py unless --upgrade_std is used.")
    if stdlib_modules:
        for module_name in stdlib_modules:
            append("{0} {1}".format(STDLIB_SECTION_PREFIX, module_name))
    else:
        append("# stdlib: -")
    append("")

    append("# Third-party distributions")
    if resolved_requirements:
        for requirement in resolved_requirements:
            append(requirement)
    else:
        append("# none")
    append("")

    append("# Unresolved third-party imports - manual package mapping may be required.")
    if unresolved_imports:
        for module_name in sorted(unresolved_imports):
            append("{0} {1} ({2})".format(UNRESOLVED_SECTION_PREFIX, module_name, unresolved_imports[module_name]))
    else:
        append("# unresolved-import: -")

    append("")
    return "\n".join(lines)


def parse_args(argv) :
    parser = argparse.ArgumentParser(
        description=(
            "Durchsucht rekursiv ein Projekt nach Python-Dateien, analysiert Importe, "
            "kategorisiert sie in eigene Module / Standardbibliothek / Drittanbieter "
            "und erzeugt zusätzlich direkt eine requirements.txt."
        )
    )
    parser.add_argument(
        "-d",
        "--dir",
        default=".",
        help="Projektwurzel, die rekursiv gescannt werden soll.",
    )
    parser.add_argument(
        "-o",
        "--out",
        default="imports_report.txt",
        help="Ausgabedatei für den Analysebericht.",
    )
    parser.add_argument(
        "--requirements-out",
        default="requirements.txt",
        help="Ausgabedatei für die generierte requirements.txt.",
    )
    parser.add_argument(
        "--source-root",
        action="append",
        default=[],
        help=(
            "Optionale Import-Wurzel relativ zur Projektwurzel oder absolut. "
            "Kann mehrfach angegeben werden, z. B. --source-root src --source-root tools."
        ),
    )
    parser.add_argument(
        "--exclude-dir",
        action="append",
        default=[],
        help=(
            "Verzeichnisnamen, die beim rekursiven Scan ignoriert werden sollen. "
            "Mehrfach oder komma-separiert verwendbar."
        ),
    )
    parser.add_argument(
        "--package-map",
        default="",
        help=(
            "Optionale JSON-Datei fuer Importname->Paketname-Mapping, "
            "z. B. {'yaml': 'PyYAML', 'cv2': 'opencv-python'}."
        ),
    )
    parser.add_argument(
        "--no-dynamic-imports",
        action="store_true",
        help="Einfache dynamische Imports via __import__()/importlib.import_module() nicht erkennen.",
    )
    parser.add_argument(
        "--no-pin-versions",
        action="store_true",
        help="Drittanbieter-Pakete ohne exakte installierte Version in requirements.txt ausgeben.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Mit Exit-Code 1 beenden, falls Parse-Fehler oder ungeloeste Drittanbieter-Imports gefunden werden.",
    )
    return parser.parse_args(list(argv))


def merge_ignored_dirs(raw_exclude_dirs) :
    ignored_dirs = set(DEFAULT_IGNORED_DIRS)
    ignored_dirs.update(split_csv_values(raw_exclude_dirs))
    return ignored_dirs


def main(argv) :
    args = parse_args(argv)
    eprint = functools.partial(print, file=sys.stderr)

    project_root = Path(args.dir).resolve()
    report_path = Path(args.out).resolve()
    requirements_path = Path(args.requirements_out).resolve()
    package_map_path = Path(args.package_map).resolve() if args.package_map else None

    if not project_root.exists() or not project_root.is_dir():
        eprint("Fehler: Projektverzeichnis nicht gefunden oder kein Verzeichnis: {0}".format(project_root))
        return 2

    source_roots = build_source_roots(project_root, args.source_root)
    ignored_dirs = merge_ignored_dirs(args.exclude_dir)

    try:
        package_map = load_package_map(package_map_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        eprint("Fehler beim Laden der Package-Map: {0}: {1}".format(type(exc).__name__, exc))
        return 2

    inventory = collect_import_inventory(
        project_root=project_root,
        source_roots=source_roots,
        ignored_dirs=ignored_dirs,
        detect_dynamic_imports=not bool(args.no_dynamic_imports),
        package_map=package_map,
        pin_versions=not bool(args.no_pin_versions),
    )

    report_text = render_report(inventory)
    requirements_text = render_requirements(inventory)

    try:
        write_text(report_path, report_text)
        write_text(requirements_path, requirements_text)
    except OSError as exc:
        eprint("Fehler beim Schreiben der Ausgabedateien: {0}: {1}".format(type(exc).__name__, exc))
        return 2

    own_count = len(inventory["own_modules"])
    std_count = len(inventory["stdlib_modules"])
    third_count = len(inventory["third_party_modules"])
    req_count = len(inventory["resolved_requirements"])
    unresolved_count = len(inventory["unresolved_imports"])
    parse_error_count = len(inventory["parse_errors"])

    print(
        "OK: {0} Python-Dateien analysiert | eigene={1} | stdlib={2} | drittanbieter={3} | "
        "requirements={4} | unaufgeloest={5} | parse-fehler={6}".format(
            len(inventory["py_files"]),
            own_count,
            std_count,
            third_count,
            req_count,
            unresolved_count,
            parse_error_count,
        )
    )
    print("Bericht: {0}".format(report_path))
    print("requirements.txt: {0}".format(requirements_path))

    if parse_error_count:
        eprint("")
        eprint("Warnung: Einige Dateien konnten nicht vollständig geparst werden.")
        for path, message in inventory["parse_errors"]:
            eprint(" - {0}: {1}".format(path, message))

    if unresolved_count:
        eprint("")
        eprint("Warnung: Nicht alle Drittanbieter-Importe konnten auf Paketnamen aufgeloest werden.")
        for module_name in sorted(inventory["unresolved_imports"]):
            eprint(" - {0}: {1}".format(module_name, inventory["unresolved_imports"][module_name]))

    if args.strict and (parse_error_count or unresolved_count):
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
