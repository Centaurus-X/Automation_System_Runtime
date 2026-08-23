# -*- coding: utf-8 -*-

# /tools/_check_code_specific_standards.py


"""
Specific project standards checker for Python source code.

This module evaluates Python files against project-specific development
guidelines. The checker is intentionally split into three categories:

- hard rules: directly machine-checkable syntax or AST rules
- heuristic rules: useful approximations for quality guidance
- manual review hints: architecture and design aspects that cannot be
  verified generically without domain-specific knowledge

The module is designed for library usage and workflow automation.
It exposes single-file and multi-path interfaces and returns normalized,
JSON-friendly payloads.

Example:

    from _check_code_specific_standards import run_specific_standards

    result = run_specific_standards(
        paths=["src", "tests"],
        guidelines_file="Entwicklungsrichtlinieren.md",
        cwd=".",
    )

    if result["status"] != "passed":
        print(result["summary"]["rule_counts"])

python _check_code_specific_standards.py check --paths src tests --guidelines-file Entwicklungsrichtlinieren.md --pretty

"""

import argparse
import ast
import io
import json
import os
import re
import time
import tokenize
from collections.abc import Mapping
from datetime import datetime, timezone
from functools import partial
from pathlib import Path


MODULE_NAME = "_check_code_specific_standards"
MODULE_VERSION = "2.0.0"
TOOL_NAME = "specific_standards"

STATUS_PASSED = "passed"
STATUS_FINDINGS = "findings"
STATUS_ERROR = "error"

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"
SEVERITY_ADVISORY = "advisory"
DEFAULT_EXCLUDE_PATTERNS = (
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "build",
    "dist",
    "node_modules",
)

DEFAULT_FILE_EXTENSIONS = (".py",)

# These modules use SHA-256 for transport, security, diagnostics or artifact
# integrity. They do not define configuration canonicalization or fingerprints.
FOREIGN_TYPE_VALIDATION_ALLOWLIST = (
    ("/src/interfaces/mqtt_interface.py", "HMAC authentication digest"),
    ("/src/system/_metrics_system.py", "non-config metrics signature"),
    (
        "/src/library/_generic_system_contract_library/protocol_contracts.py",
        "bounded transport chunk checksum",
    ),
    ("/src/library/_generate_ca_signed_certs.py", "certificate material checksum"),
    ("/_tools/build_release_archive.py", "release artifact checksum"),
    ("/_tools/convert_tia_openness_xml.py", "source artifact checksum"),
)

CONTROL_FLOW_NODE_TYPES = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.Try,
    ast.With,
    ast.AsyncWith,
)

BRANCH_NODE_TYPES = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.Try,
    ast.IfExp,
)

_MATCH_NODE_TYPE = getattr(ast, "Match", None)
if _MATCH_NODE_TYPE is not None:
    CONTROL_FLOW_NODE_TYPES = CONTROL_FLOW_NODE_TYPES + (_MATCH_NODE_TYPE,)
    BRANCH_NODE_TYPES = BRANCH_NODE_TYPES + (_MATCH_NODE_TYPE,)

_TRY_STAR_NODE_TYPE = getattr(ast, "TryStar", None)
if _TRY_STAR_NODE_TYPE is not None:
    CONTROL_FLOW_NODE_TYPES = CONTROL_FLOW_NODE_TYPES + (_TRY_STAR_NODE_TYPE,)
    BRANCH_NODE_TYPES = BRANCH_NODE_TYPES + (_TRY_STAR_NODE_TYPE,)

GERMAN_HINT_WORDS = {
    "aber",
    "alle",
    "alles",
    "auch",
    "bitte",
    "dann",
    "das",
    "dem",
    "den",
    "der",
    "des",
    "die",
    "ein",
    "eine",
    "einer",
    "einen",
    "einem",
    "eines",
    "erweiterbar",
    "fehler",
    "funktion",
    "funktionen",
    "genau",
    "gut",
        "hier",
    "ist",
    "keine",
    "klar",
    "leicht",
    "muss",
    "müssen",
    "nicht",
    "nur",
    "oder",
    "prägnant",
        "soll",
    "sollen",
        "und",
    "verwende",
    "werden",
    "wichtig",
}

RULE_DEFINITIONS = {
    "no_classes": {
        "code": "SPEC001",
        "severity": SEVERITY_ERROR,
        "kind": "syntax_rule",
        "title": "No classes",
        "message": "Classes are forbidden by the active development guidelines.",
    },
    "no_lambda": {
        "code": "SPEC002",
        "severity": SEVERITY_ERROR,
        "kind": "syntax_rule",
        "title": "No lambda",
        "message": "Lambda expressions are forbidden. Use named functions or functools.partial.",
    },
    "no_decorators": {
        "code": "SPEC003",
        "severity": SEVERITY_ERROR,
        "kind": "syntax_rule",
        "title": "No decorators",
        "message": "Decorators are forbidden by the active development guidelines.",
    },
    "no_future_annotations": {
        "code": "SPEC004",
        "severity": SEVERITY_ERROR,
        "kind": "syntax_rule",
        "title": "No __future__ annotations import",
        "message": "The import 'from __future__ import annotations' is forbidden.",
    },
    "no_at_operator": {
        "code": "SPEC005",
        "severity": SEVERITY_ERROR,
        "kind": "syntax_rule",
        "title": "No @ operator",
        "message": "The '@' operator is forbidden by the active development guidelines.",
    },
    "foreign_type_validation": {
        "code": "SPEC006",
        "severity": SEVERITY_ERROR,
        "kind": "architecture_rule",
        "title": "Absolute Type Authority",
        "message": "Schema, canonical config and fingerprint logic must use the Type Authority.",
    },
    "single_library_root": {
        "code": "SPEC007",
        "severity": SEVERITY_ERROR,
        "kind": "architecture_rule",
        "title": "Single library root",
        "message": "Only src/library and canonical src.library imports are permitted.",
    },
    "forbid_epoch_stale": {
        "code": "SPEC008",
        "severity": SEVERITY_ERROR,
        "kind": "contract_rule",
        "title": "Unambiguous epoch conflict code",
        "message": "The ambiguous generic epoch conflict code is forbidden.",
    },
    "require_type_hints": {
        "code": "SPEC101",
        "severity": SEVERITY_WARNING,
        "kind": "heuristic",
        "title": "Type hints required",
        "message": "Public functions should provide parameter and return annotations.",
    },
    "english_comments_only": {
        "code": "SPEC102",
        "severity": SEVERITY_ADVISORY,
        "kind": "heuristic",
        "title": "English comments only",
        "message": "Comments should stay short, precise and written in English.",
    },
    "require_try_except_in_functions": {
        "code": "SPEC103",
        "severity": SEVERITY_WARNING,
        "kind": "heuristic",
        "title": "Try-except expected",
        "message": "Functions above the configured size should contain explicit try-except handling.",
    },
    "max_function_length": {
        "code": "SPEC104",
        "severity": SEVERITY_WARNING,
        "kind": "heuristic",
        "title": "Function length",
        "message": "Function length exceeds the configured maximum.",
    },
    "max_branch_count": {
        "code": "SPEC105",
        "severity": SEVERITY_WARNING,
        "kind": "heuristic",
        "title": "Branch count",
        "message": "Function branching exceeds the configured maximum.",
    },
    "max_nesting_depth": {
        "code": "SPEC106",
        "severity": SEVERITY_WARNING,
        "kind": "heuristic",
        "title": "Nesting depth",
        "message": "Function nesting depth exceeds the configured maximum.",
    },
}

MANUAL_CHECK_DEFINITIONS = {
    "layered_architecture": {
        "code": "MANUAL001",
        "title": "Layered architecture boundaries",
        "message": "Review whether modules separate domain, application and infrastructure responsibilities cleanly.",
    },
    "hexagonal_boundaries": {
        "code": "MANUAL002",
        "title": "Clean or hexagonal boundaries",
        "message": "Review whether side effects and external integrations are isolated behind explicit boundaries.",
    },
    "event_handler_flow": {
        "code": "MANUAL003",
        "title": "Event handler registration and handling",
        "message": "Review whether event registration and event handling flow are explicit, coherent and testable.",
    },
    "data_consistency": {
        "code": "MANUAL004",
        "title": "Data consistency",
        "message": "Review whether state transitions and data validation protect consistency across layers or events.",
    },
    "function_sorting": {
        "code": "MANUAL005",
        "title": "Function grouping and ordering",
        "message": "Review whether functions are grouped by responsibility and ordered clearly by context.",
    },
    "dynamic_extensibility": {
        "code": "MANUAL006",
        "title": "Dynamic extensibility",
        "message": "Review whether extension points stay functional, generic and easy to wire into automation workflows.",
    },
    "sync_async_fit": {
        "code": "MANUAL007",
        "title": "Sync or async fit",
        "message": "Review whether synchronous and asynchronous flows are chosen intentionally per use case.",
    },
}

GUIDELINE_RULE_HINTS = {
    "no_classes": ("oop/classes", "kein oop", "keine klassen", "classes"),
    "no_lambda": ("keine lambda", "lambda funktionen", "functools.partial"),
    "no_decorators": ("decorators", "@ decorator", "@ decorators", "@ dataclass", "@ classmethod", "@ staticmethod"),
    "no_future_annotations": ("from __future__ import annotations",),
    "no_at_operator": ("@ operatoren", '"@" operatoren', " @ "),
    "foreign_type_validation": ("type authority", "fremd-validierung", "fingerprint-berechnung"),
    "single_library_root": ("single library root", "src/libraries", "src.libraries"),
    "forbid_epoch_stale": ("conflict codes", "eindeutigen konfliktcodes"),
    "require_type_hints": ("type hinting", "_type_notations", "_type_dynamics"),
    "english_comments_only": ("comments in english", "kommentare in englisch", "comment style in english"),
    "require_try_except_in_functions": ("try&exception", "try&exception blöcke", "try&exception blöcken"),
    "max_function_length": ("single responsibility", "geringe komplexität", "clean code"),
    "max_branch_count": ("geringe komplexität", "clean code"),
    "max_nesting_depth": ("geringe komplexität", "clean code"),
}

GUIDELINE_MANUAL_HINTS = {
    "layered_architecture": ("layered architecture", "layered-architecture", "layered architecture,", "microservice"),
    "hexagonal_boundaries": ("hexagonal", "clean/hexagonal", "clean code"),
    "event_handler_flow": ("event handler", "handler registration", "eventdriven", "evendriven"),
    "data_consistency": ("datenkonsistenz", "data consistency"),
    "function_sorting": ("sortierung", "kategorisierung", "grouping", "ordered"),
    "dynamic_extensibility": ("dynamisch", "generisch", "erweiterbar", "workflow-automatisierung"),
    "sync_async_fit": ("sync / async", "sync/async", "async"),
}

DEFAULT_PROFILE = {
    "rules": {
        "no_classes": {"enabled": True},
        "no_lambda": {"enabled": True},
        "no_decorators": {"enabled": True},
        "no_future_annotations": {"enabled": True},
        "no_at_operator": {"enabled": True},
        "foreign_type_validation": {"enabled": True},
        "single_library_root": {"enabled": True},
        "forbid_epoch_stale": {"enabled": True},
        "require_type_hints": {
            # The project guidelines forbid the typing module, therefore
            # annotation enforcement stays opt-in instead of a default rule.
            "enabled": False,
            "public_only": True,
        },
        "english_comments_only": {"enabled": True},
        "require_try_except_in_functions": {
            "enabled": True,
            "public_only": True,
            "min_body_statements": 2,
        },
        "max_function_length": {
            "enabled": True,
            "limit": 80,
        },
        "max_branch_count": {
            "enabled": True,
            "limit": 10,
        },
        "max_nesting_depth": {
            "enabled": True,
            "limit": 4,
        },
    },
    "manual_checks": {
        "layered_architecture": {"enabled": True},
        "hexagonal_boundaries": {"enabled": True},
        "event_handler_flow": {"enabled": True},
        "data_consistency": {"enabled": True},
        "function_sorting": {"enabled": True},
        "dynamic_extensibility": {"enabled": True},
        "sync_async_fit": {"enabled": True},
    },
}


def _utc_now_iso() :
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def _coerce_path(value) :
    try:
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, os.PathLike):
            return os.fspath(value)
        return str(value)
    except Exception:
        return str(value)


def _ensure_sequence(value) :
    try:
        if value is None:
            return []
        if isinstance(value, (str, bytes, os.PathLike, Path)):
            return [value]
        return list(value)
    except Exception:
        return [value]


def _normalize_paths(paths) :
    normalized = []
    for item in _ensure_sequence(paths):
        text = _coerce_path(item).strip()
        if text:
            normalized.append(text)
    if not normalized:
        return ["."]
    return normalized


def _normalize_mapping(value) :
    try:
        if isinstance(value, Mapping):
            return dict(value)
        return {}
    except Exception:
        return {}


def _normalize_rule_names(rule_names) :
    names = []
    for item in _ensure_sequence(rule_names):
        text = str(item).strip()
        if not text:
            continue
        if text not in RULE_DEFINITIONS:
            supported = ", ".join(sorted(RULE_DEFINITIONS))
            raise ValueError(f"Unknown rule '{text}'. Supported rules: {supported}")
        names.append(text)
    if not names:
        return list(sorted(RULE_DEFINITIONS))
    return names


def _normalize_exclude_patterns(patterns) :
    values = []
    for item in _ensure_sequence(patterns):
        text = str(item).strip()
        if text:
            values.append(text)
    if not values:
        return list(DEFAULT_EXCLUDE_PATTERNS)
    return values


def _normalize_extensions(extensions) :
    normalized = []
    for item in _ensure_sequence(extensions):
        text = str(item).strip()
        if not text:
            continue
        if not text.startswith("."):
            text = "." + text
        normalized.append(text.lower())
    if not normalized:
        return list(DEFAULT_FILE_EXTENSIONS)
    return normalized


def _stringify_exception(exc) :
    try:
        return f"{type(exc).__name__}: {exc}"
    except Exception:
        return "Unknown exception"


def _deep_copy_json_compatible(value) :
    try:
        return json.loads(json.dumps(value))
    except Exception:
        if isinstance(value, Mapping):
            return {str(key): _deep_copy_json_compatible(sub_value) for key, sub_value in value.items()}
        if isinstance(value, (list, tuple)):
            return [_deep_copy_json_compatible(item) for item in value]
        return value


def _merge_nested_dict(base, override) :
    result = _deep_copy_json_compatible(base)
    for key, value in dict(override).items():
        if isinstance(result.get(key), Mapping) and isinstance(value, Mapping):
            result[key] = _merge_nested_dict(result[key], value)
        else:
            result[key] = _deep_copy_json_compatible(value)
    return result


def _read_text_file(path) :
    target = Path(_coerce_path(path))
    return target.read_text(encoding="utf-8")


def _safe_read_text_file(path) :
    try:
        return _read_text_file(path), None
    except Exception as exc:
        return None, {
            "type": "read_error",
            "message": _stringify_exception(exc),
            "path": _coerce_path(path),
        }


def _path_is_excluded(path, exclude_patterns) :
    try:
        text = str(path).replace("\\", "/")
        name = path.name
        for pattern in exclude_patterns:
            normalized = pattern.replace("\\", "/")
            if not normalized:
                continue
            if normalized in text:
                return True
            if name == normalized:
                return True
        return False
    except Exception:
        return False


def _relative_display_path(path, cwd) :
    try:
        if cwd is None:
            return str(path)
        return str(path.relative_to(cwd))
    except Exception:
        return str(path)


def collect_python_files(
    paths = None,
    *,
    cwd = None,
    extensions = None,
    exclude_patterns = None,
) :
    base_dir = Path(_coerce_path(cwd)).resolve() if cwd else None
    normalized_paths = _normalize_paths(paths)
    normalized_extensions = set(_normalize_extensions(extensions))
    normalized_excludes = _normalize_exclude_patterns(exclude_patterns)

    discovered = []
    seen = set()

    for item in normalized_paths:
        try:
            raw_path = Path(item)
            current = raw_path if raw_path.is_absolute() else ((base_dir / raw_path) if base_dir else raw_path)
            current = current.resolve()
        except Exception:
            continue

        if not current.exists():
            continue

        if current.is_file():
            if current.suffix.lower() in normalized_extensions and not _path_is_excluded(current, normalized_excludes):
                text = str(current)
                if text not in seen:
                    seen.add(text)
                    discovered.append(text)
            continue

        for root, dirnames, filenames in os.walk(current):
            current_root = Path(root)
            dirnames[:] = [name for name in dirnames if not _path_is_excluded(current_root / name, normalized_excludes)]
            for filename in filenames:
                candidate = current_root / filename
                if candidate.suffix.lower() not in normalized_extensions:
                    continue
                if _path_is_excluded(candidate, normalized_excludes):
                    continue
                text = str(candidate.resolve())
                if text in seen:
                    continue
                seen.add(text)
                discovered.append(text)

    discovered.sort()
    return discovered


def _normalize_guidelines_text(text) :
    normalized = text.lower()
    normalized = normalized.replace("ä", "ae")
    normalized = normalized.replace("ö", "oe")
    normalized = normalized.replace("ü", "ue")
    normalized = normalized.replace("ß", "ss")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _load_guidelines_metadata(guidelines_file = None) :
    if not guidelines_file:
        return {
            "path": None,
            "loaded": False,
            "normalized_text": "",
            "matched_rule_ids": [],
            "matched_manual_check_ids": [],
        }

    text, error = _safe_read_text_file(guidelines_file)
    if error is not None or text is None:
        return {
            "path": _coerce_path(guidelines_file),
            "loaded": False,
            "normalized_text": "",
            "matched_rule_ids": [],
            "matched_manual_check_ids": [],
            "error": error,
        }

    normalized = _normalize_guidelines_text(text)
    matched_rule_ids = []
    matched_manual_ids = []

    for rule_id, patterns in GUIDELINE_RULE_HINTS.items():
        if any(_normalize_guidelines_text(pattern) in normalized for pattern in patterns):
            matched_rule_ids.append(rule_id)

    for manual_id, patterns in GUIDELINE_MANUAL_HINTS.items():
        if any(_normalize_guidelines_text(pattern) in normalized for pattern in patterns):
            matched_manual_ids.append(manual_id)

    return {
        "path": _coerce_path(guidelines_file),
        "loaded": True,
        "normalized_text": normalized,
        "matched_rule_ids": matched_rule_ids,
        "matched_manual_check_ids": matched_manual_ids,
    }


def build_profile(
    *,
    guidelines_file = None,
    profile_overrides = None,
    rule_overrides = None,
    manual_check_overrides = None,
) :
    try:
        base_profile = _deep_copy_json_compatible(DEFAULT_PROFILE)
        guidelines = _load_guidelines_metadata(guidelines_file)
        merged_profile = _merge_nested_dict(base_profile, _normalize_mapping(profile_overrides))

        if guidelines.get("loaded"):
            matched_rules = set(guidelines.get("matched_rule_ids", []))
            matched_manual = set(guidelines.get("matched_manual_check_ids", []))
            for rule_id in RULE_DEFINITIONS:
                if matched_rules:
                    merged_profile["rules"].setdefault(rule_id, {})
                    merged_profile["rules"][rule_id]["enabled"] = rule_id in matched_rules
            for manual_id in MANUAL_CHECK_DEFINITIONS:
                if matched_manual:
                    merged_profile["manual_checks"].setdefault(manual_id, {})
                    merged_profile["manual_checks"][manual_id]["enabled"] = manual_id in matched_manual

        merged_profile["rules"] = _merge_nested_dict(
            merged_profile.get("rules", {}),
            _normalize_mapping(rule_overrides),
        )
        merged_profile["manual_checks"] = _merge_nested_dict(
            merged_profile.get("manual_checks", {}),
            _normalize_mapping(manual_check_overrides),
        )
        merged_profile["guidelines"] = {
            "path": guidelines.get("path"),
            "loaded": guidelines.get("loaded", False),
            "matched_rule_ids": list(guidelines.get("matched_rule_ids", [])),
            "matched_manual_check_ids": list(guidelines.get("matched_manual_check_ids", [])),
            "error": guidelines.get("error"),
        }
        return merged_profile
    except Exception as exc:
        return {
            "rules": _deep_copy_json_compatible(DEFAULT_PROFILE["rules"]),
            "manual_checks": _deep_copy_json_compatible(DEFAULT_PROFILE["manual_checks"]),
            "guidelines": {
                "path": _coerce_path(guidelines_file) if guidelines_file else None,
                "loaded": False,
                "matched_rule_ids": [],
                "matched_manual_check_ids": [],
                "error": {
                    "type": "profile_error",
                    "message": _stringify_exception(exc),
                },
            },
        }

def get_module_metadata() :
    try:
        return {
            "module": MODULE_NAME,
            "version": MODULE_VERSION,
            "tool": TOOL_NAME,
            "rules": list(sorted(RULE_DEFINITIONS)),
            "manual_checks": list(sorted(MANUAL_CHECK_DEFINITIONS)),
        }
    except Exception as exc:
        return {
            "module": MODULE_NAME,
            "version": MODULE_VERSION,
            "tool": TOOL_NAME,
            "error": {
                "type": "metadata_error",
                "message": _stringify_exception(exc),
            },
        }


def _iter_comment_tokens(source_text) :
    buffer = io.StringIO(source_text)
    tokens = []
    for token in tokenize.generate_tokens(buffer.readline):
        if token.type == tokenize.COMMENT:
            tokens.append(token)
    return tokens


def _extract_comment_text(comment_token) :
    text = comment_token.string.strip()
    if text.startswith("#"):
        text = text[1:].strip()
    return text


def _should_ignore_comment(comment_text) :
    lowered = comment_text.lower()
    if not lowered:
        return True
    if lowered.startswith("!") or lowered.startswith("-*-"):
        return True
    if lowered.startswith("noqa") or lowered.startswith("type:") or lowered.startswith("pragma:"):
        return True
    if "http://" in lowered or "https://" in lowered:
        return True
    return False


def _looks_like_non_english_comment(comment_text) :
    normalized = comment_text.strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    if any(character in lowered for character in ("ä", "ö", "ü", "ß")):
        return True
    words = re.findall(r"[A-Za-zÄÖÜäöüß]+", lowered)
    if not words:
        return False
    hits = 0
    for word in words:
        if word in GERMAN_HINT_WORDS:
            hits += 1
    return hits >= 2


def _iter_function_nodes(module_ast) :
    return [node for node in ast.walk(module_ast) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]


def _iter_class_nodes(module_ast) :
    return [node for node in ast.walk(module_ast) if isinstance(node, ast.ClassDef)]


def _iter_lambda_nodes(module_ast) :
    return [node for node in ast.walk(module_ast) if isinstance(node, ast.Lambda)]


def _iter_decorated_nodes(module_ast) :
    candidates = []
    for node in ast.walk(module_ast):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and getattr(node, "decorator_list", None):
            candidates.append(node)
    return candidates


def _iter_future_annotation_imports(module_ast) :
    imports = []
    for node in ast.walk(module_ast):
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            if any(alias.name == "annotations" for alias in node.names):
                imports.append(node)
    return imports


def _iter_at_operator_nodes(module_ast) :
    nodes = []
    for node in ast.walk(module_ast):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.MatMult):
            nodes.append(node)
        elif isinstance(node, ast.AugAssign) and isinstance(node.op, ast.MatMult):
            nodes.append(node)
    return nodes


def _normalized_source_path(source_name) :
    try:
        return "/" + str(source_name).replace("\\", "/").lower().lstrip("/")
    except Exception:
        return "/"


def _is_type_authority_source(source_name) :
    normalized = _normalized_source_path(source_name)
    return normalized.endswith("/src/library/_generic_type_library.py")


def _foreign_type_validation_allowance(source_name) :
    normalized = _normalized_source_path(source_name)
    for suffix, reason in FOREIGN_TYPE_VALIDATION_ALLOWLIST:
        if normalized.endswith(suffix):
            return reason
    return ""


def _assignment_target_names(target) :
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        names = []
        for item in target.elts:
            names.extend(_assignment_target_names(item))
        return names
    return []


def _attribute_name(node) :
    parts = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


def _iter_foreign_type_validation_findings(module_ast, source_name) :
    if _is_type_authority_source(source_name):
        return []
    allowance = _foreign_type_validation_allowance(source_name)
    findings = []
    canonical_names = frozenset((
        "canonicalize_config_for_fingerprint",
        "_canonicalize_config_for_fingerprint",
        "_compat_canonicalize_config_for_fingerprint",
    ))
    for node in ast.walk(module_ast):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "jsonschema" or alias.name.startswith("jsonschema."):
                    findings.append({"node": node, "reason": "jsonschema_import"})
        elif isinstance(node, ast.ImportFrom):
            module_name = str(node.module or "")
            if module_name == "jsonschema" or module_name.startswith("jsonschema."):
                findings.append({"node": node, "reason": "jsonschema_import"})
            if module_name == "hashlib" and not allowance:
                for alias in node.names:
                    if alias.name == "sha256":
                        findings.append({"node": node, "reason": "foreign_sha256"})
        elif isinstance(node, ast.Assign):
            names = []
            for target in node.targets:
                names.extend(_assignment_target_names(target))
            if "CONFIG_SCHEMA_REGISTRY" in names:
                findings.append({"node": node, "reason": "config_schema_registry"})
        elif isinstance(node, ast.AnnAssign):
            if "CONFIG_SCHEMA_REGISTRY" in _assignment_target_names(node.target):
                findings.append({"node": node, "reason": "config_schema_registry"})
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in canonical_names:
                findings.append({"node": node, "reason": "foreign_config_canonicalizer"})
        elif isinstance(node, ast.Attribute):
            if _attribute_name(node) == "hashlib.sha256" and not allowance:
                findings.append({"node": node, "reason": "foreign_sha256"})
    return findings


def _iter_single_library_root_findings(module_ast, source_name) :
    findings = []
    normalized = _normalized_source_path(source_name)
    if "/src/libraries/" in normalized:
        findings.append({"node": module_ast, "reason": "physical_src_libraries"})
    for node in ast.walk(module_ast):
        modules = []
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = [str(node.module or "")]
        for module_name in modules:
            if (
                module_name == "src.libraries"
                or module_name.startswith("src.libraries.")
                or module_name == "libraries"
                or module_name.startswith("libraries.")
                or module_name == "library"
                or module_name.startswith("library.")
            ):
                findings.append({"node": node, "reason": "noncanonical_library_import"})
    return findings


def _iter_forbidden_epoch_code_nodes(module_ast) :
    forbidden = "epoch" + "_stale"
    return [
        node for node in ast.walk(module_ast)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value == forbidden
    ]


def _is_public_function(node) :
    try:
        name = getattr(node, "name", "")
        return bool(name) and not str(name).startswith("_")
    except Exception:
        return False


def _iter_function_arguments(node) :
    try:
        args = []
        arguments = getattr(node, "args", None)
        if arguments is None:
            return args
        args.extend(list(arguments.posonlyargs))
        args.extend(list(arguments.args))
        args.extend(list(arguments.kwonlyargs))
        if arguments.vararg is not None:
            args.append(arguments.vararg)
        if arguments.kwarg is not None:
            args.append(arguments.kwarg)
        return args
    except Exception:
        return []


def _function_body_statement_count(node) :
    try:
        return len(list(getattr(node, "body", [])))
    except Exception:
        return 0


def _function_contains_try(node) :
    try:
        stack = list(getattr(node, "body", []))
        while stack:
            current = stack.pop()
            if isinstance(current, (ast.Try,) + ((_TRY_STAR_NODE_TYPE,) if _TRY_STAR_NODE_TYPE is not None else ())):
                return True
            if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            stack.extend(list(ast.iter_child_nodes(current)))
        return False
    except Exception:
        return False


def _function_length(node) :
    try:
        start_line = int(getattr(node, "lineno", 0) or 0)
        end_line = int(getattr(node, "end_lineno", start_line) or start_line)
        return max(0, end_line - start_line + 1)
    except Exception:
        return 0


def _function_branch_count(node) :
    count = 0
    stack = list(getattr(node, "body", []))
    while stack:
        current = stack.pop()
        if isinstance(current, BRANCH_NODE_TYPES):
            count += 1
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        stack.extend(list(ast.iter_child_nodes(current)))
    return count


def _function_nesting_depth(node) :
    def walk(current, depth) :
        current_depth = depth + 1 if isinstance(current, CONTROL_FLOW_NODE_TYPES) else depth
        best_depth = current_depth
        for child in ast.iter_child_nodes(current):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            candidate = walk(child, current_depth)
            if candidate > best_depth:
                best_depth = candidate
        return best_depth

    try:
        best = 0
        for child in getattr(node, "body", []):
            candidate = walk(child, 0)
            if candidate > best:
                best = candidate
        return best
    except Exception:
        return 0


def _module_name_from_path(display_path) :
    try:
        path = Path(display_path)
        if path.suffix:
            path = path.with_suffix("")
        parts = [part for part in path.parts if part not in ("", ".")]
        return ".".join(parts)
    except Exception:
        return display_path


def _make_finding(
    *,
    rule_id,
    path,
    line,
    column,
    message = None,
    object_name = None,
    module_name = None,
    extra = None,
) :
    definition = RULE_DEFINITIONS[rule_id]
    return {
        "tool": TOOL_NAME,
        "rule_id": rule_id,
        "path": path,
        "line": line,
        "column": column,
        "end_line": None,
        "end_column": None,
        "code": definition["code"],
        "symbol": rule_id,
        "message": message or definition["message"],
        "severity": definition["severity"],
        "confidence": None,
        "object": object_name,
        "module": module_name,
        "kind": definition["kind"],
        "fixable": False,
        "extra": dict(extra or {}),
    }


def _make_parse_error_finding(path, syntax_error, module_name = None) :
    message = _stringify_exception(syntax_error)
    return {
        "tool": TOOL_NAME,
        "rule_id": "parse_error",
        "path": path,
        "line": getattr(syntax_error, "lineno", None),
        "column": getattr(syntax_error, "offset", None),
        "end_line": None,
        "end_column": None,
        "code": "SPEC900",
        "symbol": "parse_error",
        "message": message,
        "severity": SEVERITY_ERROR,
        "confidence": None,
        "object": None,
        "module": module_name,
        "kind": "parse_error",
        "fixable": False,
        "extra": {},
    }


def _rule_enabled(profile, rule_id) :
    try:
        rule_config = dict(profile.get("rules", {}).get(rule_id, {}))
        return bool(rule_config.get("enabled", False))
    except Exception:
        return False


def _manual_check_enabled(profile, manual_check_id) :
    try:
        manual_config = dict(profile.get("manual_checks", {}).get(manual_check_id, {}))
        return bool(manual_config.get("enabled", False))
    except Exception:
        return False


def _profile_rule_config(profile, rule_id) :
    try:
        return dict(profile.get("rules", {}).get(rule_id, {}))
    except Exception:
        return {}


def analyze_source_text(
    source_text,
    *,
    source_name = "<memory>",
    profile = None,
) :
    started_at = _utc_now_iso()
    started_perf = time.perf_counter()
    active_profile = build_profile() if profile is None else _deep_copy_json_compatible(profile)

    findings = []
    file_errors = []
    module_name = _module_name_from_path(source_name)

    try:
        module_ast = ast.parse(source_text, filename=source_name)
    except SyntaxError as exc:
        findings.append(_make_parse_error_finding(source_name, exc, module_name=module_name))
        file_errors.append({
            "type": "syntax_error",
            "message": _stringify_exception(exc),
        })
        ended_at = _utc_now_iso()
        duration_seconds = round(time.perf_counter() - started_perf, 6)
        return {
            "module": MODULE_NAME,
            "tool": TOOL_NAME,
            "path": source_name,
            "module_name": module_name,
            "status": STATUS_ERROR,
            "execution_ok": True,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "finding_count": len(findings),
            "findings": findings,
            "summary": {
                "rule_counts": {"parse_error": 1},
                "severity_counts": {SEVERITY_ERROR: 1},
                "file_errors": file_errors,
            },
            "error": None,
        }
    except Exception as exc:
        ended_at = _utc_now_iso()
        duration_seconds = round(time.perf_counter() - started_perf, 6)
        return {
            "module": MODULE_NAME,
            "tool": TOOL_NAME,
            "path": source_name,
            "module_name": module_name,
            "status": STATUS_ERROR,
            "execution_ok": False,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "finding_count": 0,
            "findings": [],
            "summary": {
                "rule_counts": {},
                "severity_counts": {},
                "file_errors": [],
            },
            "error": {
                "type": "analysis_error",
                "message": _stringify_exception(exc),
            },
        }

    if _rule_enabled(active_profile, "no_classes"):
        for node in _iter_class_nodes(module_ast):
            findings.append(
                _make_finding(
                    rule_id="no_classes",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    object_name=getattr(node, "name", None),
                    module_name=module_name,
                )
            )

    if _rule_enabled(active_profile, "no_lambda"):
        for node in _iter_lambda_nodes(module_ast):
            findings.append(
                _make_finding(
                    rule_id="no_lambda",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    module_name=module_name,
                )
            )

    if _rule_enabled(active_profile, "no_decorators"):
        for node in _iter_decorated_nodes(module_ast):
            decorator_names = []
            for decorator in getattr(node, "decorator_list", []):
                decorator_names.append(ast.unparse(decorator) if hasattr(ast, "unparse") else type(decorator).__name__)
            findings.append(
                _make_finding(
                    rule_id="no_decorators",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    object_name=getattr(node, "name", None),
                    module_name=module_name,
                    extra={"decorators": decorator_names},
                )
            )

    if _rule_enabled(active_profile, "no_future_annotations"):
        for node in _iter_future_annotation_imports(module_ast):
            findings.append(
                _make_finding(
                    rule_id="no_future_annotations",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    module_name=module_name,
                )
            )

    if _rule_enabled(active_profile, "no_at_operator"):
        for node in _iter_at_operator_nodes(module_ast):
            findings.append(
                _make_finding(
                    rule_id="no_at_operator",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    module_name=module_name,
                    extra={"node_type": type(node).__name__},
                )
            )

    if _rule_enabled(active_profile, "foreign_type_validation"):
        for item in _iter_foreign_type_validation_findings(module_ast, source_name):
            node = item["node"]
            findings.append(
                _make_finding(
                    rule_id="foreign_type_validation",
                    path=source_name,
                    line=getattr(node, "lineno", 1),
                    column=getattr(node, "col_offset", 0),
                    module_name=module_name,
                    extra={"reason": item["reason"]},
                )
            )

    if _rule_enabled(active_profile, "single_library_root"):
        for item in _iter_single_library_root_findings(module_ast, source_name):
            node = item["node"]
            findings.append(
                _make_finding(
                    rule_id="single_library_root",
                    path=source_name,
                    line=getattr(node, "lineno", 1),
                    column=getattr(node, "col_offset", 0),
                    module_name=module_name,
                    extra={"reason": item["reason"]},
                )
            )

    if _rule_enabled(active_profile, "forbid_epoch_stale"):
        for node in _iter_forbidden_epoch_code_nodes(module_ast):
            findings.append(
                _make_finding(
                    rule_id="forbid_epoch_stale",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    module_name=module_name,
                )
            )

    function_nodes = _iter_function_nodes(module_ast)

    if _rule_enabled(active_profile, "require_type_hints"):
        rule_config = _profile_rule_config(active_profile, "require_type_hints")
        public_only = bool(rule_config.get("public_only", True))
        for node in function_nodes:
            if public_only and not _is_public_function(node):
                continue

            missing_arguments = []
            for argument in _iter_function_arguments(node):
                if argument.annotation is None:
                    missing_arguments.append(argument.arg)

            missing_return = getattr(node, "returns", None) is None

            if missing_arguments or missing_return:
                findings.append(
                    _make_finding(
                        rule_id="require_type_hints",
                        path=source_name,
                        line=getattr(node, "lineno", None),
                        column=getattr(node, "col_offset", None),
                        object_name=getattr(node, "name", None),
                        module_name=module_name,
                        extra={
                            "missing_arguments": missing_arguments,
                            "missing_return_annotation": missing_return,
                        },
                        message="Public functions should define complete parameter and return annotations.",
                    )
                )

    if _rule_enabled(active_profile, "require_try_except_in_functions"):
        rule_config = _profile_rule_config(active_profile, "require_try_except_in_functions")
        public_only = bool(rule_config.get("public_only", True))
        min_body_statements = int(rule_config.get("min_body_statements", 2) or 0)
        for node in function_nodes:
            if public_only and not _is_public_function(node):
                continue
            if _function_body_statement_count(node) < min_body_statements:
                continue
            if _function_contains_try(node):
                continue
            findings.append(
                _make_finding(
                    rule_id="require_try_except_in_functions",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    object_name=getattr(node, "name", None),
                    module_name=module_name,
                    extra={"body_statement_count": _function_body_statement_count(node)},
                )
            )

    if _rule_enabled(active_profile, "max_function_length"):
        rule_config = _profile_rule_config(active_profile, "max_function_length")
        limit = int(rule_config.get("limit", 80) or 80)
        for node in function_nodes:
            length = _function_length(node)
            if length <= limit:
                continue
            findings.append(
                _make_finding(
                    rule_id="max_function_length",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    object_name=getattr(node, "name", None),
                    module_name=module_name,
                    extra={"length": length, "limit": limit},
                    message=f"Function length is {length} lines and exceeds the limit {limit}.",
                )
            )

    if _rule_enabled(active_profile, "max_branch_count"):
        rule_config = _profile_rule_config(active_profile, "max_branch_count")
        limit = int(rule_config.get("limit", 10) or 10)
        for node in function_nodes:
            branch_count = _function_branch_count(node)
            if branch_count <= limit:
                continue
            findings.append(
                _make_finding(
                    rule_id="max_branch_count",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    object_name=getattr(node, "name", None),
                    module_name=module_name,
                    extra={"branch_count": branch_count, "limit": limit},
                    message=f"Function branch count is {branch_count} and exceeds the limit {limit}.",
                )
            )

    if _rule_enabled(active_profile, "max_nesting_depth"):
        rule_config = _profile_rule_config(active_profile, "max_nesting_depth")
        limit = int(rule_config.get("limit", 4) or 4)
        for node in function_nodes:
            nesting_depth = _function_nesting_depth(node)
            if nesting_depth <= limit:
                continue
            findings.append(
                _make_finding(
                    rule_id="max_nesting_depth",
                    path=source_name,
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                    object_name=getattr(node, "name", None),
                    module_name=module_name,
                    extra={"nesting_depth": nesting_depth, "limit": limit},
                    message=f"Function nesting depth is {nesting_depth} and exceeds the limit {limit}.",
                )
            )

    if _rule_enabled(active_profile, "english_comments_only"):
        try:
            comment_tokens = _iter_comment_tokens(source_text)
        except Exception as exc:
            file_errors.append({
                "type": "comment_scan_error",
                "message": _stringify_exception(exc),
            })
            comment_tokens = []

        for token in comment_tokens:
            comment_text = _extract_comment_text(token)
            if _should_ignore_comment(comment_text):
                continue
            if not _looks_like_non_english_comment(comment_text):
                continue
            findings.append(
                _make_finding(
                    rule_id="english_comments_only",
                    path=source_name,
                    line=token.start[0],
                    column=token.start[1],
                    module_name=module_name,
                    extra={"comment": comment_text},
                )
            )

    rule_counts = {}
    severity_counts = {}
    for finding in findings:
        rule_id = str(finding.get("rule_id"))
        severity = str(finding.get("severity"))
        rule_counts[rule_id] = rule_counts.get(rule_id, 0) + 1
        severity_counts[severity] = severity_counts.get(severity, 0) + 1

    status = STATUS_PASSED if not findings else STATUS_FINDINGS
    if file_errors:
        status = STATUS_ERROR if any(item.get("type") == "syntax_error" for item in file_errors) else status

    ended_at = _utc_now_iso()
    duration_seconds = round(time.perf_counter() - started_perf, 6)

    return {
        "module": MODULE_NAME,
        "tool": TOOL_NAME,
        "path": source_name,
        "module_name": module_name,
        "status": status,
        "execution_ok": True,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "finding_count": len(findings),
        "findings": findings,
        "summary": {
            "rule_counts": rule_counts,
            "severity_counts": severity_counts,
            "file_errors": file_errors,
        },
        "error": None,
    }


def analyze_python_file(
    file_path,
    *,
    profile = None,
    cwd = None,
) :
    display_path = _coerce_path(file_path)
    try:
        resolved = Path(_coerce_path(file_path))
        if not resolved.is_absolute() and cwd:
            resolved = (Path(_coerce_path(cwd)) / resolved).resolve()
        elif not resolved.is_absolute():
            resolved = resolved.resolve()
        display_path = _relative_display_path(resolved, Path(_coerce_path(cwd)).resolve() if cwd else None)
    except Exception:
        resolved = Path(_coerce_path(file_path))

    source_text, read_error = _safe_read_text_file(resolved)
    if read_error is not None or source_text is None:
        return {
            "module": MODULE_NAME,
            "tool": TOOL_NAME,
            "path": display_path,
            "module_name": _module_name_from_path(display_path),
            "status": STATUS_ERROR,
            "execution_ok": False,
            "started_at": _utc_now_iso(),
            "ended_at": _utc_now_iso(),
            "duration_seconds": 0.0,
            "finding_count": 0,
            "findings": [],
            "summary": {
                "rule_counts": {},
                "severity_counts": {},
                "file_errors": [read_error],
            },
            "error": read_error,
        }

    result = analyze_source_text(
        source_text,
        source_name=display_path,
        profile=profile,
    )
    return result


def summarize_specific_results(
    file_results,
    *,
    profile = None,
    paths = None,
    cwd = None,
    started_at = None,
    ended_at = None,
    duration_seconds = None,
) :
    normalized_results = [dict(item) for item in file_results]
    all_findings = []
    rule_counts = {}
    severity_counts = {}
    status_counts = {
        STATUS_PASSED: 0,
        STATUS_FINDINGS: 0,
        STATUS_ERROR: 0,
    }
    checked_files = 0
    file_error_count = 0

    for result in normalized_results:
        checked_files += 1
        status = str(result.get("status"))
        if status not in status_counts:
            status_counts[status] = 0
        status_counts[status] += 1

        file_errors = list(result.get("summary", {}).get("file_errors", []))
        file_error_count += len(file_errors)

        findings = list(result.get("findings", []))
        all_findings.extend(findings)
        for finding in findings:
            rule_id = str(finding.get("rule_id"))
            severity = str(finding.get("severity"))
            rule_counts[rule_id] = rule_counts.get(rule_id, 0) + 1
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

    overall_status = STATUS_PASSED
    if status_counts.get(STATUS_ERROR, 0) > 0:
        overall_status = STATUS_ERROR
    elif all_findings:
        overall_status = STATUS_FINDINGS

    active_profile = build_profile() if profile is None else _deep_copy_json_compatible(profile)
    active_rules = []
    for rule_id, definition in RULE_DEFINITIONS.items():
        rule_config = _profile_rule_config(active_profile, rule_id)
        if not bool(rule_config.get("enabled", False)):
            continue
        active_rules.append({
            "id": rule_id,
            "code": definition["code"],
            "severity": definition["severity"],
            "kind": definition["kind"],
            "title": definition["title"],
            "config": rule_config,
        })

    manual_checks = []
    for manual_id, definition in MANUAL_CHECK_DEFINITIONS.items():
        if not _manual_check_enabled(active_profile, manual_id):
            continue
        manual_checks.append({
            "id": manual_id,
            "code": definition["code"],
            "title": definition["title"],
            "message": definition["message"],
        })

    return {
        "module": MODULE_NAME,
        "tool": TOOL_NAME,
        "version": MODULE_VERSION,
        "command_source": "internal",
        "command": [MODULE_NAME],
        "command_text": MODULE_NAME,
        "cwd": _coerce_path(cwd) if cwd else None,
        "paths": list(paths or []),
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "status": overall_status,
        "execution_ok": True,
        "passed": overall_status == STATUS_PASSED,
        "has_findings": bool(all_findings),
        "finding_count": len(all_findings),
        "findings": all_findings,
        "summary": {
            "checked_files": checked_files,
            "file_error_count": file_error_count,
            "status_counts": status_counts,
            "rule_counts": rule_counts,
            "severity_counts": severity_counts,
            "active_rules": active_rules,
            "manual_review_items": manual_checks,
            "guidelines": dict(active_profile.get("guidelines", {})),
            "file_results": normalized_results,
        },
        "raw": {
            "profile": active_profile,
            "file_results": normalized_results,
        },
        "error": None,
        "compatibility_warnings": [],
        "returncode": 0 if overall_status == STATUS_PASSED else 1,
    }


def run_specific_rules(
    paths = None,
    *,
    rule_names = None,
    cwd = None,
    guidelines_file = None,
    profile_overrides = None,
    rule_overrides = None,
    manual_check_overrides = None,
    extensions = None,
    exclude_patterns = None,
) :
    active_rule_names = set(_normalize_rule_names(rule_names))
    merged_rule_overrides = _normalize_mapping(rule_overrides)
    for rule_id in RULE_DEFINITIONS:
        merged_rule_overrides.setdefault(rule_id, {})
        merged_rule_overrides[rule_id]["enabled"] = rule_id in active_rule_names

    return run_specific_standards(
        paths=paths,
        cwd=cwd,
        guidelines_file=guidelines_file,
        profile_overrides=profile_overrides,
        rule_overrides=merged_rule_overrides,
        manual_check_overrides=manual_check_overrides,
        extensions=extensions,
        exclude_patterns=exclude_patterns,
    )


def run_specific_standards(
    paths = None,
    *,
    cwd = None,
    guidelines_file = None,
    profile_overrides = None,
    rule_overrides = None,
    manual_check_overrides = None,
    extensions = None,
    exclude_patterns = None,
) :
    started_at = _utc_now_iso()
    started_perf = time.perf_counter()
    normalized_paths = _normalize_paths(paths)
    active_profile = build_profile(
        guidelines_file=guidelines_file,
        profile_overrides=profile_overrides,
        rule_overrides=rule_overrides,
        manual_check_overrides=manual_check_overrides,
    )

    try:
        discovered_files = collect_python_files(
            normalized_paths,
            cwd=cwd,
            extensions=extensions,
            exclude_patterns=exclude_patterns,
        )
    except Exception as exc:
        ended_at = _utc_now_iso()
        duration_seconds = round(time.perf_counter() - started_perf, 6)
        return {
            "module": MODULE_NAME,
            "tool": TOOL_NAME,
            "version": MODULE_VERSION,
            "command_source": "internal",
            "command": [MODULE_NAME],
            "command_text": MODULE_NAME,
            "cwd": _coerce_path(cwd) if cwd else None,
            "paths": normalized_paths,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "status": STATUS_ERROR,
            "execution_ok": False,
            "passed": False,
            "has_findings": False,
            "finding_count": 0,
            "findings": [],
            "summary": {
                "checked_files": 0,
                "file_error_count": 0,
                "status_counts": {STATUS_PASSED: 0, STATUS_FINDINGS: 0, STATUS_ERROR: 1},
                "rule_counts": {},
                "severity_counts": {},
                "active_rules": [],
                "manual_review_items": [],
                "guidelines": dict(active_profile.get("guidelines", {})),
                "file_results": [],
            },
            "raw": {"profile": active_profile},
            "error": {
                "type": "discovery_error",
                "message": _stringify_exception(exc),
            },
            "compatibility_warnings": [],
            "returncode": 2,
        }

    file_results = []
    for file_path in discovered_files:
        file_results.append(
            analyze_python_file(
                file_path,
                profile=active_profile,
                cwd=cwd,
            )
        )

    ended_at = _utc_now_iso()
    duration_seconds = round(time.perf_counter() - started_perf, 6)

    payload = summarize_specific_results(
        file_results,
        profile=active_profile,
        paths=normalized_paths,
        cwd=cwd,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
    )
    if payload["status"] == STATUS_ERROR:
        payload["returncode"] = 2
    elif payload["status"] == STATUS_FINDINGS:
        payload["returncode"] = 1
    else:
        payload["returncode"] = 0
    return payload


def make_specific_runner(**default_kwargs) :
    return partial(run_specific_standards, **default_kwargs)


def normalized_exit_code(
    payload,
    *,
    fail_on_findings = True,
    fail_on_error = True,
) :
    status = payload.get("status")
    if status == STATUS_PASSED:
        return 0
    if status == STATUS_FINDINGS:
        return 1 if fail_on_findings else 0
    if status == STATUS_ERROR:
        return 2 if fail_on_error else 0
    return 2 if fail_on_error else 0


def to_json(payload, *, pretty = False) :
    if pretty:
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False)
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def write_json_report(payload, destination, *, pretty = True) :
    target = Path(_coerce_path(destination))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(to_json(payload, pretty=pretty), encoding="utf-8")
    return str(target)


def _load_json_payload_from_cli(json_text, json_file) :
    if json_text and json_file:
        raise ValueError("Please provide either JSON text or a JSON file, not both.")
    if json_text:
        payload = json.loads(json_text)
        if not isinstance(payload, Mapping):
            raise ValueError("The JSON payload must be an object.")
        return dict(payload)
    if json_file:
        content = Path(json_file).read_text(encoding="utf-8")
        payload = json.loads(content)
        if not isinstance(payload, Mapping):
            raise ValueError("The JSON file payload must be an object.")
        return dict(payload)
    return {}


def _build_parser() :
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Check Python source code against project-specific standards.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_profile = subparsers.add_parser("profile", help="Show active rules and manual review hints.")
    parser_profile.add_argument("--guidelines-file", default=None, help="Guidelines markdown file.")
    parser_profile.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    parser_check = subparsers.add_parser("check", help="Run the specific standards check.")
    parser_check.add_argument("--cwd", default=None, help="Working directory.")
    parser_check.add_argument("--paths", nargs="*", default=["."], help="Files or directories to inspect.")
    parser_check.add_argument("--guidelines-file", default=None, help="Guidelines markdown file.")
    parser_check.add_argument("--extensions", nargs="*", default=list(DEFAULT_FILE_EXTENSIONS), help="Allowed file extensions.")
    parser_check.add_argument("--exclude-patterns", nargs="*", default=list(DEFAULT_EXCLUDE_PATTERNS), help="Excluded paths or path parts.")
    parser_check.add_argument("--profile-overrides-json", default=None, help="Profile overrides as JSON text.")
    parser_check.add_argument("--profile-overrides-file", default=None, help="Profile overrides from JSON file.")
    parser_check.add_argument("--rule-overrides-json", default=None, help="Rule overrides as JSON text.")
    parser_check.add_argument("--rule-overrides-file", default=None, help="Rule overrides from JSON file.")
    parser_check.add_argument("--manual-overrides-json", default=None, help="Manual check overrides as JSON text.")
    parser_check.add_argument("--manual-overrides-file", default=None, help="Manual check overrides from JSON file.")
    parser_check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser_check.add_argument("--write-report", default=None, help="Write the JSON report to a file.")
    parser_check.add_argument("--allow-findings", action="store_true", help="Return exit code 0 even if findings exist.")
    parser_check.add_argument("--allow-errors", action="store_true", help="Return exit code 0 even if analysis errors exist.")

    return parser


def _emit_output(payload, *, pretty, write_report = None) :
    text = to_json(payload, pretty=pretty)
    print(text)
    if write_report:
        write_json_report(payload, write_report, pretty=pretty)


def main(argv = None) :
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "profile":
        profile = build_profile(guidelines_file=args.guidelines_file)
        payload = {
            "module": MODULE_NAME,
            "version": MODULE_VERSION,
            "tool": TOOL_NAME,
            "profile": profile,
        }
        _emit_output(payload, pretty=args.pretty)
        return 0

    if args.command == "check":
        profile_overrides = _load_json_payload_from_cli(args.profile_overrides_json, args.profile_overrides_file)
        rule_overrides = _load_json_payload_from_cli(args.rule_overrides_json, args.rule_overrides_file)
        manual_overrides = _load_json_payload_from_cli(args.manual_overrides_json, args.manual_overrides_file)

        payload = run_specific_standards(
            paths=args.paths,
            cwd=args.cwd,
            guidelines_file=args.guidelines_file,
            profile_overrides=profile_overrides,
            rule_overrides=rule_overrides,
            manual_check_overrides=manual_overrides,
            extensions=args.extensions,
            exclude_patterns=args.exclude_patterns,
        )
        _emit_output(payload, pretty=args.pretty, write_report=args.write_report)
        return normalized_exit_code(
            payload,
            fail_on_findings=not args.allow_findings,
            fail_on_error=not args.allow_errors,
        )

    parser.error("Unknown command.")
    return 2


__all__ = [
    "MODULE_NAME",
    "MODULE_VERSION",
    "TOOL_NAME",
    "RULE_DEFINITIONS",
    "MANUAL_CHECK_DEFINITIONS",
    "DEFAULT_PROFILE",
    "collect_python_files",
    "build_profile",
    "get_module_metadata",
    "analyze_source_text",
    "analyze_python_file",
    "summarize_specific_results",
    "run_specific_rules",
    "run_specific_standards",
    "make_specific_runner",
    "normalized_exit_code",
    "to_json",
    "write_json_report",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
