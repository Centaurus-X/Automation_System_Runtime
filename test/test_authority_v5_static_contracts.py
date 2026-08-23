# -*- coding: utf-8 -*-

"""Executable static acceptance gate for Authority V5 source contracts."""

import os
import sys

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from _tools import _check_code_specific_standards as standards
from src.library import _generic_type_library as type_authority
from src.library import _generic_system_contract_library as contract_authority
SCAN_ROOTS = ("src", "test", "_tools")
HARD_RULES = (
    "no_classes",
    "no_lambda",
    "no_decorators",
    "no_future_annotations",
    "no_at_operator",
    "foreign_type_validation",
    "single_library_root",
    "forbid_epoch_stale",
)
CONTRACT_MODULES = frozenset((
    "__init__.py",
    "module_contracts.py",
    "protocol_contracts.py",
    "persistence_contracts.py",
    "authority_flow_contracts.py",
    "config_sharding_contracts.py",
    "errors.py",
))


def _profile_for(rule_id):
    overrides = {}
    for current_rule in standards.RULE_DEFINITIONS:
        overrides[current_rule] = {"enabled": current_rule == rule_id}
    return standards.build_profile(rule_overrides=overrides)


def _finding_ids(source_text, source_name, rule_id):
    result = standards.analyze_source_text(
        source_text,
        source_name=source_name,
        profile=_profile_for(rule_id),
    )
    return tuple(item["rule_id"] for item in result["findings"])


def _forbidden_epoch_source():
    conflict_code = "_".join(("epoch", "stale"))
    return "code = \"" + conflict_code + "\"\n"


def _assert_rule_fixtures():
    assert "foreign_type_validation" in _finding_ids(
        "import hashlib\nvalue = hashlib.sha256(b'x').hexdigest()\n",
        "src/core/foreign_hash.py",
        "foreign_type_validation",
    )
    assert "foreign_type_validation" in _finding_ids(
        "import jsonschema\n",
        "src/core/foreign_schema.py",
        "foreign_type_validation",
    )
    assert "foreign_type_validation" not in _finding_ids(
        "import hashlib\nvalue = hashlib.sha256(b'x').hexdigest()\n",
        "src/interfaces/mqtt_interface.py",
        "foreign_type_validation",
    )
    assert "single_library_root" in _finding_ids(
        "from src.libraries import invalid\n",
        "src/core/invalid_import.py",
        "single_library_root",
    )
    assert "forbid_epoch_stale" in _finding_ids(
        _forbidden_epoch_source(),
        "src/core/invalid_conflict.py",
        "forbid_epoch_stale",
    )



def _assert_explicit_override_precedence():
    overrides = {
        "max_function_length": {"enabled": False},
        "max_branch_count": {"enabled": False},
        "max_nesting_depth": {"enabled": False},
    }
    profile = standards.build_profile(
        guidelines_file=os.path.join(
            PROJECT_ROOT,
            "ARCHITEKTUR_CONFIG_V0101_DE.md",
        ),
        rule_overrides=overrides,
    )
    assert profile["guidelines"]["loaded"]
    for rule_id in overrides:
        assert profile["rules"][rule_id]["enabled"] is False
def _assert_physical_contract():
    assert not os.path.exists(os.path.join(PROJECT_ROOT, "src", "libraries"))
    assert not os.path.exists(os.path.join(PROJECT_ROOT, "automation_contracts"))
    assert not os.path.exists(os.path.join(
        PROJECT_ROOT, "src", "library", "_config_compatibility.py"))
    assert not os.path.exists(os.path.join(
        PROJECT_ROOT, "src", "library", "_cross_scope_library.py"))

    package_root = os.path.join(
        PROJECT_ROOT,
        "src",
        "library",
        "_generic_system_contract_library",
    )
    actual_modules = frozenset(
        name for name in os.listdir(package_root) if name.endswith(".py"))
    assert actual_modules == CONTRACT_MODULES
    assert type_authority.TYPE_SCHEMA_VERSION == "2.0.0"
    assert type_authority.CONFIG_NORM_VERSION == {
        "node": 9,
        "cross": 8,
        "legacy_import": 5,
    }
    assert contract_authority.CONTRACTS_VERSION == "2.0"


def run_test():
    _assert_rule_fixtures()
    _assert_explicit_override_precedence()
    _assert_physical_contract()
    result = standards.run_specific_rules(
        paths=SCAN_ROOTS,
        rule_names=HARD_RULES,
        cwd=PROJECT_ROOT,
    )
    assert result["passed"], "Authority V5 static findings: {}".format(
        result["findings"][:50])
    print("PASS Authority V5 static source and contract gates")

if __name__ == "__main__":
    run_test()
