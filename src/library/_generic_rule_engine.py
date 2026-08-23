# -*- coding: utf-8 -*-

# src/library/_generic_rule_engine.py


"""
 =============================================================================
 _GENERIC_RULE_ENGINE
 =============================================================================

 WHAT IT IS :
     A generic, deterministic, purely functional rule engine library.
     It evaluates declarative rule definitions (condition trees plus
     ordered action lists) against arbitrary fact snapshots and produces
     a deterministic, ordered decision plan. The library is fully data
     driven: operators, signal sources, value resolvers and action
     handlers are open registries, so any domain (industrial automation,
     IoT, monitoring, business logic, games, testing) can plug in its
     own vocabulary without touching the engine core.

 WHY IT IS NEEDED :
     Automation systems need one central, auditable place where
     "IF <signals> THEN <actions>" logic lives. Hard coded if/else
     chains scatter logic across threads and are impossible to sync,
     diagnose or persist. This library turns rule logic into pure data
     (JSON compatible), evaluates it deterministically and keeps all
     mutable evaluation state inside one explicit context object.
     It is natively compatible with the "device_state_data_v5" rule
     schema (when / until / do / else_do, sensor / state / timer
     sources, param_of_state value references, latch, cooldown_s,
     filter_id, priorities, engine safety block) while staying fully
     generic and reusable for future applications.

 HOW IT WORKS :
     1. create_engine_context() builds ONE central context dict that
        owns every registry, every compiled rule and all runtime state.
        The context is protected by a threading.RLock and is therefore
        safe under free threaded (no GIL) Python. Facts snapshots are
        read only for the engine: the engine is a pure decision kernel,
        a single writer applies the returned action plan.
     2. Rules are compiled (compile_rule) into a normal form:
            "when"    -> condition tree (a plain list is an implicit ALL)
            "until"   -> optional release tree (hysteresis / Schmitt)
            "do"      -> ordered action list for the TRUE branch
            "else_do" -> ordered action list for the FALSE branch
        Every condition leaf receives a stable condition id ("_cid"),
        so per condition runtime state (cooldown hold, change memory)
        is addressable, exportable and restorable.
     3. evaluate_rules(context, facts, now) walks all enabled rules in
        a stable order (priority ascending, then rule_id, then insert
        sequence). Every condition is evaluated WITHOUT short circuit,
        so stateful conditions advance uniformly. The per rule state
        machine applies hysteresis ("until"), manual latching ("latch"),
        per condition cooldown hold ("cooldown_s") and missing signal
        safety fallbacks. The result is a decision report containing a
        globally ordered action plan: applying actions in plan order
        means higher priority rules write last and therefore win.
     4. execute_actions(context, decision, target) optionally applies
        the plan through registered action handlers (single writer
        pattern) honouring engine constraints such as writable_fields
        and required_mode_id.
     5. Determinism: identical (rules + runtime state + facts + now)
        input always produces byte identical decision output
        (verifiable via decisions_fingerprint). Time is always injected
        (now parameter or facts["now"]); wall clocks are only an
        explicit fallback and are replaceable inside the context.

 DESIGN RULES (project code specification) :
     Python standard library only, synchronous functional style,
     no classes, no decorators, no dataclasses, no lambdas (only
     functools.partial), no type hints, comments in English,
     C conversion friendly flat functions, deterministic behaviour.
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import copy
import functools

import json
import logging
import operator
import re
import threading
import time

from src.library import _generic_type_library as type_library


# =============================================================================
# CENTRAL MODULE "CONTEXT MANAGEMENT" - DEFINITION & INITIALISATION
# =============================================================================

# Module level logger. Minimal by design: the host application decides
# about handlers, formatting and levels. NullHandler avoids noise.
LOGGER = logging.getLogger("_generic_rule_engine")
LOGGER.addHandler(logging.NullHandler())

# Unique sentinel object for unresolvable values. Any comparison against
# MISSING deterministically evaluates to False and is recorded in the
# trace and in the decision "missing" list.
MISSING = object()

# Textual representation of the MISSING sentinel inside traces / JSON.
MISSING_TEXT = "<MISSING>"

# Decision schema tag, embedded in every decision report.
DECISION_SCHEMA = "generic_rule_engine.decision.v1"

# Library wide default configuration. A copy of this dict is placed
# into every new engine context and may be adjusted per context.
ENGINE_LIBRARY_DEFAULTS = {
    "engine_enabled": True,        # global engine on/off switch
    "trace": True,                 # collect per rule condition traces
    "emit": "level",               # "level": emit branch every tick / "edge": only on change
    "strict": False,               # True: add_rule raises on fatal issues
    "default_priority": 100,       # priority when a rule defines none
    "default_src": "fact",         # source used when a leaf has no "src"
    "default_field": "value_id",   # field used by the "state" source
    "coordinator_min_priority": None,  # rules >= this priority get scope "coordinator"
    "max_signal_age_s": None,      # staleness limit for {"value","ts"} signals
    "sp_tolerance": 0.0,           # default tolerance for the "near" operator
    "safety_on_missing": None,     # e.g. {"value_id": 2} -> safety branch patch
    "required_mode_id": None,      # state writes only for states in this mode
    "writable_fields": None,       # e.g. ["value_id", "parameters"] or None = all
}


def create_engine_context(engine_cfg=None, config=None):
    """Create and initialise one central engine context dict.

    The context is THE single owner of all registries, compiled rules
    and runtime state. It is safe to share between threads: every
    mutating engine function acquires context["lock"].
    """
    context = {
        "engine_id": None,
        "engine_cfg": {},                        # raw engine configuration row
        "config": dict(ENGINE_LIBRARY_DEFAULTS), # effective library configuration
        "operators": {},                         # name -> op(env, cond, left, right) -> bool
        "sources": {},                           # name -> src(context, facts, cond, env) -> value
        "value_resolvers": [],                   # ordered list of (marker_key, resolver)
        "action_handlers": [],                   # ordered list of (marker_key, handler)
        "rules": {},                             # rule_id -> compiled rule
        "rule_order": [],                        # rule_ids in insertion order
        "sorted_cache": None,                    # cached deterministic evaluation order
        "seq_counter": 0,                        # insertion sequence source
        "runtime": {"rules": {}, "timers": {}},  # all mutable evaluation state
        "dataset": None,                         # optional bound dataset index
        "stats": {"ticks": 0, "rules_evaluated": 0,
                  "actions_emitted": 0, "last_now": None},
        "lock": threading.RLock(),
        "clock": time.time,                      # replaceable wall clock (fallback only)
        "localtime": time.localtime,             # replaceable local time (timers "at"/"cron")
    }
    if isinstance(config, dict):
        context["config"].update(config)
    _register_default_operators(context)
    _register_default_sources(context)
    _register_default_value_resolvers(context)
    _register_default_action_handlers(context)
    if isinstance(engine_cfg, dict):
        _absorb_engine_cfg(context, engine_cfg)
    return context


# =============================================================================
# HELP-FUNCTIONS
# =============================================================================

def _is_number(value):
    """Return True for int/float but not bool (bool is an int subtype)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _multi_get(mapping, key):
    """Tolerant dict lookup: try the native key, then str(key), then int(key).

    JSON object keys are always strings while configuration ids are
    integers; this helper bridges both worlds deterministically.
    Returns a (found_flag, value) tuple.
    """
    if not isinstance(mapping, dict):
        return (False, MISSING)
    if key in mapping:
        return (True, mapping[key])
    text_key = str(key)
    if text_key in mapping:
        return (True, mapping[text_key])
    try:
        int_key = int(text_key)
    except (TypeError, ValueError):
        return (False, MISSING)
    if int_key in mapping:
        return (True, mapping[int_key])
    return (False, MISSING)


def _deep_get(data, path, default):
    """Resolve a dotted path ("a.b.0.c") or a segment list inside nested data.

    Numeric segments address list indices; dict segments use _multi_get.
    Returns default when any segment cannot be resolved.
    """
    if path is None:
        return default
    if isinstance(path, str):
        segments = path.split(".") if path else []
    elif isinstance(path, (list, tuple)):
        segments = list(path)
    else:
        return default
    node = data
    for segment in segments:
        if isinstance(node, list):
            try:
                index = int(segment)
            except (TypeError, ValueError):
                return default
            if -len(node) <= index < len(node):
                node = node[index]
            else:
                return default
        elif isinstance(node, dict):
            found, value = _multi_get(node, segment)
            if not found:
                return default
            node = value
        else:
            return default
    return node


def _json_default(value):
    """JSON fallback encoder used by _stable_json (handles MISSING etc.)."""
    if value is MISSING:
        return MISSING_TEXT
    return str(value)


def _stable_json(data):
    """Canonical JSON text: sorted keys, compact separators, deterministic."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"),
                      default=_json_default, ensure_ascii=True)


def _traceable(value):
    """Convert a value into a trace/JSON friendly representation."""
    if value is MISSING:
        return MISSING_TEXT
    if isinstance(value, (dict, list, tuple)):
        try:
            return json.loads(_stable_json(value))
        except (TypeError, ValueError):
            return str(value)
    return value


def _rule_sort_key(rule):
    """Deterministic evaluation order: priority, then rule_id, then sequence."""
    return (rule.get("priority", 0), rule.get("rule_id", 0), rule.get("seq", 0))


def _stable_id_key(value):
    """Deterministic sort key for mixed int/str identifiers."""
    try:
        return (0, int(str(value)), "")
    except (TypeError, ValueError):
        return (1, 0, str(value))


def _rule_runtime_slot(context, rule_id):
    """Fetch or create the runtime state slot of one rule (lock held by caller)."""
    slots = context["runtime"]["rules"]
    key = str(rule_id)
    if key not in slots:
        slots[key] = {
            "active": False,       # current state machine output
            "latched": False,      # manual latch flag
            "fires": 0,            # rising edge counter
            "last_change": None,   # timestamp of the last active flip
            "conditions": {},      # per condition runtime state keyed by _cid
        }
    return slots[key]


def _cid_state(env, cond):
    """Fetch or create per condition runtime state (cooldown, change memory)."""
    cid = cond.get("_cid", "?")
    conditions = env["rt"]["conditions"]
    if cid not in conditions:
        conditions[cid] = {}
    return conditions[cid]


_REGEX_CACHE = {}


def _regex_compiled(pattern):
    """Compile and cache a regex pattern; return None on invalid patterns."""
    compiled = _REGEX_CACHE.get(pattern)
    if compiled is None:
        try:
            compiled = re.compile(pattern)
        except (re.error, TypeError):
            LOGGER.warning("invalid regex pattern: %r", pattern)
            return None
        _REGEX_CACHE[pattern] = compiled
    return compiled


def _collect_leaves(node, out_list):
    """Recursively collect all condition leaves of a tree into out_list."""
    if node is None or isinstance(node, bool):
        return out_list
    if isinstance(node, list):
        for child in node:
            _collect_leaves(child, out_list)
        return out_list
    if isinstance(node, dict):
        for gate in ("all", "any", "none", "xor"):
            if gate in node:
                for child in node[gate]:
                    _collect_leaves(child, out_list)
                return out_list
        if "not" in node:
            _collect_leaves(node["not"], out_list)
            return out_list
        out_list.append(node)
    return out_list


def _warn_once(context, key, message, *args):
    """Log a warning only once per context and key (keeps logs minimal)."""
    seen = context.setdefault("_warned", {})
    if key not in seen:
        seen[key] = True
        LOGGER.warning(message, *args)


# =============================================================================
# CODEBASE - OPERATOR FUNCTIONS
# =============================================================================
# Uniform operator signature: op(env, cond, left, right) -> bool
# Comparison and flag operators are built with functools.partial.

def _op_compare(compare_func, env, cond, left, right):
    """Generic safe comparison; MISSING or type errors yield False."""
    if left is MISSING or right is MISSING:
        return False
    try:
        return bool(compare_func(left, right))
    except TypeError:
        return False


def _op_in(env, cond, left, right):
    """True when left is a member of the right container."""
    if left is MISSING or right is MISSING:
        return False
    try:
        return left in right
    except TypeError:
        return False


def _op_not_in(env, cond, left, right):
    """Deterministic negation of the membership test."""
    if left is MISSING or right is MISSING:
        return False
    try:
        return left not in right
    except TypeError:
        return False


def _op_contains(env, cond, left, right):
    """True when the left container contains the right value."""
    if left is MISSING or right is MISSING:
        return False
    try:
        return right in left
    except TypeError:
        return False


def _op_not_contains(env, cond, left, right):
    """Deterministic negation of the contains test."""
    if left is MISSING or right is MISSING:
        return False
    try:
        return right not in left
    except TypeError:
        return False


def _op_startswith(env, cond, left, right):
    """String prefix test."""
    if left is MISSING or right is MISSING:
        return False
    try:
        return left.startswith(right)
    except (TypeError, AttributeError):
        return False


def _op_endswith(env, cond, left, right):
    """String suffix test."""
    if left is MISSING or right is MISSING:
        return False
    try:
        return left.endswith(right)
    except (TypeError, AttributeError):
        return False


def _op_regex(env, cond, left, right):
    """Regular expression search on the string form of left."""
    if left is MISSING or right is MISSING:
        return False
    compiled = _regex_compiled(right)
    if compiled is None:
        return False
    try:
        return compiled.search(str(left)) is not None
    except TypeError:
        return False


def _op_between(env, cond, left, right):
    """Inclusive range test; right must be a [low, high] pair."""
    if left is MISSING or right is MISSING:
        return False
    if not isinstance(right, (list, tuple)) or len(right) < 2:
        return False
    try:
        return right[0] <= left <= right[1]
    except TypeError:
        return False


def _op_near(env, cond, left, right):
    """Numeric closeness: |left - right| <= tol.

    Tolerance source: cond["tol"] -> engine sp_tolerance -> 0.0.
    """
    if not _is_number(left) or not _is_number(right):
        return False
    tolerance = cond.get("tol")
    if not _is_number(tolerance):
        tolerance = env["cfg"].get("sp_tolerance")
    if not _is_number(tolerance):
        tolerance = 0.0
    return abs(left - right) <= tolerance


def _op_is_none(env, cond, left, right):
    """True when left resolved to the None value (MISSING stays False)."""
    return left is None


def _op_not_none(env, cond, left, right):
    """True when left resolved to a real value other than None."""
    return left is not MISSING and left is not None


def _op_is_true(env, cond, left, right):
    """Truthiness test of the resolved left value."""
    return left is not MISSING and bool(left)


def _op_is_false(env, cond, left, right):
    """Falsiness test of the resolved left value."""
    return left is not MISSING and not bool(left)


def _op_changed(env, cond, left, right):
    """True when the resolved left value differs from the previous tick.

    The previous value is remembered per condition inside the rule
    runtime, so the operator is deterministic and persistable.
    """
    if left is MISSING:
        return False
    state = _cid_state(env, cond)
    previous = state.get("last_value", MISSING)
    state["last_value"] = _traceable(left)
    if previous is MISSING:
        return False
    return previous != _traceable(left)


def _op_flag(flag_name, env, cond, left, right):
    """Boolean flag test on dict shaped signals (timer status and similar).

    left may be a plain bool or a status dict like {"elapsed": True}.
    right (the expected value) defaults to True when not given.
    """
    if left is MISSING:
        return False
    expected = True
    if right is not MISSING and right is not None:
        expected = bool(right)
    if isinstance(left, dict):
        found, flag_value = _multi_get(left, flag_name)
        if not found:
            return False
        return bool(flag_value) == expected
    return bool(left) == expected


def _register_default_operators(context):
    """Install the deterministic default operator vocabulary."""
    table = context["operators"]
    # Comparison operators, built via functools.partial (no lambdas).
    table["=="] = functools.partial(_op_compare, operator.eq)
    table["!="] = functools.partial(_op_compare, operator.ne)
    table[">"] = functools.partial(_op_compare, operator.gt)
    table[">="] = functools.partial(_op_compare, operator.ge)
    table["<"] = functools.partial(_op_compare, operator.lt)
    table["<="] = functools.partial(_op_compare, operator.le)
    # Readable aliases.
    table["eq"] = table["=="]
    table["ne"] = table["!="]
    table["gt"] = table[">"]
    table["ge"] = table[">="]
    table["lt"] = table["<"]
    table["le"] = table["<="]
    # Container / string operators.
    table["in"] = _op_in
    table["not_in"] = _op_not_in
    table["contains"] = _op_contains
    table["not_contains"] = _op_not_contains
    table["startswith"] = _op_startswith
    table["endswith"] = _op_endswith
    table["regex"] = _op_regex
    table["between"] = _op_between
    table["near"] = _op_near
    table["~="] = _op_near
    # Presence / truthiness operators.
    table["is_none"] = _op_is_none
    table["not_none"] = _op_not_none
    table["is_true"] = _op_is_true
    table["is_false"] = _op_is_false
    table["changed"] = _op_changed
    # Flag operators for timer style status dicts, built via partial.
    table["elapsed"] = functools.partial(_op_flag, "elapsed")
    table["fired"] = functools.partial(_op_flag, "fired")
    table["running"] = functools.partial(_op_flag, "running")


def register_operator(context, name, op_func):
    """Register or replace a custom operator. Signature:
    op_func(env, cond, left, right) -> bool
    """
    with context["lock"]:
        context["operators"][name] = op_func


# =============================================================================
# CODEBASE - SIGNAL SOURCE RESOLVERS (LEFT OPERAND)
# =============================================================================
# Uniform source signature: src(context, facts, cond, env) -> value|MISSING

def _unwrap_signal(env, cond, raw_value, notes):
    """Unwrap {"value": x, "ts": t} signal dicts and apply staleness checks."""
    if isinstance(raw_value, dict) and "value" in raw_value:
        max_age = cond.get("max_age_s")
        if not _is_number(max_age):
            max_age = env["cfg"].get("max_signal_age_s")
        timestamp = raw_value.get("ts")
        if _is_number(max_age) and _is_number(timestamp):
            if (env["now"] - timestamp) > max_age:
                notes.append("stale_signal")
                return MISSING
        return raw_value.get("value", MISSING)
    return raw_value


def _source_fact(context, facts, cond, env):
    """Generic source: resolve cond["path"] directly inside the facts dict."""
    return _deep_get(facts, cond.get("path"), MISSING)


def _source_const(context, facts, cond, env):
    """Constant source: the left operand is taken from cond["const"]."""
    if "const" in cond:
        return cond["const"]
    return MISSING


def _source_sensor(context, facts, cond, env):
    """Sensor source of the v5 config schema.

    Lookup order:
      1. filtered signal   facts["filters"][filter_id][controller_uuid][sensor_id]
      2. raw signal        facts["sensors"][controller_uuid][sensor_id]
    Signals may be plain numbers or {"value": x, "ts": t} dicts; dict
    signals are staleness checked against max_signal_age_s.
    """
    controller_uuid = cond.get("controller_uuid")
    sensor_id = cond.get("sensor_id")
    filter_id = cond.get("filter_id")
    notes = env["notes"]
    raw_value = MISSING
    if filter_id is not None:
        found, filter_map = _multi_get(facts.get("filters", {}), filter_id)
        if found:
            found, controller_map = _multi_get(filter_map, controller_uuid)
            if found:
                found, raw_value = _multi_get(controller_map, sensor_id)
        if raw_value is MISSING:
            notes.append("filter_fallback_raw")
    if raw_value is MISSING:
        found, controller_map = _multi_get(facts.get("sensors", {}), controller_uuid)
        if found:
            found, raw_value = _multi_get(controller_map, sensor_id)
    if raw_value is MISSING:
        return MISSING
    return _unwrap_signal(env, cond, raw_value, notes)


def _source_state(context, facts, cond, env):
    """State source: read a field of a process state.

    Live facts win; when the state is absent in the facts the bound
    dataset (initial configuration) is used as deterministic fallback.
    """
    state_id = cond.get("state_id")
    field = cond.get("field", env["cfg"].get("default_field", "value_id"))
    found, state_row = _multi_get(facts.get("states", {}), state_id)
    if not found:
        dataset = context.get("dataset")
        if isinstance(dataset, dict):
            found, state_row = _multi_get(dataset.get("states", {}), state_id)
            if found:
                env["notes"].append("state_from_dataset")
    if not found:
        return MISSING
    if isinstance(state_row, dict):
        found, value = _multi_get(state_row, field)
        if not found:
            return MISSING
        return _unwrap_signal(env, cond, value, env["notes"])
    return state_row


def _source_timer(context, facts, cond, env):
    """Timer source: returns the timer status dict from the facts."""
    found, status = _multi_get(facts.get("timers", {}), cond.get("timer_id"))
    if not found:
        return MISSING
    return status


def _register_default_sources(context):
    """Install the default source vocabulary (config compatible + generic)."""
    table = context["sources"]
    table["fact"] = _source_fact
    table["const"] = _source_const
    table["sensor"] = _source_sensor
    table["state"] = _source_state
    table["timer"] = _source_timer


def register_source(context, name, source_func):
    """Register or replace a custom signal source. Signature:
    source_func(context, facts, cond, env) -> value | MISSING
    """
    with context["lock"]:
        context["sources"][name] = source_func


# =============================================================================
# CODEBASE - VALUE RESOLVERS (RIGHT OPERAND)
# =============================================================================
# A literal right operand is used as is. A dict right operand is routed
# to the first resolver whose marker key is present (registration order).

def _apply_scale_offset(value, spec):
    """Apply optional numeric scale and offset from a value spec."""
    if _is_number(value):
        scale = spec.get("scale")
        if _is_number(scale):
            value = value * scale
        offset = spec.get("offset")
        if _is_number(offset):
            value = value + offset
    return value


def _resolve_param_of_state(context, facts, spec, env):
    """Resolve {"param_of_state": id, "key": name, "scale"?, "offset"?}.

    Live facts win; the bound dataset serves as deterministic fallback,
    so setpoints work even before the first live state snapshot exists.
    """
    state_id = spec.get("param_of_state")
    key = spec.get("key")
    found, state_row = _multi_get(facts.get("states", {}), state_id)
    if not (found and isinstance(state_row, dict) and "parameters" in state_row):
        dataset = context.get("dataset")
        found = False
        if isinstance(dataset, dict):
            found, state_row = _multi_get(dataset.get("states", {}), state_id)
    if not (found and isinstance(state_row, dict)):
        return MISSING
    parameters = state_row.get("parameters")
    found, value = _multi_get(parameters if isinstance(parameters, dict) else {}, key)
    if not found:
        return MISSING
    return _apply_scale_offset(value, spec)


def _resolve_fact_path(context, facts, spec, env):
    """Resolve {"path": "a.b.c", "default"?, "scale"?, "offset"?} from facts."""
    value = _deep_get(facts, spec.get("path"), MISSING)
    if value is MISSING and "default" in spec:
        return spec["default"]
    if value is MISSING:
        return MISSING
    return _apply_scale_offset(value, spec)


def _resolve_literal(context, facts, spec, env):
    """Resolve {"value": literal} as an escape hatch for dict literals."""
    return spec.get("value", MISSING)


def _register_default_value_resolvers(context):
    """Install the default right operand resolvers in deterministic order."""
    context["value_resolvers"] = [
        ("param_of_state", _resolve_param_of_state),
        ("path", _resolve_fact_path),
        ("value", _resolve_literal),
    ]


def register_value_resolver(context, marker_key, resolver_func):
    """Register or replace a value resolver bound to a dict marker key.
    Signature: resolver_func(context, facts, spec, env) -> value | MISSING
    """
    with context["lock"]:
        resolvers = context["value_resolvers"]
        for index in range(len(resolvers)):
            if resolvers[index][0] == marker_key:
                resolvers[index] = (marker_key, resolver_func)
                return
        resolvers.append((marker_key, resolver_func))


def resolve_value(context, facts, spec, env):
    """Resolve a right operand: literals pass through, dict specs are
    routed to the first matching registered value resolver."""
    if not isinstance(spec, dict):
        return spec
    for marker_key, resolver_func in context["value_resolvers"]:
        if marker_key in spec:
            try:
                return resolver_func(context, facts, spec, env)
            except Exception:
                LOGGER.exception("value resolver %r failed", marker_key)
                return MISSING
    _warn_once(context, "value_spec:%s" % _stable_json(spec),
               "unknown value spec: %s", _stable_json(spec))
    return MISSING


# =============================================================================
# CODEBASE - CONDITION TREE EVALUATION
# =============================================================================

def _evaluate_leaf(context, env, cond):
    """Evaluate one condition leaf: resolve left/right, apply the
    operator, then apply the deterministic cooldown hold modifier."""
    env["notes"] = []
    cid = cond.get("_cid", "?")
    # ---- left operand (signal source) -------------------------------------
    source_name = cond.get("src", env["cfg"].get("default_src", "fact"))
    source_func = context["sources"].get(source_name)
    left = MISSING
    if source_func is None:
        env["notes"].append("unknown_source")
        _warn_once(context, "source:%s" % source_name,
                   "unknown source: %r", source_name)
    else:
        try:
            left = source_func(context, env["facts"], cond, env)
        except Exception:
            LOGGER.exception("source %r failed (cid=%s)", source_name, cid)
            env["notes"].append("source_error")
            left = MISSING
    # ---- right operand (expected value) ------------------------------------
    right = MISSING
    has_value_key = "value" in cond
    if has_value_key:
        right = resolve_value(context, env["facts"], cond["value"], env)
    # ---- operator -----------------------------------------------------------
    op_name = cond.get("op", "==")
    op_func = context["operators"].get(op_name)
    raw_result = False
    if op_func is None:
        env["notes"].append("unknown_operator")
        _warn_once(context, "op:%s" % op_name, "unknown operator: %r", op_name)
    else:
        try:
            raw_result = bool(op_func(env, cond, left, right))
        except Exception:
            LOGGER.exception("operator %r failed (cid=%s)", op_name, cid)
            env["notes"].append("operator_error")
            raw_result = False
    # ---- missing bookkeeping ------------------------------------------------
    if left is MISSING:
        env["missing"].append({"cid": cid, "src": source_name,
                               "reason": "unresolved_left"})
    if has_value_key and right is MISSING:
        env["missing"].append({"cid": cid, "src": source_name,
                               "reason": "unresolved_right"})
    # ---- cooldown hold modifier ----------------------------------------------
    # Semantics: after the raw result was True, the effective result is
    # held True for at least cooldown_s seconds (anti flapping hold).
    effective = raw_result
    cooldown_s = cond.get("cooldown_s")
    if _is_number(cooldown_s) and cooldown_s > 0:
        state = _cid_state(env, cond)
        if raw_result:
            state["cooldown_until"] = env["now"] + cooldown_s
        else:
            hold_until = state.get("cooldown_until")
            if _is_number(hold_until) and env["now"] < hold_until:
                effective = True
                env["notes"].append("cooldown_hold")
    # ---- trace ---------------------------------------------------------------
    if env["trace"] is not None:
        env["trace"].append({
            "cid": cid,
            "src": source_name,
            "op": op_name,
            "left": _traceable(left),
            "right": _traceable(right) if has_value_key else None,
            "raw": raw_result,
            "result": effective,
            "notes": list(env["notes"]),
        })
    return effective


def _evaluate_group(context, env, gate, children):
    """Evaluate a logic gate over child nodes WITHOUT short circuit,
    so stateful conditions (cooldown, changed) advance uniformly.

    Empty gates are deterministic: all([])=True, any([])=False,
    none([])=True, xor([])=False. "xor" means exactly one child True.
    """
    results = []
    if isinstance(children, (list, tuple)):
        for child in children:
            results.append(_evaluate_node(context, env, child))
    if gate == "all":
        return all(results)
    if gate == "any":
        return any(results)
    if gate == "none":
        return not any(results)
    if gate == "xor":
        true_count = 0
        for flag in results:
            if flag:
                true_count += 1
        return true_count == 1
    return False


def _evaluate_node(context, env, node):
    """Evaluate a condition tree node: gate dict, leaf dict, list or literal.

    A None tree is vacuously True (a rule without conditions always fires).
    """
    if node is None:
        return True
    if isinstance(node, bool):
        return node
    if isinstance(node, list):
        return _evaluate_group(context, env, "all", node)
    if isinstance(node, dict):
        for gate in ("all", "any", "none", "xor"):
            if gate in node:
                return _evaluate_group(context, env, gate, node[gate])
        if "not" in node:
            return not _evaluate_node(context, env, node["not"])
        return _evaluate_leaf(context, env, node)
    return bool(node)


# =============================================================================
# CODEBASE - RULE COMPILATION, VALIDATION & MANAGEMENT
# =============================================================================

def _normalize_tree(node, cid_prefix):
    """Normalise a condition tree in place: plain lists become explicit
    "all" gates and every leaf receives a stable condition id ("_cid")."""
    if node is None or isinstance(node, bool):
        return node
    if isinstance(node, list):
        children = []
        for index in range(len(node)):
            child_cid = "%s.%d" % (cid_prefix, index)
            children.append(_normalize_tree(node[index], child_cid))
        return {"all": children}
    if isinstance(node, dict):
        for gate in ("all", "any", "none", "xor"):
            if gate in node:
                children = []
                child_nodes = node.get(gate) or []
                for index in range(len(child_nodes)):
                    child_cid = "%s.%s%d" % (cid_prefix, gate[0], index)
                    children.append(_normalize_tree(child_nodes[index], child_cid))
                node[gate] = children
                return node
        if "not" in node:
            node["not"] = _normalize_tree(node["not"], cid_prefix + ".n")
            return node
        node["_cid"] = cid_prefix
        return node
    return node


def compile_rule(context, rule):
    """Compile a raw rule dict into the deterministic internal normal form.

    The input dict is never mutated; a deep copy is normalised instead.
    """
    compiled = copy.deepcopy(rule)
    rule_id = compiled.get("rule_id")
    compiled["rule_id"] = rule_id
    compiled["name"] = compiled.get("name", "rule_%s" % rule_id)
    compiled["group"] = compiled.get("group", "")
    compiled["enabled"] = bool(compiled.get("enabled", True))
    priority = compiled.get("priority")
    if not _is_number(priority):
        priority = context["config"]["default_priority"]
    compiled["priority"] = priority
    compiled["latch"] = compiled.get("latch")
    compiled["emit"] = compiled.get("emit", context["config"]["emit"])
    compiled["when"] = _normalize_tree(compiled.get("when"), "w")
    compiled["until"] = _normalize_tree(compiled.get("until"), "u")
    do_actions = compiled.get("do")
    compiled["do"] = list(do_actions) if isinstance(do_actions, list) else []
    else_actions = compiled.get("else_do")
    compiled["else_do"] = list(else_actions) if isinstance(else_actions, list) else []
    return compiled


def validate_rule(context, rule):
    """Validate one raw rule dict; returns a list of issue strings.

    Issues starting with "fatal:" prevent the rule from being added.
    """
    issues = []
    if not isinstance(rule, dict):
        return ["fatal: rule is not a dict"]
    rule_id = rule.get("rule_id")
    if rule_id is None:
        issues.append("fatal: missing rule_id")
    for section in ("when", "until"):
        node = rule.get(section)
        if node is not None and not isinstance(node, (list, dict, bool)):
            issues.append("fatal: %s must be list/dict/bool/None" % section)
    for section in ("do", "else_do"):
        actions = rule.get(section)
        if actions is None:
            continue
        if not isinstance(actions, list):
            issues.append("fatal: %s must be a list" % section)
            continue
        for index in range(len(actions)):
            if not isinstance(actions[index], dict):
                issues.append("fatal: %s[%d] must be a dict" % (section, index))
    priority = rule.get("priority")
    if priority is not None and not _is_number(priority):
        issues.append("warn: priority not numeric, default is used")
    leaves = []
    for section in ("when", "until"):
        node = rule.get(section)
        if isinstance(node, (list, dict)):
            _collect_leaves(node, leaves)
    for leaf in leaves:
        op_name = leaf.get("op", "==")
        if not isinstance(op_name, str):
            issues.append("fatal: condition op must be a string")
        elif op_name not in context["operators"]:
            issues.append("warn: unknown operator %r" % op_name)
        source_name = leaf.get("src", context["config"]["default_src"])
        if source_name not in context["sources"]:
            issues.append("warn: unknown source %r" % source_name)
    return issues


def _has_fatal(issues):
    """Return True when the issue list contains a fatal entry."""
    for issue in issues:
        if issue.startswith("fatal:"):
            return True
    return False


def add_rule(context, rule, replace=False):
    """Validate, compile and register one rule. Returns the issue list.

    Fatal issues skip the rule (or raise ValueError in strict mode).
    """
    issues = validate_rule(context, rule)
    with context["lock"]:
        rule_id = rule.get("rule_id") if isinstance(rule, dict) else None
        if rule_id is not None and rule_id in context["rules"] and not replace:
            issues.append("fatal: duplicate rule_id %r" % rule_id)
        if _has_fatal(issues):
            if context["config"]["strict"]:
                raise ValueError("rule rejected: %s" % "; ".join(issues))
            LOGGER.warning("rule %r rejected: %s", rule_id, "; ".join(issues))
            return issues
        compiled = compile_rule(context, rule)
        if rule_id not in context["rules"]:
            context["rule_order"].append(rule_id)
            context["seq_counter"] += 1
            compiled["seq"] = context["seq_counter"]
        else:
            compiled["seq"] = context["rules"][rule_id].get("seq", 0)
        context["rules"][rule_id] = compiled
        _rule_runtime_slot(context, rule_id)
        context["sorted_cache"] = None
    return issues


def remove_rule(context, rule_id):
    """Remove one rule and its runtime state; returns True when found."""
    with context["lock"]:
        if rule_id not in context["rules"]:
            return False
        del context["rules"][rule_id]
        if rule_id in context["rule_order"]:
            context["rule_order"].remove(rule_id)
        context["runtime"]["rules"].pop(str(rule_id), None)
        context["sorted_cache"] = None
    return True


def clear_rules(context):
    """Remove all rules and all per rule runtime state."""
    with context["lock"]:
        context["rules"] = {}
        context["rule_order"] = []
        context["runtime"]["rules"] = {}
        context["sorted_cache"] = None


def get_rule(context, rule_id):
    """Return a detached deep copy of one compiled rule (or None)."""
    with context["lock"]:
        compiled = context["rules"].get(rule_id)
        return copy.deepcopy(compiled) if compiled is not None else None


def list_rules(context):
    """Return all rule_ids in deterministic evaluation order."""
    with context["lock"]:
        ordered = _sorted_rules(context)
        result = []
        for rule in ordered:
            result.append(rule.get("rule_id"))
        return result


def set_rule_enabled(context, rule_id, enabled):
    """Enable or disable one rule at runtime; returns True when found."""
    with context["lock"]:
        compiled = context["rules"].get(rule_id)
        if compiled is None:
            return False
        compiled["enabled"] = bool(enabled)
        context["sorted_cache"] = None
    return True


def _sorted_rules(context):
    """Return (and cache) the deterministic rule evaluation order."""
    cache = context["sorted_cache"]
    if cache is None:
        cache = sorted(context["rules"].values(), key=_rule_sort_key)
        context["sorted_cache"] = cache
    return cache


# =============================================================================
# CODEBASE - RULE STATE MACHINE & ENGINE TICK
# =============================================================================

def _apply_rule_state_machine(rule, rt, when_result, until_result):
    """Apply hysteresis ("until"), manual latch ("latch") and edge
    detection. Returns (active, changed) deterministically.

    - latch "manual": once active the rule stays active until
      reset_rule_latch() is called, regardless of the conditions.
    - "until" present: Schmitt trigger. Inactive -> "when" arms the
      rule; active -> only "until" releases it ("when" may drop).
    - plain rule: active follows "when" directly.
    """
    previous_active = bool(rt.get("active"))
    if rt.get("latched"):
        active = True
    elif rule.get("until") is not None:
        if previous_active:
            active = not bool(until_result)
        else:
            active = bool(when_result)
    else:
        active = bool(when_result)
    if active and rule.get("latch") == "manual":
        rt["latched"] = True
    changed = active != previous_active
    rt["active"] = active
    return (active, changed)


def _derive_safety_actions(context, rule):
    """Derive deterministic safety actions for one rule from the engine
    safety patch (e.g. {"value_id": 2}). Targets are taken from the
    rule's own do/else_do actions (state_ids / p_ids)."""
    patch = context["config"].get("safety_on_missing")
    if not isinstance(patch, dict):
        return []
    actions = []
    seen = {}
    for action in rule.get("do", []) + rule.get("else_do", []):
        target = {}
        if "state_ids" in action:
            target["state_ids"] = list(action["state_ids"])
        elif "p_ids" in action:
            target["p_ids"] = list(action["p_ids"])
        else:
            continue
        for key in sorted(patch.keys()):
            target[key] = patch[key]
        fingerprint = _stable_json(target)
        if fingerprint in seen:
            continue
        seen[fingerprint] = True
        actions.append(target)
    return actions


def _action_scope(context, priority):
    """Classify an action scope by the coordinator priority threshold."""
    minimum = context["config"].get("coordinator_min_priority")
    if _is_number(minimum) and _is_number(priority) and priority >= minimum:
        return "coordinator"
    return "local"


def _emit_actions(context, decision, rule, branch, actions):
    """Append one branch's actions to the globally ordered action plan."""
    for index in range(len(actions)):
        decision["actions"].append({
            "seq": len(decision["actions"]),
            "rule_id": rule.get("rule_id"),
            "rule_name": rule.get("name"),
            "group": rule.get("group"),
            "priority": rule.get("priority"),
            "branch": branch,
            "scope": _action_scope(context, rule.get("priority")),
            "index": index,
            "action": copy.deepcopy(actions[index]),
        })


def _evaluate_rule_step(context, decision, rule, facts, now):
    """Evaluate one rule inside a tick: conditions, state machine,
    branch selection, safety fallback and action emission."""
    rt = _rule_runtime_slot(context, rule.get("rule_id"))
    trace = [] if context["config"]["trace"] else None
    env = {
        "now": now,
        "facts": facts,
        "cfg": context["config"],
        "rule": rule,
        "rt": rt,
        "missing": [],
        "trace": trace,
        "notes": [],
    }
    when_result = _evaluate_node(context, env, rule.get("when"))
    until_result = None
    if rule.get("until") is not None:
        until_result = _evaluate_node(context, env, rule.get("until"))
    active, changed = _apply_rule_state_machine(rule, rt, when_result, until_result)
    if changed:
        rt["last_change"] = now
        if active:
            rt["fires"] += 1
            decision["fired"].append(rule.get("rule_id"))
        else:
            decision["released"].append(rule.get("rule_id"))
    if active:
        decision["active"].append(rule.get("rule_id"))
    # ---- missing signal safety ---------------------------------------------
    when_sensor_missing = False
    for entry in env["missing"]:
        decision["missing"].append({
            "rule_id": rule.get("rule_id"),
            "cid": entry["cid"],
            "src": entry["src"],
            "reason": entry["reason"],
        })
        if str(entry["cid"]).startswith("w") and entry["src"] == "sensor":
            when_sensor_missing = True
    branch = None
    if when_sensor_missing and isinstance(context["config"].get("safety_on_missing"), dict):
        branch = "safety"
        safety_actions = _derive_safety_actions(context, rule)
        _emit_actions(context, decision, rule, "safety", safety_actions)
        for action in safety_actions:
            decision["safety"].append({"rule_id": rule.get("rule_id"),
                                       "action": copy.deepcopy(action)})
    else:
        if active and rule.get("do"):
            branch = "do"
        elif (not active) and rule.get("else_do"):
            branch = "else_do"
        if branch is not None and rule.get("emit") == "edge" and not changed:
            branch = None
        if branch == "do":
            _emit_actions(context, decision, rule, "do", rule["do"])
        elif branch == "else_do":
            _emit_actions(context, decision, rule, "else_do", rule["else_do"])
    # ---- trace ----------------------------------------------------------------
    if context["config"]["trace"]:
        decision["rules"][str(rule.get("rule_id"))] = {
            "name": rule.get("name"),
            "group": rule.get("group"),
            "priority": rule.get("priority"),
            "when": bool(when_result),
            "until": (None if until_result is None else bool(until_result)),
            "active": active,
            "changed": changed,
            "latched": bool(rt.get("latched")),
            "branch": branch,
            "conditions": trace,
        }
    return branch


def _resolve_now(context, facts, now):
    """Deterministic time resolution: explicit now -> facts["now"] -> clock."""
    if _is_number(now):
        return float(now)
    if isinstance(facts, dict) and _is_number(facts.get("now")):
        return float(facts["now"])
    return float(context["clock"]())


def evaluate_rules(context, facts, now=None):
    """Evaluate ALL enabled rules against one immutable facts snapshot.

    Returns the decision report: an ordered action plan plus rule level
    results, edges, missing signals, safety fallbacks and traces.
    The facts dict is never mutated. Runtime state (hysteresis, latch,
    cooldown, change memory) is updated inside the context lock.
    """
    if not isinstance(facts, dict):
        facts = {}
    resolved_now = _resolve_now(context, facts, now)
    decision = {
        "schema": DECISION_SCHEMA,
        "engine_id": context.get("engine_id"),
        "now": resolved_now,
        "engine_enabled": bool(context["config"].get("engine_enabled", True)),
        "evaluated": 0,
        "fired": [],
        "released": [],
        "active": [],
        "actions": [],
        "notifications": [],
        "missing": [],
        "safety": [],
        "rules": {},
    }
    if not decision["engine_enabled"]:
        return decision
    with context["lock"]:
        ordered = _sorted_rules(context)
        for rule in ordered:
            if not rule.get("enabled", True):
                continue
            try:
                _evaluate_rule_step(context, decision, rule, facts, resolved_now)
            except Exception:
                LOGGER.exception("rule %r evaluation failed", rule.get("rule_id"))
            decision["evaluated"] += 1
        stats = context["stats"]
        stats["ticks"] += 1
        stats["rules_evaluated"] += decision["evaluated"]
        stats["actions_emitted"] += len(decision["actions"])
        stats["last_now"] = resolved_now
    return decision


def evaluate_rule(context, rule_id, facts, now=None):
    """Evaluate exactly one rule (by id) against a facts snapshot.

    Returns a mini decision report limited to that rule (or None when
    the rule does not exist). Runtime state of the rule advances.
    """
    if not isinstance(facts, dict):
        facts = {}
    resolved_now = _resolve_now(context, facts, now)
    decision = {
        "schema": DECISION_SCHEMA,
        "engine_id": context.get("engine_id"),
        "now": resolved_now,
        "engine_enabled": bool(context["config"].get("engine_enabled", True)),
        "evaluated": 0,
        "fired": [],
        "released": [],
        "active": [],
        "actions": [],
        "notifications": [],
        "missing": [],
        "safety": [],
        "rules": {},
    }
    with context["lock"]:
        rule = context["rules"].get(rule_id)
        if rule is None:
            return None
        try:
            _evaluate_rule_step(context, decision, rule, facts, resolved_now)
        except Exception:
            LOGGER.exception("rule %r evaluation failed", rule_id)
        decision["evaluated"] = 1
    return decision


# =============================================================================
# CODEBASE - LATCH & RUNTIME CONTROL
# =============================================================================

def reset_rule_latch(context, rule_id):
    """Release the manual latch of one rule; returns True when found."""
    with context["lock"]:
        slot = context["runtime"]["rules"].get(str(rule_id))
        if slot is None:
            return False
        slot["latched"] = False
        slot["active"] = False
    return True


def reset_all_latches(context):
    """Release all manual latches; returns the number of released rules."""
    released = 0
    with context["lock"]:
        for key in sorted(context["runtime"]["rules"].keys(), key=_stable_id_key):
            slot = context["runtime"]["rules"][key]
            if slot.get("latched"):
                slot["latched"] = False
                slot["active"] = False
                released += 1
    return released


def get_rule_runtime(context, rule_id):
    """Return a detached deep copy of one rule's runtime state (or None)."""
    with context["lock"]:
        slot = context["runtime"]["rules"].get(str(rule_id))
        return copy.deepcopy(slot) if slot is not None else None


def engine_export_runtime(context):
    """Export the complete mutable runtime state as a JSON safe dict.
    Used for persistent heap -> file/DB synchronisation."""
    with context["lock"]:
        blob = _stable_json(context["runtime"])
    return json.loads(blob)


def engine_import_runtime(context, blob):
    """Restore a previously exported runtime state (deterministic resume)."""
    if not isinstance(blob, dict):
        return False
    with context["lock"]:
        restored = json.loads(_stable_json(blob))
        if "rules" not in restored or "timers" not in restored:
            return False
        context["runtime"] = restored
    return True


def engine_stats(context):
    """Return a detached copy of the engine statistics counters."""
    with context["lock"]:
        return dict(context["stats"])


# =============================================================================
# CODEBASE - ACTION EXECUTION (SINGLE WRITER)
# =============================================================================

def _writable(context, field):
    """Check one field against the engine writable_fields constraint."""
    allowed = context["config"].get("writable_fields")
    if allowed is None:
        return True
    return field in allowed


def _handler_state_write(context, target, action, meta, decision):
    """Default handler for {"state_ids": [...]} actions.

    Writes value_id and/or parameters into target["states"], honouring
    the engine writable_fields and required_mode_id constraints.
    """
    states = target.setdefault("states", {})
    required_mode = context["config"].get("required_mode_id")
    results = []
    for state_id in action.get("state_ids", []):
        found, state_row = _multi_get(states, state_id)
        if not found or not isinstance(state_row, dict):
            state_row = {} if not isinstance(state_row, dict) else state_row
            states[state_id] = state_row
        if required_mode is not None:
            found_mode, mode_id = _multi_get(state_row, "mode_id")
            if found_mode and mode_id != required_mode:
                results.append({"state_id": state_id, "status": "skipped_mode"})
                continue
        written = []
        if "value_id" in action and _writable(context, "value_id"):
            state_row["value_id"] = action["value_id"]
            written.append("value_id")
        if "parameters" in action and _writable(context, "parameters"):
            parameters = state_row.setdefault("parameters", {})
            patch = action["parameters"]
            if isinstance(patch, dict):
                for key in sorted(patch.keys()):
                    parameters[key] = patch[key]
                written.append("parameters")
        results.append({"state_id": state_id, "status": "ok", "written": written})
    return results


def _handler_process_write(context, target, action, meta, decision):
    """Default handler for {"p_ids": [...]} actions.

    Expands each process id into its state ids (dataset index first,
    then a deterministic scan of target["states"]) and delegates to the
    state write handler with a synthetic state_ids action.
    """
    dataset = context.get("dataset")
    states_by_pid = dataset.get("states_by_pid", {}) if isinstance(dataset, dict) else {}
    expanded = []
    for process_id in action.get("p_ids", []):
        found, state_ids = _multi_get(states_by_pid, process_id)
        if found:
            for state_id in state_ids:
                expanded.append(state_id)
            continue
        states = target.get("states", {})
        for key in sorted(states.keys(), key=_stable_id_key):
            state_row = states[key]
            if isinstance(state_row, dict):
                found_pid, pid_value = _multi_get(state_row, "p_id")
                if found_pid and str(pid_value) == str(process_id):
                    expanded.append(key)
    synthetic = {"state_ids": sorted(set(expanded), key=_stable_id_key)}
    if "value_id" in action:
        synthetic["value_id"] = action["value_id"]
    if "parameters" in action:
        synthetic["parameters"] = action["parameters"]
    return _handler_state_write(context, target, synthetic, meta, decision)


def _handler_notify(context, target, action, meta, decision):
    """Default handler for {"notify": "message"} actions."""
    message = action.get("notify")
    entry = {
        "rule_id": meta.get("rule_id"),
        "priority": meta.get("priority"),
        "message": message,
    }
    decision["notifications"].append(entry)
    LOGGER.info("notify [rule %s]: %s", meta.get("rule_id"), message)
    return entry


def _register_default_action_handlers(context):
    """Install the default action handler chain in deterministic order."""
    context["action_handlers"] = [
        ("state_ids", _handler_state_write),
        ("p_ids", _handler_process_write),
        ("notify", _handler_notify),
    ]


def register_action_handler(context, marker_key, handler_func):
    """Register or replace an action handler bound to a dict marker key.
    Signature: handler_func(context, target, action, meta, decision)
    """
    with context["lock"]:
        handlers = context["action_handlers"]
        for index in range(len(handlers)):
            if handlers[index][0] == marker_key:
                handlers[index] = (marker_key, handler_func)
                return
        handlers.append((marker_key, handler_func))


def _find_action_handler(context, action):
    """Find the first registered handler whose marker key is present."""
    for marker_key, handler_func in context["action_handlers"]:
        if marker_key in action:
            return (marker_key, handler_func)
    return (None, None)


def execute_actions(context, decision, target=None):
    """Apply the ordered action plan of a decision onto a target store.

    Single writer pattern: exactly one thread should call this per
    scope. Actions are applied strictly in plan order, therefore the
    highest priority rules write last and deterministically win.
    Application results are appended to decision["applied"].
    """
    if target is None:
        target = {}
    applied = []
    for item in decision.get("actions", []):
        marker_key, handler_func = _find_action_handler(context, item["action"])
        if handler_func is None:
            applied.append({"seq": item["seq"], "status": "unhandled"})
            _warn_once(context, "action:%s" % _stable_json(item["action"]),
                       "no action handler for: %s", _stable_json(item["action"]))
            continue
        try:
            result = handler_func(context, target, item["action"], item, decision)
            applied.append({"seq": item["seq"], "status": "ok",
                            "handler": marker_key, "result": result})
        except Exception:
            LOGGER.exception("action handler %r failed (seq=%s)",
                             marker_key, item["seq"])
            applied.append({"seq": item["seq"], "status": "error",
                            "handler": marker_key})
    decision["applied"] = applied
    return target


# =============================================================================
# CODEBASE - DETERMINISTIC TIMER SUPPORT
# =============================================================================
# Timer kinds (detected by config key):
#   duration_s : stopwatch, needs timer_start(); "elapsed" once expired
#   interval_s : free running metronome anchored at first evaluation
#   at         : daily "HH:MM" local time trigger
#   cron       : 5 field cron expression (minute hour dom month dow)
# timers_evaluate() returns a status dict ready to inject as facts["timers"].

def timer_start(context, timer_id, now=None):
    """Start (or restart) a duration timer at the given deterministic time."""
    resolved_now = _resolve_now(context, {}, now)
    with context["lock"]:
        context["runtime"]["timers"][str(timer_id)] = {
            "started_at": resolved_now,
            "elapsed_fired": False,
        }
    return resolved_now


def timer_stop(context, timer_id):
    """Stop a duration timer; returns True when a runtime slot existed."""
    with context["lock"]:
        slot = context["runtime"]["timers"].get(str(timer_id))
        if slot is None:
            return False
        slot["started_at"] = None
    return True


def timer_reset(context, timer_id):
    """Fully reset one timer runtime slot; returns True when it existed."""
    with context["lock"]:
        return context["runtime"]["timers"].pop(str(timer_id), None) is not None


def _parse_hhmm(text):
    """Parse "HH:MM" into an (hour, minute) tuple or None."""
    if not isinstance(text, str):
        return None
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return (hour, minute)
    return None


def _cron_field_match(expression, value, min_value, max_value, is_dow):
    """Match one cron field. Supports "*", "*/n", "a", "a-b", "a-b/n", lists.

    For the day-of-week field the value 7 is normalised to 0 (Sunday).
    Invalid expressions deterministically match nothing.
    """
    if not isinstance(expression, str) or expression == "":
        return False
    for part in expression.split(","):
        part = part.strip()
        step = 1
        if "/" in part:
            base, step_text = part.split("/", 1)
            try:
                step = int(step_text)
            except ValueError:
                continue
            if step <= 0:
                continue
            part = base
        if part == "*":
            low, high = min_value, max_value
        elif "-" in part:
            bounds = part.split("-", 1)
            try:
                low = int(bounds[0])
                high = int(bounds[1])
            except ValueError:
                continue
        else:
            try:
                low = int(part)
            except ValueError:
                continue
            high = low if step == 1 else max_value
        if is_dow:
            if low == 7:
                low = 0
            if high == 7 and low != 0:
                high = 6
        if low > high:
            continue
        candidate = low
        while candidate <= high:
            if candidate == value:
                return True
            candidate += step
    return False


def _cron_match(expression, tm):
    """Match a 5 field cron expression against a struct_time (minute level)."""
    if not isinstance(expression, str):
        return False
    fields = expression.split()
    if len(fields) != 5:
        return False
    cron_dow = (tm.tm_wday + 1) % 7  # struct_time: Monday=0 -> cron: Sunday=0
    return (_cron_field_match(fields[0], tm.tm_min, 0, 59, False)
            and _cron_field_match(fields[1], tm.tm_hour, 0, 23, False)
            and _cron_field_match(fields[2], tm.tm_mday, 1, 31, False)
            and _cron_field_match(fields[3], tm.tm_mon, 1, 12, False)
            and _cron_field_match(fields[4], cron_dow, 0, 6, True))


def _timer_status_duration(slot, duration_s, now):
    """Deterministic status of a duration (stopwatch) timer."""
    started_at = slot.get("started_at")
    if not _is_number(started_at):
        return {"kind": "duration", "running": False, "elapsed": False,
                "fired": False, "remaining_s": None}
    passed = now - started_at
    elapsed = passed >= duration_s
    fired = False
    if elapsed and not slot.get("elapsed_fired"):
        slot["elapsed_fired"] = True
        fired = True
    remaining = duration_s - passed
    if remaining < 0.0:
        remaining = 0.0
    return {"kind": "duration", "running": not elapsed, "elapsed": elapsed,
            "fired": fired, "remaining_s": remaining}


def _timer_status_interval(slot, interval_s, now):
    """Deterministic status of a free running interval timer.

    The anchor is set at first evaluation; multiple missed periods
    collapse into one "fired" (documented catch up behaviour).
    """
    if not _is_number(interval_s) or interval_s <= 0:
        return {"kind": "interval", "running": False, "elapsed": False,
                "fired": False, "remaining_s": None}
    anchor = slot.get("anchor")
    if not _is_number(anchor):
        slot["anchor"] = now
        slot["last_k"] = 0
        return {"kind": "interval", "running": True, "elapsed": False,
                "fired": False, "remaining_s": interval_s}
    period_count = int((now - anchor) // interval_s)
    fired = period_count > slot.get("last_k", 0)
    slot["last_k"] = period_count
    remaining = (anchor + (period_count + 1) * interval_s) - now
    return {"kind": "interval", "running": True, "elapsed": False,
            "fired": fired, "remaining_s": remaining}


def _timer_status_stamped(slot, kind, matched, tm):
    """Shared status logic for minute stamped triggers ("at" and "cron")."""
    stamp = "%04d-%03d-%02d-%02d" % (tm.tm_year, tm.tm_yday, tm.tm_hour, tm.tm_min)
    fired = False
    if matched and slot.get("last_stamp") != stamp:
        slot["last_stamp"] = stamp
        fired = True
    return {"kind": kind, "running": True, "elapsed": False,
            "fired": fired, "remaining_s": None}


def timers_evaluate(context, now=None, timer_configs=None):
    """Evaluate all timers deterministically for the given time.

    timer_configs: {timer_id: cfg} mapping; defaults to the bound
    dataset timers. The returned status dict is directly usable as
    facts["timers"] for the rule evaluation.
    """
    resolved_now = _resolve_now(context, {}, now)
    if timer_configs is None:
        dataset = context.get("dataset")
        timer_configs = dataset.get("timers", {}) if isinstance(dataset, dict) else {}
    status = {}
    with context["lock"]:
        runtime = context["runtime"]["timers"]
        for timer_id in sorted(timer_configs.keys(), key=_stable_id_key):
            cfg = timer_configs[timer_id]
            if not isinstance(cfg, dict):
                continue
            slot_key = str(timer_id)
            if slot_key not in runtime:
                runtime[slot_key] = {}
            slot = runtime[slot_key]
            try:
                if _is_number(cfg.get("duration_s")):
                    status[timer_id] = _timer_status_duration(
                        slot, cfg["duration_s"], resolved_now)
                elif _is_number(cfg.get("interval_s")):
                    status[timer_id] = _timer_status_interval(
                        slot, cfg["interval_s"], resolved_now)
                elif "at" in cfg:
                    tm = context["localtime"](resolved_now)
                    hhmm = _parse_hhmm(cfg.get("at"))
                    matched = (hhmm is not None
                               and tm.tm_hour == hhmm[0]
                               and tm.tm_min == hhmm[1])
                    status[timer_id] = _timer_status_stamped(slot, "at", matched, tm)
                elif "cron" in cfg:
                    tm = context["localtime"](resolved_now)
                    matched = _cron_match(cfg.get("cron"), tm)
                    status[timer_id] = _timer_status_stamped(slot, "cron", matched, tm)
                else:
                    status[timer_id] = {"kind": "unknown", "running": False,
                                        "elapsed": False, "fired": False,
                                        "remaining_s": None}
            except Exception:
                LOGGER.exception("timer %r evaluation failed", timer_id)
                status[timer_id] = {"kind": "error", "running": False,
                                    "elapsed": False, "fired": False,
                                    "remaining_s": None}
    return status


# =============================================================================
# CODEBASE - DATASET LOADING & ENGINE BINDING (v5 CONFIG COMPATIBILITY)
# =============================================================================

def _index_rows(rows, key_field):
    """Index a list of row dicts by one key field (deterministic)."""
    index = {}
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict) and key_field in row:
                index[row[key_field]] = row
    return index


def load_dataset(source):
    """Load and index a configuration dataset.

    source: dict, JSON text or a file path. Returns a dataset index
    with engines, rules, timers, filters, states, states_by_pid and
    process_values. Unknown sections stay reachable via dataset["raw"].
    """
    raw = None
    try:
        if isinstance(source, dict):
            raw = source
        elif isinstance(source, str):
            stripped = source.strip()
            if stripped.startswith("{") or stripped.startswith("["):
                raw = json.loads(source)
            else:
                handle = open(source, "r", encoding="utf-8")
                try:
                    raw = json.load(handle)
                finally:
                    handle.close()
    except (OSError, ValueError):
        LOGGER.exception("dataset load failed")
        raw = None
    if not isinstance(raw, dict):
        return {"raw": {}, "engines": {}, "rules": [], "timers": {},
                "filters": {}, "states": {}, "states_by_pid": {},
                "process_values": {}}
    states = _index_rows(raw.get("process_states"), "state_id")
    states_by_pid = {}
    for state_id in sorted(states.keys(), key=_stable_id_key):
        row = states[state_id]
        process_id = row.get("p_id")
        if process_id is None:
            continue
        if process_id not in states_by_pid:
            states_by_pid[process_id] = []
        states_by_pid[process_id].append(state_id)
    for process_id in states_by_pid:
        states_by_pid[process_id] = sorted(states_by_pid[process_id],
                                           key=_stable_id_key)
    rules = raw.get("rules")
    return {
        "raw": raw,
        "engines": _index_rows(raw.get("rule_engine"), "engine_id"),
        "rules": rules if isinstance(rules, list) else [],
        "timers": _index_rows(raw.get("timers"), "timer_id"),
        "filters": _index_rows(raw.get("filters"), "filter_id"),
        "states": states,
        "states_by_pid": states_by_pid,
        "process_values": _index_rows(raw.get("process_values"), "value_id"),
    }


def _absorb_engine_cfg(context, engine_cfg):
    """Map a v5 "rule_engine" configuration row onto the context config."""
    cfg = context["config"]
    context["engine_cfg"] = copy.deepcopy(engine_cfg)
    context["engine_id"] = engine_cfg.get("engine_id")
    cfg["engine_enabled"] = bool(engine_cfg.get("enabled", True))
    if _is_number(engine_cfg.get("sp_tolerance")):
        cfg["sp_tolerance"] = engine_cfg["sp_tolerance"]
    if "required_mode_id" in engine_cfg:
        cfg["required_mode_id"] = engine_cfg["required_mode_id"]
    if isinstance(engine_cfg.get("writable_fields"), list):
        cfg["writable_fields"] = list(engine_cfg["writable_fields"])
    defaults = engine_cfg.get("defaults")
    if isinstance(defaults, dict) and _is_number(defaults.get("priority")):
        cfg["default_priority"] = defaults["priority"]
    runtime = engine_cfg.get("runtime")
    if isinstance(runtime, dict):
        if _is_number(runtime.get("max_signal_age_s")):
            cfg["max_signal_age_s"] = runtime["max_signal_age_s"]
        if _is_number(runtime.get("coordinator_min_priority")):
            cfg["coordinator_min_priority"] = runtime["coordinator_min_priority"]
    safety = engine_cfg.get("safety")
    if isinstance(safety, dict) and isinstance(safety.get("on_missing_sensor"), dict):
        cfg["safety_on_missing"] = dict(safety["on_missing_sensor"])


def engine_bind_dataset(context, dataset, engine_id=None):
    """Bind a loaded dataset to the context and register its rules.

    engine_id selects one "rule_engine" row; when omitted the single
    (or lowest id) engine is chosen. Rules of other engines are skipped.
    Returns a deterministic binding report.
    """
    report = {"engine_id": None, "added": [], "skipped": [], "issues": {}}
    if not isinstance(dataset, dict):
        return report
    with context["lock"]:
        context["dataset"] = dataset
        engines = dataset.get("engines", {})
        selected_id = engine_id
        if selected_id is None and engines:
            selected_id = sorted(engines.keys(), key=_stable_id_key)[0]
        if selected_id is not None:
            found, engine_cfg = _multi_get(engines, selected_id)
            if found:
                _absorb_engine_cfg(context, engine_cfg)
        report["engine_id"] = context.get("engine_id")
        for rule in dataset.get("rules", []):
            rule_id = rule.get("rule_id") if isinstance(rule, dict) else None
            rule_engine_id = rule.get("engine_id") if isinstance(rule, dict) else None
            if (rule_engine_id is not None and selected_id is not None
                    and str(rule_engine_id) != str(selected_id)):
                report["skipped"].append(rule_id)
                continue
            issues = add_rule(context, rule)
            if _has_fatal(issues):
                report["skipped"].append(rule_id)
            else:
                report["added"].append(rule_id)
            if issues:
                report["issues"][str(rule_id)] = issues
    return report


# =============================================================================
# CODEBASE - DEEP DIAGNOSIS
# =============================================================================

def engine_diagnose(context):
    """Produce a deep deterministic diagnosis report of the engine:
    rule inventory, operator/source coverage, feature usage, duplicate
    ids, dataset reference integrity and writable field violations."""
    with context["lock"]:
        report = {
            "engine_id": context.get("engine_id"),
            "engine_enabled": bool(context["config"].get("engine_enabled", True)),
            "rule_count": len(context["rules"]),
            "enabled_rule_count": 0,
            "groups": {},
            "priorities": [],
            "operators_used": [],
            "operators_unknown": [],
            "sources_used": [],
            "sources_unknown": [],
            "features": {"until": 0, "latch": 0, "cooldown_s": 0,
                         "filter_id": 0, "param_of_state": 0, "else_do": 0},
            "duplicate_rule_ids": [],
            "references": {"timers_missing": [], "filters_missing": [],
                           "states_missing": [], "processes_missing": []},
            "writable_field_violations": [],
            "issues": [],
        }
        operators_used = {}
        sources_used = {}
        priorities = {}
        seen_ids = {}
        dataset = context.get("dataset")
        has_dataset = isinstance(dataset, dict)
        writable = context["config"].get("writable_fields")
        for rule in _sorted_rules(context):
            rule_id = rule.get("rule_id")
            if rule_id in seen_ids:
                report["duplicate_rule_ids"].append(rule_id)
            seen_ids[rule_id] = True
            if rule.get("enabled", True):
                report["enabled_rule_count"] += 1
            group = rule.get("group", "")
            report["groups"][group] = report["groups"].get(group, 0) + 1
            priorities[rule.get("priority")] = True
            if rule.get("until") is not None:
                report["features"]["until"] += 1
            if rule.get("latch"):
                report["features"]["latch"] += 1
            if rule.get("else_do"):
                report["features"]["else_do"] += 1
            leaves = []
            _collect_leaves(rule.get("when"), leaves)
            _collect_leaves(rule.get("until"), leaves)
            for leaf in leaves:
                op_name = leaf.get("op", "==")
                operators_used[op_name] = True
                if op_name not in context["operators"]:
                    if op_name not in report["operators_unknown"]:
                        report["operators_unknown"].append(op_name)
                source_name = leaf.get("src", context["config"]["default_src"])
                sources_used[source_name] = True
                if source_name not in context["sources"]:
                    if source_name not in report["sources_unknown"]:
                        report["sources_unknown"].append(source_name)
                if _is_number(leaf.get("cooldown_s")):
                    report["features"]["cooldown_s"] += 1
                if leaf.get("filter_id") is not None:
                    report["features"]["filter_id"] += 1
                    if has_dataset:
                        found, _unused = _multi_get(dataset.get("filters", {}),
                                                    leaf.get("filter_id"))
                        if not found:
                            report["references"]["filters_missing"].append(
                                {"rule_id": rule_id, "filter_id": leaf.get("filter_id")})
                if leaf.get("src") == "timer" and has_dataset:
                    found, _unused = _multi_get(dataset.get("timers", {}),
                                                leaf.get("timer_id"))
                    if not found:
                        report["references"]["timers_missing"].append(
                            {"rule_id": rule_id, "timer_id": leaf.get("timer_id")})
                if leaf.get("src") == "state" and has_dataset:
                    found, _unused = _multi_get(dataset.get("states", {}),
                                                leaf.get("state_id"))
                    if not found:
                        report["references"]["states_missing"].append(
                            {"rule_id": rule_id, "state_id": leaf.get("state_id")})
                value_spec = leaf.get("value")
                if isinstance(value_spec, dict) and "param_of_state" in value_spec:
                    report["features"]["param_of_state"] += 1
                    if has_dataset:
                        found, _unused = _multi_get(dataset.get("states", {}),
                                                    value_spec.get("param_of_state"))
                        if not found:
                            report["references"]["states_missing"].append(
                                {"rule_id": rule_id,
                                 "state_id": value_spec.get("param_of_state")})
            for action in rule.get("do", []) + rule.get("else_do", []):
                if has_dataset and "state_ids" in action:
                    for state_id in action.get("state_ids", []):
                        found, _unused = _multi_get(dataset.get("states", {}), state_id)
                        if not found:
                            report["references"]["states_missing"].append(
                                {"rule_id": rule_id, "state_id": state_id})
                if has_dataset and "p_ids" in action:
                    for process_id in action.get("p_ids", []):
                        found, _unused = _multi_get(dataset.get("states_by_pid", {}),
                                                    process_id)
                        if not found:
                            report["references"]["processes_missing"].append(
                                {"rule_id": rule_id, "p_id": process_id})
                if isinstance(writable, list) and (
                        "state_ids" in action or "p_ids" in action):
                    for field in sorted(action.keys()):
                        if field in ("state_ids", "p_ids"):
                            continue
                        if field not in writable:
                            report["writable_field_violations"].append(
                                {"rule_id": rule_id, "field": field})
        report["operators_used"] = sorted(operators_used.keys())
        report["sources_used"] = sorted(sources_used.keys())
        report["priorities"] = sorted(priorities.keys(), key=_stable_id_key)
        if report["duplicate_rule_ids"]:
            report["issues"].append("duplicate rule ids present")
        if report["operators_unknown"]:
            report["issues"].append("unknown operators referenced")
        if report["sources_unknown"]:
            report["issues"].append("unknown sources referenced")
        for reference_key in sorted(report["references"].keys()):
            if report["references"][reference_key]:
                report["issues"].append("unresolved references: %s" % reference_key)
        if report["writable_field_violations"]:
            report["issues"].append("writable_fields violations present")
    return report


# =============================================================================
# CODEBASE - SIMULATION & DETERMINISM PROOF
# =============================================================================

def simulate(context, steps):
    """Run a scripted sequence of steps and collect all decisions.

    Each step is a dict: {"facts": {...}, "now": float}. The engine
    runtime advances exactly as it would in production, which makes
    simulate() the reference tool for tests and determinism proofs.
    """
    decisions = []
    if not isinstance(steps, list):
        return decisions
    for step in steps:
        if not isinstance(step, dict):
            continue
        decisions.append(evaluate_rules(context, step.get("facts", {}),
                                        step.get("now")))
    return decisions


def decisions_fingerprint(decisions):
    """Compute a canonical SHA-256 fingerprint over decision output.

    Two runs with identical inputs MUST produce identical fingerprints;
    this is the executable determinism contract of the library.
    """
    return type_library.fingerprint(decisions)


# =============================================================================
# INTERFACE - EXTERNAL CALLABLE
# =============================================================================
# Deterministic, flat public interface of the library. Host modules may
# import single functions or dispatch dynamically through this table.

INTERFACE = {
    # context & registries
    "create_engine_context": create_engine_context,
    "register_operator": register_operator,
    "register_source": register_source,
    "register_value_resolver": register_value_resolver,
    "register_action_handler": register_action_handler,
    # rule management
    "compile_rule": compile_rule,
    "validate_rule": validate_rule,
    "add_rule": add_rule,
    "remove_rule": remove_rule,
    "clear_rules": clear_rules,
    "get_rule": get_rule,
    "list_rules": list_rules,
    "set_rule_enabled": set_rule_enabled,
    # evaluation
    "resolve_value": resolve_value,
    "evaluate_rule": evaluate_rule,
    "evaluate_rules": evaluate_rules,
    "execute_actions": execute_actions,
    # latch & runtime control
    "reset_rule_latch": reset_rule_latch,
    "reset_all_latches": reset_all_latches,
    "get_rule_runtime": get_rule_runtime,
    "engine_export_runtime": engine_export_runtime,
    "engine_import_runtime": engine_import_runtime,
    "engine_stats": engine_stats,
    # timers
    "timer_start": timer_start,
    "timer_stop": timer_stop,
    "timer_reset": timer_reset,
    "timers_evaluate": timers_evaluate,
    # dataset & diagnosis
    "load_dataset": load_dataset,
    "engine_bind_dataset": engine_bind_dataset,
    "engine_diagnose": engine_diagnose,
    # simulation & determinism
    "simulate": simulate,
    "decisions_fingerprint": decisions_fingerprint,
}
