"""Rule packs: ``.lineagepilot/rules.yml`` in the workspace.

Custom rules live in a file, so they persist by construction (review finding F10). The
file is untrusted input: it is size-capped, validated field by field, and its regexes
go through ``SafeRegex``. A problem costs the rule it is in, never the built-in rules,
and is reported. In a restricted (untrusted) workspace the language server does not load
packs at all (design section 10).

Format::

    version: 1
    disable: [DBT_003]                # built-in or custom rule IDs
    severity: {DBT_001: error}        # overrides
    rules:
      - id: ACME_001                  # upper case, digits and _; not a built-in ID
        name: Finance marts are prefixed
        description: ...              # optional
        severity: warning             # critical, error, warning (default) or info
        scope: node                   # node (default) or column
        resource_types: [model]       # model (default), seed, snapshot, source, exposure
        when:                         # all must hold, or the rule does not apply
          layer: [gold]
          tags: [finance]             # any of
          materialization: [table, incremental]
          path_matches: "^models/finance/"
          name_matches: "^fin_"
        require:                      # all must hold, or the rule fails
          name_matches: "^fin_(dim|fct)_"
          description: true
          tests: true
          test_types: [unique]        # each must be present
          owner: true
          domain: true
          meta_keys: [sla]
          tags: [certified]           # each must be present
        remediation: ...              # optional

An unknown key under ``require`` or ``when`` does not drop the rule: its results are
``unsupported``, so coverage shows that it was not checked.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from lineagepilot_core.graph.model import Column, MetadataGraph, Node, NodeKind
from lineagepilot_core.rules.model import (
    NOT_APPLICABLE,
    PASS,
    Category,
    Outcome,
    Result,
    Rule,
    Scope,
    Severity,
)
from lineagepilot_core.rules.safe_regex import SafeRegex, UnsafePattern

MAX_PACK_BYTES = 256 * 1024
MAX_RULES = 200
DEFAULT_PATH = Path(".lineagepilot") / "rules.yml"

_RULE_ID = re.compile(r"^[A-Z][A-Z0-9_]{1,39}$")
_RESOURCE_TYPES = {
    k.value: k
    for k in (NodeKind.MODEL, NodeKind.SEED, NodeKind.SNAPSHOT, NodeKind.SOURCE, NodeKind.EXPOSURE)
}
_WHEN_KEYS = {"layer", "tags", "materialization", "path_matches", "name_matches"}
_REQUIRE_KEYS = {
    "name_matches",
    "description",
    "tests",
    "test_types",
    "owner",
    "domain",
    "meta_keys",
    "tags",
}
_RULE_KEYS = {
    "id",
    "name",
    "description",
    "severity",
    "scope",
    "resource_types",
    "category",
    "when",
    "require",
    "remediation",
}


@dataclass
class RulePack:
    path: str
    rules: list[Rule] = field(default_factory=list)
    disabled: set[str] = field(default_factory=set)
    severities: dict[str, Severity] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)


def load_rule_pack(path: str | Path, builtin_ids: set[str] | None = None) -> RulePack:
    """Read a rule pack. Problems are collected in ``RulePack.problems``, never raised."""
    path = Path(path)
    pack = RulePack(path=str(path))
    try:
        size = path.stat().st_size
        if size > MAX_PACK_BYTES:
            pack.problems.append(f"{path}: larger than {MAX_PACK_BYTES // 1024} KB; not loaded")
            return pack
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return pack
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as e:
        pack.problems.append(f"{path}: cannot be read: {' '.join(str(e).split())[:300]}")
        return pack
    return parse_rule_pack(data, str(path), builtin_ids or set(), pack)


def parse_rule_pack(
    data: Any, source: str, builtin_ids: set[str], pack: RulePack | None = None
) -> RulePack:
    pack = pack or RulePack(path=source)
    if data is None:
        return pack
    if not isinstance(data, dict):
        pack.problems.append(f"{source}: the top level must be a mapping")
        return pack

    for key in sorted(set(data) - {"version", "disable", "severity", "rules"}, key=str):
        pack.problems.append(f"{source}: unknown key {key!r} ignored")
    if data.get("version", 1) != 1:
        pack.problems.append(f"{source}: version {data.get('version')!r} is not supported")
        return pack

    disable = data.get("disable", [])
    if _is_str_list(disable):
        pack.disabled = set(disable)
    else:
        pack.problems.append(f"{source}: 'disable' must be a list of rule IDs")

    severities = data.get("severity", {})
    if isinstance(severities, dict):
        for rule_id, level in severities.items():
            try:
                pack.severities[str(rule_id)] = Severity(level)
            except ValueError:
                pack.problems.append(f"{source}: severity of {rule_id}: {level!r} is not valid")
    else:
        pack.problems.append(f"{source}: 'severity' must be a mapping of rule ID to severity")

    rules = data.get("rules", [])
    if not isinstance(rules, list):
        pack.problems.append(f"{source}: 'rules' must be a list")
        return pack
    if len(rules) > MAX_RULES:
        pack.problems.append(f"{source}: only the first {MAX_RULES} rules are loaded")
    seen = set(builtin_ids)
    for index, entry in enumerate(rules[:MAX_RULES]):
        try:
            rule = _rule(entry, source)
        except _Invalid as e:
            pack.problems.append(f"{source}: rule {index + 1}: {e}")
            continue
        if rule.id in seen:
            pack.problems.append(f"{source}: rule {index + 1}: ID {rule.id} is already used")
            continue
        seen.add(rule.id)
        pack.rules.append(rule)
    return pack


class _Invalid(ValueError):
    pass


def _is_str_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(v, str) for v in value)


def _rule(entry: Any, source: str) -> Rule:
    if not isinstance(entry, dict):
        raise _Invalid("each rule must be a mapping")
    rule_id = entry.get("id")
    if not isinstance(rule_id, str) or not _RULE_ID.match(rule_id):
        raise _Invalid("'id' must be upper case letters, digits and _, starting with a letter")
    name = entry.get("name")
    if not isinstance(name, str) or not name.strip():
        raise _Invalid(f"{rule_id}: 'name' is required")
    for key in ("description", "remediation"):
        if key in entry and not isinstance(entry[key], str):
            raise _Invalid(f"{rule_id}: '{key}' must be text")
    try:
        severity = Severity(entry.get("severity", "warning"))
        scope = Scope(entry.get("scope", "node"))
        category = Category(entry.get("category", "custom"))
    except ValueError as e:
        raise _Invalid(f"{rule_id}: {e}") from None
    types = entry.get("resource_types", ["model"])
    if not _is_str_list(types) or not types or any(t not in _RESOURCE_TYPES for t in types):
        raise _Invalid(f"{rule_id}: 'resource_types' must list some of {sorted(_RESOURCE_TYPES)}")
    if scope is Scope.COLUMN and "exposure" in types:
        raise _Invalid(f"{rule_id}: exposures have no columns")

    unsupported = sorted(str(k) for k in set(entry) - _RULE_KEYS)
    when = entry.get("when", {}) or {}
    require = entry.get("require", {}) or {}
    if not isinstance(when, dict) or not isinstance(require, dict):
        raise _Invalid(f"{rule_id}: 'when' and 'require' must be mappings")
    if not require and not unsupported:
        raise _Invalid(f"{rule_id}: 'require' must state at least one check")
    unsupported += [f"when.{k}" for k in sorted(set(when) - _WHEN_KEYS, key=str)]
    unsupported += [f"require.{k}" for k in sorted(set(require) - _REQUIRE_KEYS, key=str)]

    check = (
        _unsupported_check(unsupported)
        if unsupported
        else _custom_check(
            rule_id, scope, _conditions(rule_id, when), _requirements(rule_id, require)
        )
    )
    return Rule(
        id=rule_id,
        name=name.strip(),
        description=entry.get("description", name).strip(),
        severity=severity,
        category=category,
        scope=scope,
        check=check,
        resource_types=frozenset(_RESOURCE_TYPES[t] for t in types),
        rule_set="custom",
        remediation=entry.get("remediation"),
        prefer_yaml=True,
        source=source,
    )


def _regex(rule_id: str, key: str, value: Any) -> SafeRegex:
    try:
        return SafeRegex(value)
    except UnsafePattern as e:
        raise _Invalid(f"{rule_id}: {key}: {e}") from None


def _conditions(rule_id: str, when: dict[str, Any]) -> dict[str, Any]:
    conditions: dict[str, Any] = {}
    for key in ("layer", "tags", "materialization"):
        if key in when:
            if not _is_str_list(when[key]):
                raise _Invalid(f"{rule_id}: when.{key} must be a list of text")
            conditions[key] = {v.lower() for v in when[key]}
    for key in ("path_matches", "name_matches"):
        if key in when:
            conditions[key] = _regex(rule_id, f"when.{key}", when[key])
    return conditions


def _requirements(rule_id: str, require: dict[str, Any]) -> dict[str, Any]:
    requirements: dict[str, Any] = {}
    for key in ("description", "tests", "owner", "domain"):
        if key in require:
            if not isinstance(require[key], bool):
                raise _Invalid(f"{rule_id}: require.{key} must be true or false")
            if require[key]:
                requirements[key] = True
    for key in ("test_types", "meta_keys", "tags"):
        if key in require:
            if not _is_str_list(require[key]):
                raise _Invalid(f"{rule_id}: require.{key} must be a list of text")
            requirements[key] = list(require[key])
    if "name_matches" in require:
        requirements["name_matches"] = _regex(
            rule_id, "require.name_matches", require["name_matches"]
        )
    return requirements


def _unsupported_check(keys: list[str]):
    message = f"uses {', '.join(keys)}, which this version of LineagePilot does not support"

    def check(graph: MetadataGraph, node: Node, column: Column | None) -> Outcome:
        return Outcome(Result.UNSUPPORTED, message)

    return check


def _custom_check(rule_id: str, scope: Scope, when: dict[str, Any], require: dict[str, Any]):
    def check(graph: MetadataGraph, node: Node, column: Column | None) -> Outcome:
        subject: Node | Column = column if scope is Scope.COLUMN and column else node
        name = subject.name

        if "layer" in when and (node.layer or "").lower() not in when["layer"]:
            return NOT_APPLICABLE
        if "tags" in when and not when["tags"] & {t.lower() for t in subject.tags}:
            return NOT_APPLICABLE
        if (
            "materialization" in when
            and (node.materialization or "").lower() not in when["materialization"]
        ):
            return NOT_APPLICABLE
        for key, value in (("path_matches", node.file_path or ""), ("name_matches", name)):
            if key in when:
                matched = when[key].search(value)
                if matched is None:
                    return Outcome(Result.UNKNOWN, f"when.{key} could not be evaluated safely")
                if not matched:
                    return NOT_APPLICABLE

        missing: list[str] = []
        if "name_matches" in require:
            matched = require["name_matches"].search(name)
            if matched is None:
                return Outcome(Result.UNKNOWN, "require.name_matches could not be evaluated safely")
            if not matched:
                missing.append(f"name '{name}' does not match {require['name_matches'].pattern!r}")
        if require.get("description") and not (subject.description or "").strip():
            missing.append("no description")
        if require.get("tests") and not subject.has_tests:
            missing.append("no tests")
        if require.get("owner") and not (node.owner or node.group):
            missing.append("no meta.owner or group")
        if require.get("domain") and not isinstance((node.meta or {}).get("domain"), str):
            missing.append("no meta.domain")
        for kind in require.get("test_types", []):
            if kind not in subject.test_types:
                missing.append(f"no {kind} test")
        for key in require.get("meta_keys", []):
            if key not in (subject.meta or {}):
                missing.append(f"no meta.{key}")
        for tag in require.get("tags", []):
            if tag not in subject.tags:
                missing.append(f"no tag {tag}")

        if missing:
            return Outcome(Result.FAIL, f"{rule_id}: {'; '.join(missing)}")
        return PASS

    return check
