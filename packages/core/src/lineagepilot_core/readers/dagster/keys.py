"""The Dagster asset key and group of each dbt node a definition materialises (tasks 8.5, 8.6).

The key is dagster-dbt 0.29.29's, from the manifest. ``scripts/dagster_key_conformance.py`` checks
it against dagster-dbt itself. Its default rule (``default_asset_key_fn``):

- ``meta.dagster.asset_key``, when set. The meta is the node's ``config.meta``, or its ``meta``
  when ``config.meta`` is empty. A list is the key's parts, and a string is one part;
- otherwise, for a source, ``[source_name, name]``;
- for a versioned model, ``[alias]``;
- for any other node, ``[config.schema, name]`` when its config sets a schema, or else ``[name]``.

What can change it, and is read:

- a component's ``translation``: ``key``, a template that is literal text and ``{{ node.<field> }}``
  placeholders, split on ``/`` as Dagster splits a key written as a string; and ``key_prefix``, a
  string or a list put in front of the key;
- the ``key_prefix`` of the legacy loaders (D-c), which applies to what they materialise.

A node's group, which ``AssetSelection.groups`` and ``group:`` select by (task 8.6), is
dagster-dbt's ``default_group_from_dbt_resource_props``: ``meta.dagster.group``, from the node's
``meta`` alone, then the dbt group, ``config.group``; else Dagster's ``default``. A component's
``translation.group_name`` replaces it, written as a key template is.

What cannot be read is the unknown ``custom_translator``: a ``dagster_dbt_translator`` other than
``DagsterDbtTranslator`` itself, a legacy loader's ``node_info_to_asset_key``, and any other
template. The definition's links still stand, because its selection says what it runs; only
matching by key is lost.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from lineagepilot_core.readers.common.manifest import Manifest, Node

# A key part this long is not a name: Dagster's own limit is far below it
MAX_PART = 1000
MAX_PARTS = 50
_PLACEHOLDER = re.compile(r"\{\{\s*node((?:\.[A-Za-z_][A-Za-z0-9_]*)+)\s*\}\}")
_TEMPLATE_SYNTAX = ("{{", "{%", "{#", "}}", "%}", "#}")
# A node's fields a key template can name, beyond its config and meta
_FIELDS = (
    "name",
    "alias",
    "schema",
    "database",
    "resource_type",
    "package_name",
    "unique_id",
    "original_file_path",
    "source_name",
)


# Dagster's group for an asset that has none
DEFAULT_GROUP = "default"
# Why a legacy loader's groups are not read: dagster-dbt 0.29.29 no longer has the loaders (D-c)
LEGACY_GROUPS = "it is a legacy loader, whose default groups changed between dagster-dbt versions"


@dataclass(frozen=True)
class KeyRule:
    """What sets a definition's asset keys and groups: dagster-dbt's default, a component's
    templates, a prefix, or something that cannot be read, said in ``unknown``."""

    template: str | None = None
    prefix: tuple[str, ...] = ()
    unknown: str | None = None
    group: str | None = None  # a component's translation.group_name, a template


DEFAULT = KeyRule()


def default_key(node: Node) -> list[str] | None:
    """dagster-dbt 0.29.29's ``default_asset_key_fn``; None when the node's meta sets a key that
    is not a string or a list of strings, which Dagster would refuse."""
    config_meta = node.config.get("meta")
    meta = config_meta if isinstance(config_meta, dict) and config_meta else node.meta
    dagster = meta.get("dagster") if isinstance(meta, dict) else None
    configured = dagster.get("asset_key") if isinstance(dagster, dict) else None
    if configured:
        return _parts(configured)
    if node.resource_type == "source":
        return [node.source_name or "", node.name]
    if node.version:
        return [node.alias or node.name]
    schema = node.config.get("schema")
    if schema is not None:
        return [str(schema), node.name]
    return [node.name]


def default_group(node: Node) -> str:
    """dagster-dbt 0.29.29's ``default_group_from_dbt_resource_props``, or Dagster's default."""
    dagster = node.meta.get("dagster") if isinstance(node.meta, dict) else None
    group = dagster.get("group") if isinstance(dagster, dict) else None
    if group:
        return str(group)
    group = node.config.get("group")
    return str(group) if group else DEFAULT_GROUP


def _parts(value: Any) -> list[str] | None:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and value and all(isinstance(p, str) for p in value):
        return list(value)
    return None


def prefix_of(value: Any) -> tuple[str, ...] | None:
    """A ``key_prefix`` as Dagster coerces it: a string is one part, a list is the parts. None if
    it is neither."""
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)) and all(isinstance(p, str) for p in value):
        return tuple(value)
    return None


def _field(node: Node, path: list[str]) -> str | None:
    """``node.<a>.<b>`` in a template, as text, or None when it is not a plain value."""
    first, rest = path[0], path[1:]
    if first in _FIELDS:
        value: Any = getattr(node, first)
    elif first == "config":
        value = node.config
    elif first == "meta":
        value = node.meta
    else:
        return None
    for part in rest:
        value = value.get(part) if isinstance(value, dict) else None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    return str(value)


def check_template(template: str) -> str | None:
    """Why a key template cannot be filled in, or None if it can be: literal text and
    ``{{ node.<field> }}`` placeholders only."""
    rest = _PLACEHOLDER.sub("", template)
    if any(mark in rest for mark in _TEMPLATE_SYNTAX):
        return f"the key template {template[:200]!r} uses more than node fields"
    return None


def render(template: str, node: Node) -> list[str] | None:
    """The key a template gives a node, split on ``/``; None if a field it names is not one."""
    missing = False

    def fill(found: re.Match[str]) -> str:
        nonlocal missing
        value = _field(node, found.group(1).split(".")[1:])
        if value is None:
            missing = True
            return ""
        return value

    text = _PLACEHOLDER.sub(fill, template)
    return None if missing else text.split("/")


def asset_groups(manifest: Manifest, ids: list[str], rule: KeyRule) -> dict[str, str]:
    """The group of each node in ``ids``, by ``rule``. A template that names a field a node does
    not have leaves that node out."""
    groups: dict[str, str] = {}
    for uid in ids:
        node = manifest.nodes.get(uid)
        if node is None:
            continue
        if rule.group is None:
            groups[uid] = default_group(node)
            continue
        rendered = render(rule.group, node)
        if rendered is not None:
            groups[uid] = "/".join(rendered)
    return groups


def asset_keys(
    manifest: Manifest, ids: list[str], rule: KeyRule
) -> tuple[dict[str, list[str]], str | None]:
    """The key of each node in ``ids``, by ``rule``; and why some or all cannot be given."""
    keys: dict[str, list[str]] = {}
    refused: list[str] = []
    for uid in ids:
        node = manifest.nodes.get(uid)
        if node is None:
            continue
        key = render(rule.template, node) if rule.template is not None else default_key(node)
        if key is None:
            refused.append(node.name)
            continue
        key = [*rule.prefix, *key]
        if len(key) > MAX_PARTS or any(len(part) > MAX_PART for part in key):
            refused.append(node.name)
            continue
        keys[uid] = key
    if not refused:
        return keys, None
    names = ", ".join(sorted(refused)[:5]) + (" and others" if len(refused) > 5 else "")
    why = (
        f"the key template names a field that {names} do not have"
        if rule.template is not None
        else f"the meta.dagster.asset_key of {names} is not a string or a list of strings"
    )
    return keys, why
