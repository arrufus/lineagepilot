"""Which dbt project a dbt asset definition is for, and the nodes its selection picks (tasks 8.3
and 8.4).

A component in ``defs.yaml`` and a Python definition say the same things in different syntax: a
dbt project, and dbt's ``select``, ``exclude`` and ``selector``. Both are resolved here, the same
way. A path is matched to a dbt project in the workspace by the shared ``ProjectIndex``. The
selection runs over that project's manifest through the shared selector engine, over the
resource types a dbt asset definition makes assets of. Each record ends ``resolved``, ``unknown``
with a category and a reason, or is dropped because it is another dbt project's. A resolved record
has the asset key of each node it selects (``keys``, task 8.5), or says why they are not known.
"""

from __future__ import annotations

import os
from typing import Any

from lineagepilot_core.readers.common.categories import UnknownCategory
from lineagepilot_core.readers.common.projects import ProjectIndex, ProjectMatch
from lineagepilot_core.readers.common.selector import select as run_select
from lineagepilot_core.readers.common.values import weakest
from lineagepilot_core.readers.dagster.keys import DEFAULT, KeyRule, asset_groups, asset_keys

# dagster-dbt 0.29.29's defaults (asset_utils, DBT_DEFAULT_SELECT and its neighbours)
DEFAULT_SELECT = "fqn:*"
# What a dbt asset definition makes assets of
ASSET_TYPES = ("model", "seed", "snapshot")
# A selection string this long is not one anybody wrote
MAX_SELECTION = 10_000
SELECTION_KEYS = ("select", "exclude", "selector")


def unknown(record: dict[str, Any], category: UnknownCategory, reason: str) -> dict[str, Any]:
    record.update(status="unknown", category=category.value, reason=reason)
    return record


def match_project(
    record: dict[str, Any], raw: str, base_dir: str | None, index: ProjectIndex, root: str
) -> ProjectMatch | None:
    """The dbt project ``raw`` names, relative to ``base_dir``. None when the record is now an
    unknown (no project matches), or when the project is another one in the workspace, which its
    own language server links: ``record["status"]`` tells the two apart."""
    match = index.match(raw, base_dir=base_dir)
    if match.path is None:
        reason = match.reason or f"{raw!r} matches no dbt project"
        unknown(record, UnknownCategory.NO_PROJECT, reason)
        return None
    if os.path.realpath(match.path) != os.path.realpath(root):
        record["status"] = "other_project"
        return None
    if match.note:
        record["notes"].append(match.note)
    return match


def select_nodes(
    record: dict[str, Any],
    match: ProjectMatch,
    selection: dict[str, str],
    index: ProjectIndex,
    conf: str = "inferred",
    rule: KeyRule = DEFAULT,
) -> dict[str, Any]:
    """The record, resolved with the nodes ``selection`` picks in the project's manifest, or an
    unknown that says why it cannot be. ``conf`` is how sure the values it was read from are, and
    ``rule`` is what sets the asset keys."""
    record.update(selection)
    choice = index.manifest_for(match.path or "")
    if choice.manifest is None:
        return unknown(record, UnknownCategory.NO_PROJECT, choice.reason or "no manifest")
    record["notes"].extend(choice.notes)
    found = run_select(
        choice.manifest,
        selection["select"].split() or None,
        selection["exclude"].split() or None,
        selection["selector"] or None,
        resource_types=ASSET_TYPES,
    )
    if found.unknown:
        reason = found.reason or "the selection"
        return unknown(record, UnknownCategory.UNSUPPORTED_SELECTOR, reason)
    record["notes"].extend(found.notes)
    record["status"] = "resolved"
    record["nodes"] = sorted(found.ids or ())
    record["confidence"] = weakest("inferred", conf, match.conf, choice.conf)
    if rule.unknown is not None:
        record["keys_unknown"] = {
            "category": UnknownCategory.CUSTOM_TRANSLATOR.value,
            "reason": rule.unknown,
        }
        return record
    keys, why = asset_keys(choice.manifest, record["nodes"], rule)
    record["keys"] = keys
    record["groups"] = asset_groups(choice.manifest, record["nodes"], rule)
    if why is not None:
        category = UnknownCategory.NOT_LITERAL
        if rule.template is not None:
            category = UnknownCategory.CUSTOM_TRANSLATOR
        record["keys_unknown"] = {"category": category.value, "reason": why}
    return record
