"""Non-dbt assets downstream of dbt assets: which dbt nodes each reads (task 8.7; D-b).

An asset that is not dbt's depends on asset keys (``deps``). Each is matched to a dbt node, as
dagster 1.13.29 resolves a definition's dependencies:

- by its key, against the keys of every dbt asset definition the reader found. Keys are global in a
  Dagster deployment, so an asset in one code location can read a dbt asset made in another;
- through ``get_asset_key_for_model``, by the name of a model, seed or snapshot in the manifest, or
  through a whole dbt asset definition, every node it makes;
- by name and group, for a key of one part that no asset in its code location has: the one dbt
  asset there whose key ends in that name and whose group, set and not ``default``, is the asset's
  (``resolved_asset_deps``, a beta feature).

An asset that reads no dbt node is no concern of the review, and is left out. One that may read
one, but whose dependencies cannot be read, is an unknown at its line, made in a loop or a factory
or not. So is one that names a key no asset is known to have, where a custom translator hides some
dbt keys, or a dbt asset definition cannot be read: it may be one of theirs (LP-ios).
"""

from __future__ import annotations

from typing import Any

from lineagepilot_core.readers.common.categories import UnknownCategory
from lineagepilot_core.readers.common.manifest import Manifest
from lineagepilot_core.readers.common.values import weakest
from lineagepilot_core.readers.dagster.keys import DEFAULT_GROUP
from lineagepilot_core.readers.dagster.selection import ASSET_TYPES, unknown


def resolve(
    assets: list[dict[str, Any]],
    definitions: list[dict[str, Any]],
    manifest: Manifest | None,
) -> list[dict[str, Any]]:
    """The assets that read dbt nodes, each resolved or an unknown; the others left out."""
    by_key: dict[tuple[str, ...], str] = {}
    confidence: dict[str, str] = {}
    by_definition = {record["key"]: record for record in definitions}
    hidden = sorted(r["name"] for r in definitions if r.get("keys_unknown"))
    unread = sorted(r["name"] for r in definitions if r.get("status") != "resolved")
    by_group: dict[str, dict[tuple[str, str], list[str]]] = {}
    with_dbt: set[str] = set()
    for record in definitions:
        with_dbt.add(record["location"])
        if record.get("status") != "resolved":
            continue
        groups = record.get("groups") or {}
        for uid, key in (record.get("keys") or {}).items():
            by_key[tuple(key)] = uid
            confidence[uid] = record.get("confidence", "heuristic")
            group = groups.get(uid)
            if group and group != DEFAULT_GROUP:
                by_group.setdefault(record["location"], {}).setdefault((group, key[-1]), []).append(
                    uid
                )
    local: dict[str, set[tuple[str, ...]]] = {}
    for record in assets:
        local.setdefault(record["location"], set()).add(tuple(record.get("asset_key") or ()))
    kept: list[dict[str, Any]] = []
    for record in assets:
        if not with_dbt:
            break
        found, why_unread, unmatched = _match(record, by_key, by_definition, manifest)
        hides = [k for k in unmatched if k not in set().union(*local.values())]
        if record.get("status") == "unknown":
            # Made in a loop or a factory, or with specs worked out: kept when it may read dbt
            if (
                found
                or why_unread
                or "deps" not in record
                or record.get("deps_unknown")
                or (hides and (hidden or unread))
            ):
                kept.append(record)
            continue
        location = record["location"]
        for key in unmatched:
            if len(key) == 1 and record.get("group") and key not in local.get(location, set()):
                candidates = by_group.get(location, {}).get((record["group"], key[0]), [])
                if len(candidates) == 1:
                    found.add(candidates[0])
        hides = [k for k in unmatched if k not in set().union(*local.values())]
        why = why_unread or record.get("deps_unknown")
        if found:
            record.update(
                status="resolved",
                nodes=sorted(found),
                confidence=weakest("inferred", *(confidence.get(uid, "inferred") for uid in found)),
            )
            if why:
                record["deps_unknown"] = {
                    "category": UnknownCategory.NOT_LITERAL.value,
                    "reason": why,
                }
            else:
                record.pop("deps_unknown", None)
            kept.append(record)
        elif why:
            record.pop("deps_unknown", None)
            kept.append(unknown(record, UnknownCategory.NOT_LITERAL, why))
        elif hides and unread:
            shown = "/".join(hides[0])
            names = ", ".join(unread[:5])
            what = "a dbt asset definition" if len(unread) == 1 else "dbt asset definitions"
            reason = (
                f"it reads {shown}, which no asset is known to make, and {names}, {what} that "
                "cannot be read, may make it"
            )
            kept.append(unknown(record, UnknownCategory.NOT_LITERAL, reason))
        elif hides and hidden:
            names = ", ".join(hidden[:5])
            shown = "/".join(hides[0])
            reason = (
                f"it reads {shown}, which no asset is known to make, and the asset keys of {names} "
                "are not known here: it may be one of theirs"
            )
            kept.append(unknown(record, UnknownCategory.CUSTOM_TRANSLATOR, reason))
    return kept


def _match(
    record: dict[str, Any],
    by_key: dict[tuple[str, ...], str],
    by_definition: dict[str, dict[str, Any]],
    manifest: Manifest | None,
) -> tuple[set[str], str | None, list[tuple[str, ...]]]:
    """The dbt nodes an asset's dependencies name; why one cannot be read; and the keys that
    named none."""
    found: set[str] = set()
    unread: str | None = None
    unmatched: list[tuple[str, ...]] = []
    for dep in record.get("deps") or []:
        if "model" in dep:
            definition = by_definition.get(dep.get("definition") or "")
            uid = _model(manifest, dep["model"]) if definition is not None else None
            if uid is None:
                unread = (
                    unread
                    or f"get_asset_key_for_model() names {dep['model']!r}, not a dbt node here"
                )
            else:
                found.add(uid)
        elif "definition" in dep:
            definition = by_definition.get(dep["definition"])
            if definition is None:
                continue  # an asset definition that is not dbt's
            if definition.get("status") != "resolved":
                unread = (
                    unread
                    or f"it reads {definition['name']}, a dbt asset definition that cannot be read"
                )
                continue
            found.update(definition.get("nodes") or [])
        elif "key" in dep:
            key = tuple(dep["key"])
            if key in by_key:
                found.add(by_key[key])
            else:
                unmatched.append(key)
    return found, unread, unmatched


def _model(manifest: Manifest | None, name: str) -> str | None:
    """The one model, seed or snapshot of this name, as ``get_asset_key_for_model`` finds it."""
    if manifest is None:
        return None
    matches = [n.unique_id for n in manifest.of_type(*ASSET_TYPES) if n.name == name]
    return matches[0] if len(matches) == 1 else None
