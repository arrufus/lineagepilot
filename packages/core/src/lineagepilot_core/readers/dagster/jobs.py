"""Asset jobs and their schedules: which dbt nodes each runs (task 8.6).

A job selects assets from its own code location: the dbt asset definitions there, each with the
nodes it materialises, their asset keys and their groups (``keys``). Its selection, a tree from
``select_syntax`` or ``asset_selection``, is resolved over them, as Dagster resolves it over the
location's asset graph, whose edges between dbt assets are the manifest's. A schedule runs what
its job runs: a job value, or a job named in the same location.

Only what matters for dbt is kept:

- a job that selects no dbt node is no dbt job, and is left out, with any schedule of it; unless
  it selects by key or group where a custom translator hides keys, when it is an unknown;
- a job or schedule that cannot be read is an unknown, when its location has dbt assets it might
  select;
- a selection by key or group, in a location where a custom translator hides some definitions'
  keys, may miss their nodes: the job says so (``keys_unknown``), beside what it does select;
- so may a selection of every asset, by key or group, or of what another selection leaves out, in
  a location with a dbt asset definition that cannot be read: it is an unknown if it selects no
  node the reader knows, and says what it may also run if it does (LP-ios).

A job that only a schedule lists in ``Definitions`` is in the schedule's code location, as
Dagster includes it.

The asset graph is dagster-dbt's and Dagster's: an asset's parents are the dbt assets its node
depends on, through nodes that are not assets (ephemeral models, metrics, semantic models, saved
queries and functions). A source is no asset, but Dagster 1.13.29 joins a dependency on it to the
one asset whose table is the same (``resolved_asset_deps``, a beta feature): dagster-dbt writes a
source's table as ``database.schema.name``, and a seed's or model's as ``database.schema.alias``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from lineagepilot_core.readers.common.categories import UnknownCategory
from lineagepilot_core.readers.common.manifest import Manifest, Node
from lineagepilot_core.readers.common.selector import select as run_select
from lineagepilot_core.readers.common.values import weakest
from lineagepilot_core.readers.dagster.asset_selection import uses_keys
from lineagepilot_core.readers.dagster.keys import LEGACY_GROUPS
from lineagepilot_core.readers.dagster.locations import NO_LOADER
from lineagepilot_core.readers.dagster.selection import ASSET_TYPES, unknown


class Unreadable(Exception):
    """A selection that cannot be resolved here, with why."""


# What dagster-dbt makes no asset of, and looks through for the parents behind it
_NOT_ASSETS = ("metric", "semantic_model", "saved_query", "function")


def _is_asset_less(node: Node) -> bool:
    return node.resource_type in _NOT_ASSETS or (
        node.resource_type == "model" and node.materialized == "ephemeral"
    )


def _table(*parts: str | None) -> str:
    return ".".join(part for part in parts if part)


@dataclass
class Assets:
    """A code location's dbt assets."""

    nodes: set[str] = field(default_factory=set)
    by_key: dict[tuple[str, ...], str] = field(default_factory=dict)
    groups: dict[str, str] = field(default_factory=dict)
    confidence: dict[str, str] = field(default_factory=dict)
    definitions: dict[str, dict[str, Any]] = field(default_factory=dict)
    unread: dict[str, dict[str, Any]] = field(default_factory=dict)
    hidden: list[str] = field(default_factory=list)
    # Definitions whose groups are not known, though their keys are: the legacy loaders'
    groupless: list[str] = field(default_factory=list)
    parents: dict[str, set[str]] = field(default_factory=dict)
    children: dict[str, set[str]] = field(default_factory=dict)

    def build_graph(self, manifest: Manifest) -> None:
        """The assets' edges, as Dagster's asset graph has them. Ephemeral models and the like
        are no assets, so they are left out of the nodes a job can select."""
        self.nodes = {
            uid
            for uid in self.nodes
            if uid in manifest.nodes and not _is_asset_less(manifest.nodes[uid])
        }
        by_table: dict[str, list[str]] = {}
        for uid in sorted(self.nodes):
            node = manifest.nodes[uid]
            by_table.setdefault(_table(node.database, node.schema, node.alias), []).append(uid)

        def behind(uid: str, seen: set[str]) -> set[str]:
            node = manifest.nodes.get(uid)
            if node is None or uid in seen:
                return set()
            seen.add(uid)
            if uid in self.nodes:
                return {uid}
            if _is_asset_less(node):
                return set().union(*(behind(p, seen) for p in manifest.parents.get(uid, ())))
            if node.resource_type == "source":
                matches = by_table.get(_table(node.database, node.schema, node.name), [])
                return set(matches) if len(matches) == 1 else set()
            return set()  # a dbt node no definition here makes: not an asset to walk through

        for uid in self.nodes:
            found = set().union(*(behind(p, set()) for p in manifest.parents.get(uid, ()))) - {uid}
            self.parents[uid] = found
            for parent in found:
                self.children.setdefault(parent, set()).add(uid)


def _locations(definitions: list[dict[str, Any]]) -> dict[str, Assets]:
    found: dict[str, Assets] = {}
    for record in definitions:
        assets = found.setdefault(record["location"], Assets())
        if record.get("status") != "resolved":
            assets.unread[record["key"]] = record
            continue
        assets.definitions[record["key"]] = record
        for uid in record.get("nodes") or []:
            assets.nodes.add(uid)
            assets.confidence[uid] = record.get("confidence", "heuristic")
        for uid, key in (record.get("keys") or {}).items():
            assets.by_key[tuple(key)] = uid
        assets.groups.update(record.get("groups") or {})
        if record.get("keys_unknown"):
            assets.hidden.append(record["name"])
        elif record.get("groups_unknown"):
            assets.groupless.append(record["name"])
    return found


def _pattern(text: str) -> re.Pattern[str]:
    """A ``key:`` value with ``*``, which matches anything, ``/`` included."""
    return re.compile(".*".join(re.escape(part) for part in text.split("*")))


def _walk(
    start: set[str], within: set[str], edges: dict[str, set[str]], depth: int | None, keep: bool
) -> set[str]:
    """What is reached from ``start`` along ``edges``, through the location's assets only, to
    ``depth`` levels (None for all); with ``start`` itself when ``keep``."""
    seen, frontier, reached, level = set(start), set(start), set(), 0
    while frontier and (depth is None or level < depth):
        frontier = {n for u in frontier for n in edges.get(u, ()) if n in within} - seen
        reached |= frontier
        seen |= frontier
        level += 1
    return reached | start if keep else reached - start


def _resolve(tree: dict[str, Any], assets: Assets, manifest: Manifest) -> set[str]:
    op = tree.get("op")
    if op == "all":
        return set(assets.nodes)
    if op == "keys":
        found = {assets.by_key.get(tuple(k)) for k in tree["keys"]}
        return {uid for uid in found if uid in assets.nodes}
    if op == "key_pattern":
        pattern = _pattern(tree["pattern"])
        return {
            uid
            for key, uid in assets.by_key.items()
            if uid in assets.nodes and pattern.fullmatch("/".join(key))
        }
    if op == "groups":
        names = set(tree["names"])
        return {
            uid for uid, group in assets.groups.items() if uid in assets.nodes and group in names
        }
    if op == "assets":
        chosen: set[str] = set()
        for definition in tree["definitions"]:
            chosen |= _nodes_of(definition, assets)
        return chosen & assets.nodes
    if op == "dbt":
        nodes = _nodes_of(tree["definition"], assets)
        if not nodes:
            return set()
        nodes &= assets.nodes
        chosen = run_select(
            manifest,
            tree["select"].split() or None,
            tree["exclude"].split() or None,
            tree["selector"] or None,
            resource_types=ASSET_TYPES,
        )
        if chosen.unknown:
            raise Unreadable(f"its dbt selection: {chosen.reason or 'not read'}")
        return nodes & set(chosen.ids or ())
    if op in ("up", "down"):
        start = _resolve(tree["of"], assets, manifest)
        edges = assets.parents if op == "up" else assets.children
        return _walk(start, assets.nodes, edges, tree.get("depth"), tree.get("self", True))
    if op == "not":
        return assets.nodes - _resolve(tree["of"], assets, manifest)
    if op in ("or", "and", "sub"):
        parts = [_resolve(part, assets, manifest) for part in tree["of"]]
        if op == "or":
            return set().union(*parts)
        if op == "and":
            return set.intersection(*parts)
        return parts[0] - set().union(*parts[1:])
    raise Unreadable(f"a selection of the kind {op!r} is not read")


def _nodes_of(definition: str, assets: Assets) -> set[str]:
    """The nodes a definition named in a selection materialises. One that cannot be read makes
    the selection unreadable; one in no record is not a dbt asset definition."""
    if definition in assets.definitions:
        return set(assets.definitions[definition].get("nodes") or [])
    if definition in assets.unread:
        name = assets.unread[definition]["name"]
        raise Unreadable(f"it selects {name}, a dbt asset definition that cannot be read")
    return set()


def _inherit(jobs: list[dict[str, Any]], schedules: list[dict[str, Any]]) -> None:
    """A job in no ``Definitions`` call but its schedule's is in the schedule's code location."""
    by_key = {job["key"]: job for job in jobs}
    for schedule in schedules:
        job = by_key.get(schedule.get("job_key") or "")
        if job is None or not _unloaded(job) or _unloaded(schedule):
            continue
        job["location"] = schedule["location"]
        job["notes"] = [note for note in job["notes"] if not note.startswith(NO_LOADER)]


def _unloaded(record: dict[str, Any]) -> bool:
    return any(note.startswith(NO_LOADER) for note in record.get("notes", ()))


def resolve(
    jobs: list[dict[str, Any]],
    schedules: list[dict[str, Any]],
    definitions: list[dict[str, Any]],
    manifest: Manifest | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """The jobs and schedules that run dbt nodes, each resolved or an unknown; the others left
    out. Every record has its code location already (``locations.assign``)."""
    _inherit(jobs, schedules)
    locations = _locations(definitions)
    if manifest is not None:
        for assets in locations.values():
            assets.build_graph(manifest)
    kept_jobs: list[dict[str, Any]] = []
    for job in jobs:
        assets = locations.get(job["location"])
        if assets is None or (not assets.nodes and not assets.unread):
            continue  # no dbt asset for it to select
        if job.get("status") != "unknown":
            _resolve_job(job, assets, manifest)
        if job.get("status") in ("resolved", "unknown"):
            kept_jobs.append(job)
    by_key = {job["key"]: job for job in kept_jobs}
    by_name = {(job["location"], job["name"]): job for job in kept_jobs}
    kept_schedules: list[dict[str, Any]] = []
    for schedule in schedules:
        assets = locations.get(schedule["location"])
        if assets is None or (not assets.nodes and not assets.unread):
            continue
        if schedule.get("status") != "unknown":
            job = by_key.get(schedule.get("job_key") or "") or by_name.get(
                (schedule["location"], schedule.get("job_name"))
            )
            if job is None:
                continue  # not an asset job of dbt assets the reader found
            _from_job(schedule, job)
        kept_schedules.append(schedule)
    return kept_jobs, kept_schedules


def _resolve_job(job: dict[str, Any], assets: Assets, manifest: Manifest | None) -> None:
    tree = job.get("selection") or {"op": "all"}
    if manifest is None:
        unknown(job, UnknownCategory.NO_PROJECT, "there is no manifest to resolve its selection")
        return
    try:
        nodes = _resolve(tree, assets, manifest)
    except Unreadable as exc:
        unknown(job, UnknownCategory.NOT_LITERAL, f"the job's selection: {exc}")
        return
    except (KeyError, TypeError, RecursionError):
        unknown(job, UnknownCategory.NOT_LITERAL, "the job's selection is not one the reader reads")
        return
    hidden = ", ".join(sorted(assets.hidden)[:5])
    unread = _names(r["name"] for r in assets.unread.values())
    if not nodes and _open(tree) and unread:
        unknown(job, UnknownCategory.NOT_LITERAL, f"it may select the nodes of {unread}")
        return
    groupless = ", ".join(sorted(assets.groupless)[:5])
    if not nodes and _by_group(tree) and groupless:
        unknown(
            job,
            UnknownCategory.NOT_LITERAL,
            f"it selects by group, and the groups of {groupless} are not known here: "
            + LEGACY_GROUPS,
        )
        return
    if not nodes and uses_keys(tree) and hidden:
        # It may select the nodes whose keys are hidden: say so, never drop it
        unknown(
            job,
            UnknownCategory.CUSTOM_TRANSLATOR,
            f"it selects by key or group, and the keys of {hidden} are not known here",
        )
        return
    if not nodes:
        job["status"] = "empty"
        return
    job.update(
        status="resolved",
        nodes=sorted(nodes),
        confidence=weakest("inferred", *(assets.confidence[uid] for uid in nodes)),
    )
    if _open(tree) and unread:
        job["keys_unknown"] = {
            "category": UnknownCategory.NOT_LITERAL.value,
            "reason": f"it may also run the nodes of {unread}",
        }
    elif uses_keys(tree) and hidden:
        job["keys_unknown"] = {
            "category": UnknownCategory.CUSTOM_TRANSLATOR.value,
            "reason": f"it selects by key or group, and the keys of {hidden} are not known here, "
            "so it may run more of their nodes than are listed",
        }


def _open(tree: dict[str, Any]) -> bool:
    """Whether a selection may pick assets the reader cannot see: every asset, by key or group,
    or what another selection leaves out. One of named definitions picks only theirs."""
    if tree.get("op") in ("all", "keys", "key_pattern", "groups", "not"):
        return True
    of = tree.get("of")
    if isinstance(of, dict):
        return _open(of)
    return isinstance(of, list) and any(_open(t) for t in of)


def _by_group(tree: dict[str, Any]) -> bool:
    if tree.get("op") == "groups":
        return True
    of = tree.get("of")
    if isinstance(of, dict):
        return _by_group(of)
    return isinstance(of, list) and any(_by_group(t) for t in of)


def _names(names: Any) -> str:
    """Up to five definitions that cannot be read, as a job's or an asset's reason names them."""
    found = sorted(set(names))
    if not found:
        return ""
    shown = ", ".join(found[:5]) + (f" and {len(found) - 5} more" if len(found) > 5 else "")
    what = "a dbt asset definition" if len(found) == 1 else "dbt asset definitions"
    return f"{shown}, {what} that cannot be read"


def _from_job(schedule: dict[str, Any], job: dict[str, Any]) -> None:
    schedule["job_name"] = job["name"]
    if job.get("status") == "unknown":
        unknown(
            schedule,
            UnknownCategory.NOT_LITERAL,
            f"its job, {job['name']}, cannot be read: {job.get('reason', '')}",
        )
        return
    schedule.update(
        status="resolved",
        nodes=list(job["nodes"]),
        confidence=job["confidence"],
    )
    if job.get("keys_unknown"):
        schedule["keys_unknown"] = dict(job["keys_unknown"])
