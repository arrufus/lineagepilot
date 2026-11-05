"""The Change Impact Review: from a list of changed files to a structured result (design
section 8, tasks 4.2 and 4.3).

1. Map each changed file to the nodes it defines or documents. A file that maps to nothing
   but could matter (a macro, ``dbt_project.yml``, a model the manifest has not seen) is an
   unknown, not a pass.
2. For each node whose definition changed, compare the columns its compiled SQL selects now
   with a baseline: a baseline manifest, else ``catalog.json``, else nothing, and then every
   column is ``unknown``. The compiled SQL only counts if the manifest has seen the file's
   current text: otherwise the comparison would miss the very change under review.
3. Traverse downstream on the current graph. Changed columns follow column lineage. Everything
   else below a changed node is listed too, at model level and marked ``inferred``, because
   comparing names cannot rule out a change in how an unchanged column is computed.
4. Group the result, with its evidence and owners, and list every unknown met on the way.

A change to a properties file alone is listed but not followed downstream: it documents the
data rather than changing it, and one file often documents dozens of models. That is said in
the Unknowns section rather than left out.

The result is plain JSON in the shape of the protocol's ``ReviewResult``, apart from the
``locations`` the language server adds. Nothing in it depends on the machine, the time or the
order of the input: the same inputs give the same review, so the panel and ``lineagepilot
review`` agree and reviews can be compared across pull requests.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from lineagepilot_core.analysis.baseline import ManifestBaseline
from lineagepilot_core.analysis.impact import Impact, ImpactedNode, impact, weakest
from lineagepilot_core.analysis.traversal import Granularity, Step
from lineagepilot_core.export.review_markdown import render_markdown
from lineagepilot_core.graph.column_lineage import SQL_KINDS
from lineagepilot_core.graph.ids import column_id, normalise_identifier, split_column_id
from lineagepilot_core.graph.model import (
    ColumnLineageStatus,
    Confidence,
    MetadataGraph,
    Node,
    NodeKind,
    Unresolved,
)
from lineagepilot_core.project import PROJECT_FILE, DbtProject, relative_to

# Free text from the workspace is cut to this many characters (design section 9)
TEXT_LIMIT = 4000
# A changed source file larger than this is not read to check it against the manifest
MAX_SOURCE_BYTES = 5_000_000
# How many steps into a node are kept as its evidence, the rest only counted
MAX_EVIDENCE = 5

KIND_ORDER = ("model", "snapshot", "seed", "source", "exposure", "airflow_task", "airflow_dag")
_PROJECT_FILES = frozenset(
    {PROJECT_FILE, "packages.yml", "dependencies.yml", "package-lock.yml", "selectors.yml"}
)
_DEFINITION_SUFFIXES = (".sql", ".py", ".csv")
_YAML_SUFFIXES = (".yml", ".yaml")
_MANIFEST_SOURCE_SUFFIXES = (*_DEFINITION_SUFFIXES, *_YAML_SUFFIXES)
_NODE_PATH_KEYS = ("model-paths", "seed-paths", "snapshot-paths")

CATALOG_NOTE = (
    "catalog.json lists what the warehouse held when dbt docs generate last ran. If a changed "
    "model was built before that, a rename no longer shows as a change: set a baseline manifest "
    "to compare with what is deployed."
)
COMPUTING_NOTE = (
    "Column lineage was still being computed, so some models are reached at model level. "
    "This review updates when it finishes."
)
COLUMNS_OFF_NOTE = "Column lineage is turned off, so impact is at model level throughout."
STALE = "its file changed after the manifest was written; run dbt compile to compare its columns"


class FileStatus(str, Enum):
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"
    UNTRACKED = "untracked"


_NEW = (FileStatus.ADDED, FileStatus.UNTRACKED)


@dataclass(frozen=True, slots=True)
class ChangedFile:
    """A file that differs from the base, by its path relative to the project."""

    path: str
    status: FileStatus
    previous_path: str | None = None  # where a renamed file was


@dataclass(frozen=True, slots=True)
class BaseRef:
    """What the changes are compared with: the ref asked for, and the commit it came to."""

    ref: str | None = None
    commit: str | None = None


def clip(text: str | None, limit: int = TEXT_LIMIT) -> str | None:
    if text is None or len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def baseline_inputs(
    graph: MetadataGraph, changed: Iterable[ChangedFile], project: DbtProject | None = None
) -> tuple[list[str], list[str]]:
    """What to read from a baseline manifest: the changed nodes, and the changed files.

    The files find what the current graph no longer has, such as a deleted model.
    """
    nodes: set[str] = set()
    files: set[str] = set()
    for file in changed:
        for path in (file.path, file.previous_path):
            if path:
                files.add(path)
                nodes.update(n.id for n in _nodes_for(graph, path, project))
    return sorted(nodes), sorted(files)


def review_changes(
    graph: MetadataGraph,
    changed: Iterable[ChangedFile],
    project: DbtProject,
    baseline: ManifestBaseline | None = None,
    base: BaseRef | None = None,
    outside_project: int = 0,
    column_lineage: Callable[[str], ColumnLineageStatus | None] | None = None,
    column_stage: str = "done",
) -> dict[str, Any]:
    """The review of ``changed``, as JSON.

    Args:
        project: the project the paths are relative to; its files are read to check them
            against the manifest
        baseline: a baseline manifest, when one is configured
        outside_project: how many changed files are outside the project, for the record
        column_lineage: returns a model's column-lineage status, computing it if needed, as in
            ``impact``. Defaults to what the graph has.
        column_stage: how far the indexer is with column lineage (``computing``, ``done``, ``off``)
    """
    status_of = column_lineage or graph.column_lineage.get
    files = sorted(
        {f for f in changed if not _is_output(f.path, project)}, key=lambda f: (f.path, f.status)
    )
    unknowns: list[dict[str, Any]] = []
    other_files: list[dict[str, Any]] = []
    touched: dict[str, _Touched] = {}
    baseline_only: dict[str, _Touched] = {}

    for file in files:
        nodes = _nodes_for(graph, file.path, project)
        if not nodes and file.previous_path:
            nodes = _nodes_for(graph, file.previous_path, project)
        if nodes:
            for node in nodes:
                touched.setdefault(node.id, _Touched()).add(file, _role(node, file))
            continue
        gone = _baseline_files(baseline, file)
        if gone:
            for node_id in gone:
                baseline_only.setdefault(node_id, _Touched()).add(file, "definition")
            continue
        unknown = _unmapped(file, project)
        if unknown is None:
            other_files.append({"path": clip(file.path), "status": file.status.value})
        else:
            unknowns.append(_unknown(file.path, "changed file", unknown))

    if baseline is not None and baseline.error:
        unknowns.append(_unknown(baseline.name, "baseline manifest", baseline.error))

    changed_nodes = []
    column_targets: list[str] = []
    node_targets: list[str] = []
    baselines_used: set[str] = set()
    for node_id in sorted(touched):
        entry = touched[node_id]
        node = graph.nodes[node_id]
        diff = _compare_columns(graph, node, entry, project, baseline, status_of)
        baselines_used.add(diff["baseline"])
        if diff["state"] == "unknown":
            unknowns.append(_unknown(node_id, "column changes", diff["reason"]))
        if entry.definition is not None:
            node_targets.append(node_id)
            column_targets += _changed_column_ids(graph, node_id, diff)
        else:
            unknowns.append(
                _unknown(
                    node_id,
                    "properties",
                    f"{entry.files[0]['path']} changed, which documents this node. Properties "
                    "are not followed downstream: if this changes the data, such as the table a "
                    "source reads, analyze the node's impact.",
                )
            )
        changed_nodes.append(_changed_node(node, entry, diff))
    for node_id in sorted(baseline_only):
        changed_nodes.append(_removed_node(baseline, node_id, baseline_only[node_id]))
    changed_nodes.sort(key=lambda n: (KIND_ORDER.index(n["kind"]), n["id"]))

    by_column = _impact_of(graph, column_targets, status_of)
    by_node = _impact_of(graph, node_targets, status_of)
    impacted = _merge_impact(graph, by_column, by_node, set(touched), bool(column_targets))
    unknowns += [
        _unknown(u.subject, u.reference, u.reason) for u in _all_unknowns(by_column, by_node)
    ]

    result: dict[str, Any] = {
        "project": clip(graph.project_name or project.name),
        "base": {"ref": clip(base.ref), "commit": clip(base.commit)} if base else None,
        "artifacts": {
            "manifestGeneratedAt": clip(graph.manifest.generated_at if graph.manifest else None),
            "dbtVersion": clip(graph.manifest.dbt_version if graph.manifest else None),
            "baselineManifest": _baseline_json(baseline),
        },
        "columnLineage": column_stage,
        "summary": _summary(files, changed_nodes, impacted, unknowns),
        "changedNodes": changed_nodes,
        "impacted": impacted,
        "owners": _owners(impacted),
        "airflow": {"analysed": _airflow_analysed(graph)},
        "unknowns": _deduplicated(unknowns),
        "otherFiles": other_files,
        "outsideProject": outside_project,
        "notes": _notes(baselines_used, column_stage),
    }
    result["summary"]["unknowns"] = len(result["unknowns"])
    result["markdown"] = render_markdown(result)
    return result


# Files


class _Touched:
    """The changed files that touch a node."""

    def __init__(self) -> None:
        self.files: list[dict[str, str | None]] = []
        self.definition: ChangedFile | None = None

    def add(self, file: ChangedFile, role: str) -> None:
        self.files.append({"path": clip(file.path), "status": file.status.value, "role": role})
        if role == "definition":
            self.definition = file

    @property
    def status(self) -> str:
        return self.definition.status.value if self.definition else "properties"


def _nodes_for(graph: MetadataGraph, path: str, project: DbtProject | None) -> list[Node]:
    nodes = graph.nodes_for_file(path)
    packages = (project.packages_path if project else "dbt_packages").rstrip("/") + "/"
    if not nodes and path.startswith(packages):
        package, _, inner = path[len(packages) :].partition("/")
        nodes = [n for n in graph.nodes_for_file(inner) if n.package == package]
    return nodes


def _is_output(path: str, project: DbtProject) -> bool:
    """dbt writes these: its artifacts and the snapshot cache, when not ignored by git."""
    return path == project.target_path or path.startswith(project.target_path.rstrip("/") + "/")


def _role(node: Node, file: ChangedFile) -> str:
    """How a file relates to a node. A YAML file only describes what it defines."""
    if node.kind is NodeKind.EXPOSURE or file.path.endswith(_YAML_SUFFIXES):
        return "properties"
    return "definition"


def _baseline_files(baseline: ManifestBaseline | None, file: ChangedFile) -> list[str]:
    if baseline is None or file.status is not FileStatus.DELETED:
        return []
    return baseline.files.get(file.path, [])


def _unmapped(file: ChangedFile, project: DbtProject) -> str | None:
    """Why a file no node maps to matters, or None if it does not."""
    path = file.path
    if path in _PROJECT_FILES:
        return "project configuration: it can change any node, and which ones is not known"
    if project.is_under(path, "macro-paths"):
        return "a macro: it can change any model that calls it, and which ones is not known"
    if any(project.is_under(path, key) for key in _NODE_PATH_KEYS):
        if not path.endswith(_MANIFEST_SOURCE_SUFFIXES):
            return None
        if file.status is FileStatus.DELETED:
            return "deleted, and neither the manifest nor a baseline has a node for it"
        return "not in the manifest yet; run dbt parse or dbt compile to include it"
    return None


# Columns


def _compare_columns(
    graph: MetadataGraph,
    node: Node,
    entry: _Touched,
    project: DbtProject,
    baseline: ManifestBaseline | None,
    status_of: Callable[[str], ColumnLineageStatus | None],
) -> dict[str, Any]:
    if entry.definition is None:
        return _not_compared("only its properties file changed")
    if node.kind not in SQL_KINDS:
        return _unknown_columns(f"a {node.kind.value} has no compiled SQL to compare")
    file = entry.definition
    if file.status is FileStatus.DELETED:
        return _not_compared("its file was deleted")
    fresh = _manifest_has_seen(node, project, file)
    if fresh is not None:
        return _unknown_columns(fresh, stale=fresh is STALE)
    status = status_of(node.id)  # may compute it on demand, which records the output columns
    current = graph.output_columns.get(node.id)
    if current is None:
        reason = status.reason if status and status.reason else "column lineage has not reached it"
        return _unknown_columns(f"what it selects is not known: {reason}")

    note = None
    if baseline is not None and not baseline.error:
        before = baseline.nodes.get(node.id)
        if before is None:
            return _diff(graph, current, (), {}, "baseline_manifest", "not in the baseline", node)
        if before.columns is not None:
            return _diff(
                graph, current, before.columns, before.types, "baseline_manifest", None, node
            )
        note = f"The baseline manifest cannot say: {before.reason}."
    if node.in_catalog:
        columns = [c.name for c in _by_position(graph, node.id) if c.in_catalog]
        return _diff(graph, current, columns, {}, "catalog", note, node)
    if file.status in _NEW:
        return _diff(graph, current, (), {}, "none", "a new file", node)
    advice = "No baseline has it: set lineagePilot.baseline.manifestPath, or run dbt docs generate."
    return _unknown_columns(f"{note} {advice}" if note else advice)


def _manifest_has_seen(node: Node, project: DbtProject, file: ChangedFile) -> str | None:
    """None if the manifest has this model's current text, or why that cannot be said."""
    if node.source_sha256 is None:
        return "the manifest has no source text to check it against, so its compiled SQL may be old"
    path = project.root / file.path
    try:
        # A repository can hold a symlink to anything: a device reports size 0 and never ends
        if relative_to(path, project.root) is None or not path.is_file():
            return "the file is not a regular file inside the project"
        if path.stat().st_size > MAX_SOURCE_BYTES:
            return "the file is too large to check against the manifest"
        # dbt reads the bytes as UTF-8 and strips them, with no change to line endings
        text = path.read_bytes().decode("utf-8").strip()
    except (OSError, UnicodeDecodeError) as e:
        return f"the file cannot be read: {type(e).__name__}"
    if hashlib.sha256(text.encode("utf-8")).hexdigest() != node.source_sha256:
        return STALE
    return None


def _by_position(graph: MetadataGraph, node_id: str) -> list[Any]:
    return sorted(
        graph.columns_of(node_id), key=lambda c: (c.ordinal is None, c.ordinal or 0, c.name)
    )


def _diff(
    graph: MetadataGraph,
    current: Iterable[str],
    before: Iterable[str],
    types_before: dict[str, str],
    baseline: str,
    reason: str | None,
    node: Node,
) -> dict[str, Any]:
    adapter = graph.adapter_type
    now = {normalise_identifier(name, adapter): name for name in current}
    then = {normalise_identifier(name, adapter): name for name in before}
    type_changes = []
    for folded in sorted(now.keys() & then.keys()):
        column = graph.columns.get(column_id(node.id, folded, adapter))
        old, new = types_before.get(folded), column.data_type if column else None
        if old and new and old.lower() != new.lower():
            type_changes.append(
                {"name": clip(now[folded]), "before": clip(old), "after": clip(new)}
            )
    return {
        "state": "known",
        "baseline": baseline,
        "added": [clip(now[f]) for f in sorted(now.keys() - then.keys())],
        "removed": [clip(then[f]) for f in sorted(then.keys() - now.keys())],
        "typeChanged": type_changes,
        "stale": False,
        "reason": clip(reason),
    }


def _not_compared(reason: str) -> dict[str, Any]:
    return {**_no_diff("not_compared", reason), "stale": False}


def _unknown_columns(reason: str, stale: bool = False) -> dict[str, Any]:
    return {**_no_diff("unknown", reason), "stale": stale}


def _no_diff(state: str, reason: str) -> dict[str, Any]:
    return {
        "state": state,
        "baseline": "none",
        "added": [],
        "removed": [],
        "typeChanged": [],
        "reason": clip(reason),
    }


def _changed_column_ids(graph: MetadataGraph, node_id: str, diff: dict[str, Any]) -> list[str]:
    """The changed columns the graph has. A removed column is in the graph while the catalog
    still lists it; once it is not, nothing can be traced from it, and what depends on the
    node is reached at model level."""
    if diff["state"] != "known":
        return []
    names = [*diff["removed"], *(t["name"] for t in diff["typeChanged"])]
    ids = (column_id(node_id, name, graph.adapter_type) for name in names)
    return [cid for cid in ids if cid in graph.columns]


def _changed_node(node: Node, entry: _Touched, diff: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": node.id,
        "kind": node.kind.value,
        "name": clip(node.name),
        "owner": clip(node.owner or node.group),
        "status": entry.status,
        "files": sorted(entry.files, key=lambda f: (f["path"], f["role"])),
        "columns": diff,
    }


def _removed_node(
    baseline: ManifestBaseline | None, node_id: str, entry: _Touched
) -> dict[str, Any]:
    """A node the baseline has and the current graph does not."""
    assert baseline is not None  # only a baseline can name it
    before = baseline.nodes[node_id]
    return {
        "id": node_id,
        "kind": before.kind,
        "name": clip(before.name),
        "owner": clip(before.owner),
        "status": "deleted",
        "files": sorted(entry.files, key=lambda f: (f["path"], f["role"])),
        "columns": _not_compared("it is in the baseline manifest, but no longer in the manifest"),
    }


# Impact


def _impact_of(
    graph: MetadataGraph,
    targets: list[str],
    status_of: Callable[[str], ColumnLineageStatus | None],
) -> Impact | None:
    return impact(graph, targets, column_lineage=status_of) if targets else None


def _merge_impact(
    graph: MetadataGraph,
    by_column: Impact | None,
    by_node: Impact | None,
    changed: set[str],
    columns_known: bool,
) -> list[dict[str, Any]]:
    """One entry per node: what column lineage says if it reached it, else the model-level
    fallback. A node that changed itself is listed as changed, not again as impacted."""
    merged: dict[str, dict[str, Any]] = {}
    for hit in by_node.nodes.values() if by_node else ():
        if hit.id not in changed:
            merged[hit.id] = _impacted_json(graph, hit, columns_known, by_column=False)
    for hit in by_column.nodes.values() if by_column else ():
        if hit.id not in changed:
            entry = _impacted_json(graph, hit, columns_known, by_column=True)
            if hit.id in merged:
                entry["depth"] = min(entry["depth"], merged[hit.id]["depth"])
            merged[hit.id] = entry
    return sorted(
        merged.values(),
        key=lambda n: (KIND_ORDER.index(n["kind"]), n["reach"] != "column", n["depth"], n["id"]),
    )


def _impacted_json(
    graph: MetadataGraph, hit: ImpactedNode, columns_known: bool, by_column: bool
) -> dict[str, Any]:
    node = graph.nodes[hit.id]
    traced = by_column and hit.granularity is Granularity.COLUMN
    confidence = hit.confidence if traced else weakest(hit.confidence, Confidence.INFERRED)
    reason = hit.reason
    if not traced and reason is None:
        reason = (
            "depends on a changed model, but on none of the columns known to have changed"
            if columns_known
            else "the changed columns are not known, so impact is assumed"
        )
    via = sorted(
        (
            {"source": origin, "target": step.ref, **_step_evidence(step)}
            for origin, step in hit.via
        ),
        key=lambda v: (v["source"], v["target"]),
    )
    return {
        "id": hit.id,
        "kind": hit.kind.value,
        "name": clip(node.name),
        "owner": clip(node.owner or node.group),
        "file": clip(node.file_path),
        "depth": hit.depth,
        "reach": "column" if traced else "model",
        "confidence": confidence.value,
        "origins": sorted({origin for v in via for origin in v["origins"]}),
        "columns": sorted(clip(split_column_id(c)[1]) for c in hit.columns),
        "reason": clip(reason),
        "via": via[:MAX_EVIDENCE],
        "viaMore": max(0, len(via) - MAX_EVIDENCE),
    }


def _step_evidence(step: Step) -> dict[str, Any]:
    return {
        "confidence": weakest(*(e.confidence for e in step.edges)).value,
        "origins": sorted({ev.origin.value for e in step.edges for ev in e.evidence}),
    }


def _all_unknowns(*impacts: Impact | None) -> list[Unresolved]:
    return [u for found in impacts if found is not None for u in found.unknowns]


# Summary


def _unknown(subject: str, reference: str, reason: str) -> dict[str, Any]:
    return {"subject": clip(subject), "reference": clip(reference), "reason": clip(reason)}


def _deduplicated(unknowns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique = {(u["subject"], u["reference"], u["reason"]): u for u in unknowns}
    return [unique[key] for key in sorted(unique)]


def _summary(
    files: list[ChangedFile],
    changed_nodes: list[dict[str, Any]],
    impacted: list[dict[str, Any]],
    unknowns: list[dict[str, Any]],
) -> dict[str, Any]:
    kinds: dict[str, int] = {}
    for node in impacted:
        kinds[node["kind"]] = kinds.get(node["kind"], 0) + 1
    columns = 0
    stale = 0
    for node in changed_nodes:
        diff = node["columns"]
        columns += len(diff["added"]) + len(diff["removed"]) + len(diff["typeChanged"])
        stale += diff["stale"]
    return {
        "changedFiles": len(files),
        "changedNodes": len(changed_nodes),
        "changedColumns": columns,
        "staleNodes": stale,
        "impacted": dict(sorted(kinds.items())),
        "impactedByColumn": sum(1 for n in impacted if n["reach"] == "column"),
        "unknowns": len(unknowns),
    }


def _owners(impacted: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Impacted nodes by owner, named owners first; nobody's nodes are listed, not lost."""
    by_owner: dict[str | None, list[str]] = {}
    for node in impacted:
        by_owner.setdefault(node["owner"], []).append(node["id"])
    named = sorted(owner for owner in by_owner if owner is not None)
    order: list[str | None] = [*named, *([None] if None in by_owner else [])]
    return [{"owner": owner, "nodes": sorted(by_owner[owner])} for owner in order]


def _airflow_analysed(graph: MetadataGraph) -> bool:
    return any(
        n.kind in (NodeKind.AIRFLOW_DAG, NodeKind.AIRFLOW_TASK) for n in graph.nodes.values()
    )


def _baseline_json(baseline: ManifestBaseline | None) -> dict[str, Any] | None:
    if baseline is None:
        return None
    return {
        "name": clip(baseline.name),
        "sha256": baseline.sha256,
        "generatedAt": clip(baseline.generated_at),
        "dbtVersion": clip(baseline.dbt_version),
    }


def _notes(baselines_used: set[str], column_stage: str) -> list[str]:
    notes = []
    if "catalog" in baselines_used:
        notes.append(CATALOG_NOTE)
    if column_stage == "computing":
        notes.append(COMPUTING_NOTE)
    elif column_stage == "off":
        notes.append(COLUMNS_OFF_NOTE)
    return notes
