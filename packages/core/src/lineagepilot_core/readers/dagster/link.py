"""What a read of the Dagster definitions adds to a graph, and its summary (tasks 8.2 to 8.4).

A read is JSON from the reader's child process (``scan_dagster``), so its shape is checked here
before anything from it is used. What the reader adds is composed on top of the indexed graph and
taken out again whole: nodes with the tool ``dagster``, and unknowns from ``dagster_defs``.

What becomes what is the mapping note's (design_next_phase section 7.1): a code location is a
pipeline, ``dagster:<location>``; a dbt asset definition is a task in it,
``dagster:<location>.<definition>``, which ``orchestrates`` each dbt node it materialises. The
definition's ``assetKeys`` property has each node's asset key (task 8.5). When they cannot be
read, the definition has an unknown that says so, beside its links. An asset job and a schedule
are pipelines, ``dagster:<location>/job/<name>`` and ``dagster:<location>/schedule/<name>``, each
one link as a whole that ``orchestrates`` the dbt nodes it runs (task 8.6). A non-dbt asset that
reads dbt nodes is a task in its code location, ``dagster:<location>.<key>``, with the key's parts
joined by ``/``, that ``consumes`` each of them (task 8.7, D-b).
"""

from __future__ import annotations

import posixpath
from typing import Any

from lineagepilot_core.graph.ids import pipeline_task_id
from lineagepilot_core.graph.model import (
    DAGSTER,
    ArtifactVersion,
    Confidence,
    EdgeKind,
    Evidence,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    ReaderSummary,
    Unresolved,
)
from lineagepilot_core.readers.common.categories import UnknownCategory, explanation, label
from lineagepilot_core.readers.common.folders import WORKSPACE
from lineagepilot_core.readers.common.references import References

DAGSTER_ORIGINS = frozenset({Origin.DAGSTER_DEFS})
FILE_KINDS = ("components", "projects", "workspaces", "python")
# Free text from the workspace is cut to this many characters (design section 9)
_TEXT_LIMIT = 4000
# A read cannot make the summary list more problems than this
_MAX_PROBLEMS = 100
NOT_READ = "the Dagster definitions were not read"
# A name longer than this cannot be shown, as in the DAG scan's linker
MAX_NAME = 1000
# Notes kept on one node
_NOTES_KEPT = 10
# What the review says a dbt asset definition does to a node
HOW = "dbt, through Dagster"
# What the review says a non-dbt asset does with a dbt node
READS = "reads, as a Dagster asset"
ASSET = "asset"
_CONFIDENCES = {c.value for c in Confidence}
_CATEGORIES = {c.value for c in UnknownCategory}


def _strings(value: Any) -> list[str]:
    return [v for v in value if isinstance(v, str)] if isinstance(value, list) else []


def _clip(text: str | None) -> str | None:
    if text is None or len(text) <= _TEXT_LIMIT:
        return text
    return text[: _TEXT_LIMIT - 1] + "…"


def unlink_dagster(graph: MetadataGraph) -> None:
    """Remove everything an earlier read added, leaving the dbt graph as it was indexed."""
    for node_id in [n.id for n in graph.nodes.values() if n.tool == DAGSTER]:
        graph.remove_node(node_id)
    graph.unresolved = [u for u in graph.unresolved if not _ours(u)]
    graph.readers.pop(DAGSTER, None)


def _ours(unknown: Unresolved) -> bool:
    """One of this reader's unknowns: from its read, or from the annotation file about one of its
    tasks (task 8.8)."""
    if unknown.evidence.origin is Origin.ANNOTATION:
        return unknown.subject.startswith(f"{DAGSTER}:")
    return unknown.evidence.origin in DAGSTER_ORIGINS


def mark_not_analysed(graph: MetadataGraph, reason: str) -> None:
    """Say why this graph has nothing from Dagster, so nobody reads that as "nothing runs it"."""
    unlink_dagster(graph)
    graph.readers[DAGSTER] = ReaderSummary(tool=DAGSTER, analysed=False, reason=reason)


def link_dagster(graph: MetadataGraph, scan: dict[str, Any]) -> ReaderSummary:
    """Replace the graph's Dagster part with this read's. See ``scan_dagster`` for the input."""
    unlink_dagster(graph)
    linker = _Linker(graph, scan)
    linker.run()
    files = scan.get("files") if isinstance(scan.get("files"), dict) else {}
    count = sum(len(_strings(files.get(kind))) for kind in FILE_KINDS)
    paths = tuple(_strings(scan.get("paths")))
    analysed, reason = True, None
    if linker.declared:
        pass  # the annotation file declares Dagster tasks: they are linked, whatever was found
    elif not count and paths == (WORKSPACE,):
        # By default the whole workspace is read: finding nothing there means there is no Dagster
        reason = "no Dagster files found in the workspace"
    elif not count:
        # Folders someone chose that hold nothing: as the DAG scan says, they may be elsewhere
        analysed = False
        reason = (
            f"no Dagster files were found in {', '.join(paths) or 'the folders set'}; "
            "set lineagePilot.dagster.paths if they are elsewhere"
        )
    ours = [n for n in graph.nodes.values() if n.tool == DAGSTER]
    summary = ReaderSummary(
        tool=DAGSTER,
        analysed=analysed,
        reason=reason,
        paths=paths,
        files=count,
        pipelines=sum(n.kind is NodeKind.PIPELINE for n in ours),
        tasks=sum(n.kind is NodeKind.TASK for n in ours),
        links=sum(len(graph.out_edges(n.id, (EdgeKind.ORCHESTRATES,))) for n in ours),
        unknowns=sum(1 for u in graph.unresolved if _ours(u)),
        problems=tuple(_strings(scan.get("problems"))[:_MAX_PROBLEMS]),
    )
    graph.readers[DAGSTER] = summary
    return summary


class _Linker:
    def __init__(self, graph: MetadataGraph, scan: dict[str, Any]) -> None:
        self.graph = graph
        project = scan.get("project")
        self.project = project if isinstance(project, str) and project else "."
        hashes = scan.get("hashes")
        self.hashes = hashes if isinstance(hashes, dict) else {}
        self.definitions = _records(scan.get("definitions"))
        self.unknowns = _records(scan.get("unknowns"))
        self.assets = _records(scan.get("assets"))
        self.located = _records(scan.get("located"))
        annotations = scan.get("annotations") if isinstance(scan.get("annotations"), dict) else {}
        file = annotations.get("file")
        self.annotation_file = file if isinstance(file, str) and file else None
        self.annotations = [a for a in _records(annotations.get("tasks")) if _declared(a)]
        self.acknowledged = _records(annotations.get("acknowledged"))
        # An annotated task's own unknowns are answered by its annotation, as for Airflow
        self.annotated = {
            pipeline_task_id(DAGSTER, a["dag_id"], a["task"]) for a in self.annotations
        }
        self.declared = 0
        self.references = References(graph)
        self.runs = [
            (record, subkind)
            for subkind, records in (("job", scan.get("jobs")), ("schedule", scan.get("schedules")))
            for record in _records(records)
        ]

    def run(self) -> None:
        for record in self.definitions:
            if _well_formed(record) and record.get("status") == "resolved":
                self._definition(record)
        for record in self.unknowns:
            if _well_formed(record) and record.get("category") in _CATEGORIES:
                self._unknown(record)
        for record, subkind in self.runs:
            if not _well_formed(record):
                continue
            if record.get("status") == "resolved":
                self._runs(record, subkind)
            elif record.get("category") in _CATEGORIES:
                self._unknown(record, self._run_node(record, subkind))
        for record in self.assets:
            if not _well_formed(record):
                continue
            if record.get("status") == "resolved":
                self._reads(record)
            elif record.get("category") in _CATEGORIES:
                self._unknown(record, self._definition_node(record, ASSET))
        for record in self.located:
            if _well_formed(record) and record.get("category") in _CATEGORIES:
                node = (
                    self._location_node(record["location"])
                    if record.get("of") == "location"
                    else self._definition_node(record, ASSET)
                )
                self._unknown(record, node)
        for annotation in self.annotations:
            self._annotation(annotation)

    # Paths and evidence

    def _path(self, file: str) -> str:
        """A path in the workspace, relative to the project, as every other path in the graph."""
        return file if self.project == "." else posixpath.relpath(file, self.project)

    def _evidence(self, record: dict[str, Any], confidence: str) -> Evidence:
        path = self._path(record["file"])
        line = max(record["line"] - 1, 0)
        sha256 = self.hashes.get(record["file"])
        return Evidence(
            Origin.DAGSTER_DEFS,
            Confidence(confidence),
            ArtifactVersion(path, sha256 if isinstance(sha256, str) else None),
            Location(path, line, 0, line, 0),
        )

    # Nodes

    def _location_node(self, location: str) -> Node:
        node_id = f"{DAGSTER}:{location}"
        existing = self.graph.nodes.get(node_id)
        if existing is not None:
            return existing
        return self.graph.add_node(
            Node(
                id=node_id,
                kind=NodeKind.PIPELINE,
                name=location,
                tool=DAGSTER,
                subkind="code_location",
                properties={"pipeline": location, "tasks": {}, "notes": []},
            )
        )

    def _definition_node(self, record: dict[str, Any], subkind: str = "dbt_assets") -> Node:
        location, name = record["location"], record["name"]
        self._location_node(location)
        node_id = pipeline_task_id(DAGSTER, location, name)
        node = self.graph.nodes.get(node_id)
        if node is None:
            node = self.graph.add_node(
                Node(
                    id=node_id,
                    kind=NodeKind.TASK,
                    name=name,
                    tool=DAGSTER,
                    subkind=subkind,
                    file_path=self._path(record["file"]),
                    properties={
                        "pipeline": location,
                        "operator": record.get("api")
                        if isinstance(record.get("api"), str)
                        else None,
                        "line": max(record["line"] - 1, 0),
                        "col": 0,
                        "tasks": {},
                        "notes": [],
                    },
                )
            )
        notes = node.properties["notes"]
        for note in _strings(record.get("notes")):
            if note not in notes and len(notes) < _NOTES_KEPT:
                notes.append(note)
        if not record.get("in_dg_project", True):
            note = "no dg project holds this component: its code location is named after its folder"
            if note not in notes and len(notes) < _NOTES_KEPT:
                notes.append(note)
        return node

    # Links

    def _definition(self, record: dict[str, Any]) -> None:
        node = self._definition_node(record)
        for key in ("select", "exclude", "selector"):
            if isinstance(record.get(key), str):
                node.properties[key] = record[key]
        keys = _keys(record.get("keys"))
        if keys:
            node.properties["assetKeys"] = keys
        confidence = record.get("confidence")
        confidence = confidence if confidence in _CONFIDENCES else Confidence.HEURISTIC.value
        evidence = self._evidence(record, confidence)
        selected = _strings(record.get("nodes"))
        for uid in selected:
            if uid not in self.graph.nodes:
                self._missing(node, record, uid)
                continue
            self.graph.add_edge(EdgeKind.ORCHESTRATES, node.id, uid, evidence, transformation=HOW)
        unread = record.get("keys_unknown")
        if isinstance(unread, dict) and unread.get("category") in _CATEGORIES:
            # The links stand: only matching by key, for jobs and downstream assets, is lost
            self._unknown({**record, **unread})
        if not selected:
            selection = record.get("select") or "everything"
            self.graph.add_unresolved(
                Unresolved(
                    subject=node.id,
                    reference="selects nothing",
                    reason=f"{selection} matches no node in this manifest: was a model renamed?",
                    evidence=self._evidence(record, Confidence.HEURISTIC.value),
                )
            )

    # The annotation file (task 8.8)

    def _annotation(self, annotation: dict[str, Any]) -> None:
        """A Dagster task the annotation file declares, with what it reads, runs or writes, from
        origin ``annotation``, ``declared``. It is the reader's own node when the reader found
        it, and is made when it did not."""
        file = self.annotation_file or ""
        record = {
            "location": annotation["dag_id"],
            "name": annotation["task"],
            "file": file,
            "line": 1,
        }
        node = self.graph.nodes.get(
            pipeline_task_id(DAGSTER, annotation["dag_id"], annotation["task"])
        )
        node = node or self._definition_node(record, ASSET)
        owner = annotation.get("owner")
        if isinstance(owner, str) and owner:
            node.owner = owner
        path = self._path(file) if file else None
        evidence = Evidence(
            Origin.ANNOTATION,
            Confidence.DECLARED,
            ArtifactVersion(path) if path else None,
            Location(path) if path else None,
        )
        self.declared += 1
        for how in ("consumes", "produces", "transforms", "validates"):
            for reference in _strings(annotation.get(how)):
                found, _ = self.references.resolve(reference)
                kind = EdgeKind.CONSUMES if how == "consumes" else EdgeKind.ORCHESTRATES
                for target in found:
                    self.graph.add_edge(kind, node.id, target, evidence, transformation=how)
                if not found:
                    self.graph.add_unresolved(
                        Unresolved(
                            subject=node.id,
                            reference=reference,
                            reason=f"{file} says this task {how} it, and no dbt node matches",
                            evidence=evidence,
                        )
                    )

    # Assets downstream (task 8.7)

    def _reads(self, record: dict[str, Any]) -> None:
        """A non-dbt asset, a task in its code location, that ``consumes`` the dbt nodes it
        depends on (D-b)."""
        node = self._definition_node(record, ASSET)
        confidence = record.get("confidence")
        confidence = confidence if confidence in _CONFIDENCES else Confidence.HEURISTIC.value
        evidence = self._evidence(record, confidence)
        for uid in _strings(record.get("nodes")):
            if uid not in self.graph.nodes:
                self._missing(node, record, uid)
                continue
            self.graph.add_edge(EdgeKind.CONSUMES, node.id, uid, evidence, transformation=READS)
        unread = record.get("deps_unknown")
        if isinstance(unread, dict) and unread.get("category") in _CATEGORIES:
            self._unknown({**record, **unread}, node)

    # Jobs and schedules (task 8.6)

    def _run_node(self, record: dict[str, Any], subkind: str) -> Node:
        """A job or a schedule: a pipeline that is one link as a whole, as a Cosmos DbtDag is."""
        location, name = record["location"], record["name"]
        node_id = f"{DAGSTER}:{location}/{subkind}/{name}"
        existing = self.graph.nodes.get(node_id)
        if existing is not None:
            return existing
        properties: dict[str, Any] = {
            # The review groups it with its code location's definitions (task 8.9)
            "pipeline": location,
            "location": location,
            "operator": record.get("api") if isinstance(record.get("api"), str) else None,
            "line": max(record["line"] - 1, 0),
            "col": 0,
            "tasks": {},
            "notes": [n for n in _strings(record.get("notes"))][:_NOTES_KEPT],
        }
        if subkind == "schedule":
            for key, prop in (("job_name", "job"), ("cron", "cron")):
                if isinstance(record.get(key), str):
                    properties[prop] = _clip(record[key])
        return self.graph.add_node(
            Node(
                id=node_id,
                kind=NodeKind.PIPELINE,
                name=name,
                tool=DAGSTER,
                subkind=subkind,
                file_path=self._path(record["file"]),
                properties=properties,
            )
        )

    def _runs(self, record: dict[str, Any], subkind: str) -> None:
        node = self._run_node(record, subkind)
        confidence = record.get("confidence")
        confidence = confidence if confidence in _CONFIDENCES else Confidence.HEURISTIC.value
        evidence = self._evidence(record, confidence)
        for uid in _strings(record.get("nodes")):
            if uid not in self.graph.nodes:
                self._missing(node, record, uid)
                continue
            self.graph.add_edge(EdgeKind.ORCHESTRATES, node.id, uid, evidence, transformation=HOW)
        unread = record.get("keys_unknown")
        if isinstance(unread, dict) and unread.get("category") in _CATEGORIES:
            self._unknown({**record, **unread}, node)

    def _missing(self, node: Node, record: dict[str, Any], uid: str) -> None:
        self.graph.add_unresolved(
            Unresolved(
                subject=node.id,
                reference=uid,
                reason="the definition names this node, but the manifest in the graph does not "
                "have it; it may be stale",
                evidence=self._evidence(record, Confidence.HEURISTIC.value),
            )
        )

    # Unknowns

    def _unknown(self, record: dict[str, Any], node: Node | None = None) -> None:
        node = node or self._definition_node(record)
        if node.id in self.annotated or _answered(record, self.acknowledged):
            return
        category = record["category"]
        reason = record.get("reason") if isinstance(record.get("reason"), str) else "not read"
        where = f"{self._path(record['file'])}:{record['line']}"
        self.graph.add_unresolved(
            Unresolved(
                subject=node.id,
                reference=label(category),
                reason=f"{where}: {_clip(reason)}. {explanation(category)}",
                evidence=self._evidence(record, Confidence.HEURISTIC.value),
            )
        )


def _keys(value: Any) -> dict[str, str]:
    """Each node's asset key as Dagster writes one, its parts joined by ``/``, for nodes whose key
    is a list of strings of a size that can be shown."""
    if not isinstance(value, dict):
        return {}
    return {
        uid: "/".join(parts)
        for uid, parts in value.items()
        if isinstance(uid, str)
        and isinstance(parts, list)
        and parts
        and all(isinstance(p, str) for p in parts)
        and sum(len(p) for p in parts) <= MAX_NAME
    }


def _declared(annotation: dict[str, Any]) -> bool:
    """An annotation from the child process that names its pipeline and task, as strings."""
    return all(
        isinstance(annotation.get(k), str) and annotation[k] and len(annotation[k]) <= MAX_NAME
        for k in ("dag_id", "task")
    )


def _answered(record: dict[str, Any], acknowledged: list[dict[str, Any]]) -> bool:
    """Whether the annotation file acknowledges this unknown: by its file and line, its code
    location (``dag_id``) or its task (``task_id``), as the DAG scan's are."""
    for entry in acknowledged:
        if entry.get("file") and entry["file"] != record.get("file"):
            continue
        if entry.get("line") and entry["line"] != record.get("line"):
            continue
        if entry.get("dag_id") and entry["dag_id"] != record.get("location"):
            continue
        if entry.get("task_id") and entry["task_id"] != record.get("name"):
            continue
        return True
    return False


def _records(value: Any) -> list[dict[str, Any]]:
    return [r for r in value if isinstance(r, dict)] if isinstance(value, list) else []


def _well_formed(record: dict[str, Any]) -> bool:
    """A record from the child process that names where it is, in names that can be shown."""
    return (
        all(isinstance(record.get(k), str) and record[k] for k in ("file", "location", "name"))
        and len(record["location"]) <= MAX_NAME
        and len(record["name"]) <= MAX_NAME
        and isinstance(record.get("line"), int)
        and not isinstance(record.get("line"), bool)
    )


def reader_summary_json(summary: Any) -> dict[str, Any]:
    """The reader's summary as the protocol's ``ReaderSummary`` (protocol 2)."""
    if not isinstance(summary, ReaderSummary):
        summary = ReaderSummary(tool=DAGSTER, analysed=False, reason=NOT_READ)
    return {
        "tool": DAGSTER,
        "analysed": summary.analysed,
        "reason": _clip(summary.reason),
        "paths": [_clip(p) for p in summary.paths],
        "files": summary.files,
        "pipelines": summary.pipelines,
        "tasks": summary.tasks,
        "consumers": summary.consumers,
        "links": summary.links,
        "unknowns": summary.unknowns,
        "opaqueTasks": summary.opaque_tasks,
        "problems": [_clip(p) for p in summary.problems],
    }
