"""Put a DAG scan into the graph: Airflow nodes, their edges and their unknowns (task 5.3).

A scan is held apart from the snapshot of the dbt graph and applied on top of it, because a DAG
file changes far more often than the manifest, and re-indexing costs seconds where a scan costs
milliseconds.

- A task that runs dbt gets an ``orchestrates`` edge to each node it selects. A Cosmos ``DbtDag`` or
  ``DbtTaskGroup`` gets one edge per node at the level of the DAG or group, which is what the file
  states; the per-model task ids Cosmos would render are derived and kept beside it.
- A SQL operator gets one to each dbt node whose table its SQL reads or writes.
- An annotation adds a declared edge, and answers the scanner's unknown about that task.
- Whatever the scan could not resolve becomes an ``Unresolved`` record, at the call site. A task
  whose command cannot be read and does not mention dbt is counted, not listed: it is usually
  nothing to do with dbt, and one line per such task would bury the rest.

Only DAGs that touch this project are added. Every other DAG file is only counted.
"""

from __future__ import annotations

import posixpath
from typing import Any

from lineagepilot_core.airflow.categories import explanation, label
from lineagepilot_core.graph.ids import airflow_dag_id, airflow_task_id, normalise_identifier
from lineagepilot_core.graph.model import (
    DBT_KINDS,
    AirflowSummary,
    ArtifactVersion,
    Confidence,
    EdgeKind,
    Evidence,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
)

AIRFLOW_KINDS = frozenset({NodeKind.AIRFLOW_DAG, NodeKind.AIRFLOW_TASK})
AIRFLOW_ORIGINS = frozenset({Origin.DAG_STATIC, Origin.ANNOTATION})
_NOTES_KEPT = 8
# A longer ID would not fit the protocol, and nobody names a DAG or task like this but on purpose
MAX_NAME = 1000
_WITH_SELECTION = frozenset({"run", "build", "seed", "snapshot"})


def unlink_airflow(graph: MetadataGraph) -> None:
    """Remove everything an earlier scan added, leaving the dbt graph as it was indexed."""
    for node_id in [n.id for n in graph.nodes.values() if n.kind in AIRFLOW_KINDS]:
        graph.remove_node(node_id)
    graph.unresolved = [u for u in graph.unresolved if u.evidence.origin not in AIRFLOW_ORIGINS]
    graph.airflow = None


def mark_not_analysed(graph: MetadataGraph, reason: str) -> None:
    """Say why this graph has no Airflow nodes, so nobody reads that as "no task is affected"."""
    unlink_airflow(graph)
    graph.airflow = AirflowSummary(analysed=False, reason=reason)


def link_airflow(graph: MetadataGraph, scan: dict[str, Any]) -> AirflowSummary:
    """Replace the graph's Airflow part with this scan's. See ``scan_airflow`` for the input."""
    unlink_airflow(graph)
    linker = _Linker(graph, scan)
    linker.run()
    graph.airflow = linker.summary()
    return graph.airflow


def _named_sensibly(record: dict[str, Any]) -> bool:
    return all(len(record.get(key) or "") <= MAX_NAME for key in ("dag_id", "task_id", "group_id"))


class _Linker:
    def __init__(self, graph: MetadataGraph, scan: dict[str, Any]) -> None:
        self.graph = graph
        # Counted with the tasks that cannot be read: such a name cannot be shown either
        self.dropped = sum(
            not _named_sensibly(record)
            for key in ("links", "sql", "unresolved")
            for record in scan.get(key, [])
        )
        lists = ("dags", "tasks", "links", "sql", "unresolved")
        self.scan = {
            **scan,
            **{key: [r for r in scan.get(key, []) if _named_sensibly(r)] for key in lists},
        }
        self.project = scan.get("project") or "."
        self.dags: dict[str, dict[str, Any]] = {}
        self.tasks: dict[tuple[str, str], dict[str, Any]] = {}
        self.annotated: set[tuple[str, str]] = set()
        self.opaque = 0
        self.problems: list[str] = list(scan.get("problems") or [])
        self._artifacts: dict[str, ArtifactVersion] = {}
        self._suffixes: dict[str, set[str]] | None = None

    def run(self) -> None:
        for dag in self.scan.get("dags", []):
            if dag["dag_id"] is not None:
                self.dags.setdefault(dag["dag_id"], dag)
        for task in self.scan.get("tasks", []):
            if task["dag_id"] is not None and task["task_id"] is not None:
                self.tasks.setdefault((task["dag_id"], task["task_id"]), task)
        annotations = self.scan.get("annotations") or {}
        self.problems += annotations.get("problems", [])
        for annotation in annotations.get("tasks", []):
            self._annotation(annotation, annotations.get("file"))
        for link in self.scan.get("links", []):
            if link["status"] == "resolved":
                self._link(link)
        for record in self.scan.get("sql", []):
            if record["status"] == "resolved":
                self._sql(record)
        self._unknowns(annotations.get("acknowledged", []))
        self._dependencies()

    def summary(self) -> AirflowSummary:
        counts = self.graph.stats()
        files = len(self.scan.get("files", {}))
        folders = tuple(self.scan.get("patterns", ()))
        reason = None
        if not files:
            where = ", ".join(folders) or "the DAG folders"
            reason = (
                f"no DAG files were found in {where}; set lineagePilot.airflow.dagFolders "
                "if they are elsewhere"
            )
        unknowns = sum(1 for u in self.graph.unresolved if u.evidence.origin in AIRFLOW_ORIGINS)
        annotations = self.scan.get("annotations") or {}
        return AirflowSummary(
            analysed=files > 0 or bool(annotations.get("tasks")),
            reason=reason,
            folders=folders,
            dag_files=files,
            dags=counts["node_kinds"].get(NodeKind.AIRFLOW_DAG.value, 0),
            tasks=counts["node_kinds"].get(NodeKind.AIRFLOW_TASK.value, 0),
            links=counts["edge_kinds"].get(EdgeKind.ORCHESTRATES.value, 0),
            unknowns=unknowns,
            opaque_tasks=self.opaque + self.dropped,
            annotation_file=annotations.get("file"),
            problems=tuple(self.problems),
        )

    # Paths and evidence

    def _path(self, file: str) -> str:
        """A path in the workspace, relative to the project, as every other path in the graph."""
        return file if self.project == "." else posixpath.relpath(file, self.project)

    def _artifact(self, file: str) -> ArtifactVersion:
        if file not in self._artifacts:
            sha256 = self.scan.get("files", {}).get(file)
            self._artifacts[file] = ArtifactVersion(self._path(file), sha256)
        return self._artifacts[file]

    def _evidence(self, location: dict[str, Any], confidence: str) -> Evidence:
        line = max(location["line"] - 1, 0)
        column = location.get("col") or 0
        return Evidence(
            Origin.DAG_STATIC,
            Confidence(confidence),
            self._artifact(location["file"]),
            Location(self._path(location["file"]), line, column, line, column),
        )

    # Nodes

    def _dag_node(self, dag_id: str, fallback: dict[str, Any] | None = None) -> Node:
        node_id = airflow_dag_id(dag_id)
        existing = self.graph.nodes.get(node_id)
        if existing is not None:
            return existing
        dag = self.dags.get(dag_id) or fallback or {}
        location = dag.get("location") or {}
        file = location.get("file")
        return self.graph.add_node(
            Node(
                id=node_id,
                kind=NodeKind.AIRFLOW_DAG,
                name=dag_id,
                owner=dag.get("owner"),
                file_path=self._path(file) if file else None,
                properties={
                    "dag": dag_id,
                    "operator": dag.get("how"),
                    "line": max(location.get("line", 1) - 1, 0),
                    "col": location.get("col", 0),
                    "tasks": {},
                    "notes": [],
                },
            )
        )

    def _task_node(
        self, dag_id: str, task_id: str, operator: str | None, record: dict[str, Any]
    ) -> Node:
        self._dag_node(dag_id, record)
        node_id = airflow_task_id(dag_id, task_id)
        existing = self.graph.nodes.get(node_id)
        if existing is not None:
            return existing
        location = record.get("location") or {}
        file = location.get("file")
        known = self.tasks.get((dag_id, task_id)) or {}
        return self.graph.add_node(
            Node(
                id=node_id,
                kind=NodeKind.AIRFLOW_TASK,
                name=task_id,
                owner=record.get("owner") or known.get("owner"),
                file_path=self._path(file) if file else None,
                properties={
                    "dag": dag_id,
                    "operator": operator or known.get("operator"),
                    "line": max(location.get("line", 1) - 1, 0),
                    "col": location.get("col", 0),
                    "tasks": {},
                    "notes": [],
                },
            )
        )

    def _note(self, node: Node, notes: list[str]) -> None:
        kept = node.properties["notes"]
        for note in notes:
            if note not in kept and len(kept) < _NOTES_KEPT:
                kept.append(note)

    # dbt tasks

    def _link(self, link: dict[str, Any]) -> None:
        dag_id = link["dag_id"]
        if link["kind"] == "cosmos_dag":
            node = self._dag_node(dag_id, link)
            node.properties["operator"] = "DbtDag"
            node.owner = node.owner or link.get("owner")
        elif link["kind"] == "cosmos_task_group":
            node = self._task_node(dag_id, link["group_id"], "DbtTaskGroup", link)
        else:
            node = self._task_node(dag_id, link["task_id"], link.get("operator"), link)
        self._note(node, link["notes"])
        command = link.get("dbt_command")
        how = "dbt, through Cosmos" if command == "cosmos_render" else f"dbt {command}"
        evidence = self._evidence(link["location"], link["confidence"])
        derived = {row["unique_id"]: row["task_ids"] for row in link.get("tasks") or []}
        selected = link["resolved_nodes"] or []
        for uid in selected:
            if uid not in self.graph.nodes:
                self._selects_missing(node, link, uid)
                continue
            self.graph.add_edge(EdgeKind.ORCHESTRATES, node.id, uid, evidence, transformation=how)
            if uid in derived:
                node.properties["tasks"][uid] = derived[uid]
        if link.get("extra_tasks"):
            node.properties["extraTasks"] = sorted(x["task_id"] for x in link["extra_tasks"])
        if not selected and (command in _WITH_SELECTION or command == "cosmos_render"):
            self._selects_nothing(node, link)

    def _selects_missing(self, node: Node, link: dict[str, Any], uid: str) -> None:
        self.graph.add_unresolved(
            Unresolved(
                subject=node.id,
                reference=uid,
                reason="the DAG selects this node, but the manifest in the graph does not have it; "
                "it may be stale",
                evidence=self._evidence(link["location"], Confidence.HEURISTIC.value),
            )
        )

    def _selects_nothing(self, node: Node, link: dict[str, Any]) -> None:
        selection = " ".join(link.get("select") or []) or link.get("selector") or "everything"
        self.graph.add_unresolved(
            Unresolved(
                subject=node.id,
                reference="selects nothing",
                reason=f"{selection} matches no node in this manifest: was a model renamed?",
                evidence=self._evidence(link["location"], Confidence.HEURISTIC.value),
            )
        )

    # SQL tasks

    def _sql(self, record: dict[str, Any]) -> None:
        node = None
        for tables, how in ((record["reads"], "reads"), (record["writes"], "writes")):
            for table in tables:
                found, partial = self._table(table)
                for target in found:
                    node = node or self._task_node(
                        record["dag_id"], record["task_id"], record["operator"], record
                    )
                    confidence = "heuristic" if partial else record["confidence"]
                    evidence = self._evidence(record["location"], confidence)
                    self.graph.add_edge(
                        EdgeKind.ORCHESTRATES, node.id, target, evidence, transformation=how
                    )
        if node is not None:
            self._note(node, record["notes"])

    def _table(self, parts: list[str | None]) -> tuple[list[str], bool]:
        """The dbt nodes for ``[database, schema, table]``, and whether that was by a partial name.

        A partial name counts only if exactly one relation in the graph ends with it.
        """
        adapter = self.graph.adapter_type
        names = [normalise_identifier(p, adapter) for p in parts if p]
        if not names:
            return [], False
        key = ".".join(names)
        full = f"relation:{key}"
        if full in self.graph.nodes:
            return self._builders(full), False
        matches = self._by_suffix().get(key, set())
        if len(matches) != 1:
            return [], False
        return self._builders(next(iter(matches))), True

    def _builders(self, relation: str) -> list[str]:
        """Who the table belongs to: what builds it, or else the sources that read it."""
        nodes = self.graph.logical_nodes_for(relation)
        built = [n.id for n in nodes if n.kind is not NodeKind.SOURCE]
        return sorted(built or [n.id for n in nodes])

    def _by_suffix(self) -> dict[str, set[str]]:
        if self._suffixes is None:
            self._suffixes = {}
            for node in self.graph.nodes.values():
                if node.kind is NodeKind.RELATION:
                    parts = node.id.removeprefix("relation:").split(".")
                    for start in range(1, len(parts)):
                        key = ".".join(parts[start:])
                        self._suffixes.setdefault(key, set()).add(node.id)
        return self._suffixes

    # Annotations

    def _annotation(self, annotation: dict[str, Any], file: str | None) -> None:
        dag_id, task_id = annotation["dag_id"], annotation["task"]
        self.annotated.add((dag_id, task_id))
        record = {"owner": annotation.get("owner"), "location": {"file": file, "line": 1, "col": 0}}
        known = self.tasks.get((dag_id, task_id))
        if known is not None:
            record = {**known, "owner": annotation.get("owner") or known.get("owner")}
        node = self._task_node(dag_id, task_id, None, record)
        evidence = Evidence(
            Origin.ANNOTATION, Confidence.DECLARED, self._artifact(file), Location(self._path(file))
        )
        for how in ("consumes", "produces", "transforms", "validates"):
            for reference in annotation.get(how, []):
                found, _ = self._reference(reference)
                for target in found:
                    self.graph.add_edge(
                        EdgeKind.ORCHESTRATES, node.id, target, evidence, transformation=how
                    )
                if not found:
                    self.graph.add_unresolved(
                        Unresolved(
                            subject=node.id,
                            reference=reference,
                            reason=f"{file} says this task {how} it, and no dbt node matches",
                            evidence=evidence,
                        )
                    )

    def _reference(self, reference: str) -> tuple[list[str], bool]:
        """What an annotation names: a dbt ID, a table name, or a model's name if it is unique."""
        node = self.graph.nodes.get(reference)
        if node is not None and node.kind in DBT_KINDS:
            return [reference], False
        if "." in reference:
            return self._table(reference.split("."))
        named = [
            n.id
            for n in self.graph.nodes.values()
            if n.kind in DBT_KINDS and n.name.lower() == reference.lower()
        ]
        if len(named) == 1:
            return named, False
        return ([], False) if named else self._table([reference])

    # Unknowns

    def _unknowns(self, acknowledged: list[dict[str, Any]]) -> None:
        for record in self.scan.get("unresolved", []):
            dag_id, task_id = record.get("dag_id"), record.get("task_id")
            if self._answered(record, acknowledged) or (dag_id, task_id) in self.annotated:
                continue
            if record.get("mentions_dbt") is False:
                self.opaque += 1
                continue
            if record.get("category") == "dependency" and dag_id is None:
                continue
            if record.get("category") == "dependency" and not self._has_dag(dag_id):
                continue
            self._unknown(record)

    def _has_dag(self, dag_id: str) -> bool:
        return airflow_dag_id(dag_id) in self.graph.nodes

    @staticmethod
    def _answered(record: dict[str, Any], acknowledged: list[dict[str, Any]]) -> bool:
        for entry in acknowledged:
            if entry.get("file") and entry["file"] != record["file"]:
                continue
            if entry.get("line") and entry["line"] != record["line"]:
                continue
            if entry.get("dag_id") and entry["dag_id"] != record.get("dag_id"):
                continue
            if entry.get("task_id") and entry["task_id"] != record.get("task_id"):
                continue
            return True
        return False

    def _unknown(self, record: dict[str, Any]) -> None:
        dag_id, task_id = record.get("dag_id"), record.get("task_id")
        location = {"file": record["file"], "line": record["line"], "col": record["col"]}
        if dag_id:
            self._dag_node(dag_id, {"location": location})
            subject = airflow_task_id(dag_id, task_id) if task_id else airflow_dag_id(dag_id)
            if task_id:
                self._task_node(dag_id, task_id, record.get("operator"), {"location": location})
        else:
            subject = self._path(record["file"])
        category = record.get("category") or "not_literal"
        where = f"{self._path(record['file'])}:{record['line']}"
        self.graph.add_unresolved(
            Unresolved(
                subject=subject,
                reference=label(category),
                reason=f"{where}: {record['reason']}. {explanation(category)}",
                evidence=self._evidence(location, Confidence.HEURISTIC.value),
            )
        )

    # Task order

    def _dependencies(self) -> None:
        """The DAGs in the graph get all their tasks, and the order between them."""
        in_graph = {n.name for n in self.graph.nodes.values() if n.kind is NodeKind.AIRFLOW_DAG}
        for (dag_id, task_id), task in sorted(self.tasks.items()):
            if dag_id in in_graph:
                self._task_node(dag_id, task_id, task.get("operator"), task)
        for edge in self.scan.get("dependencies", []):
            upstream, downstream = edge.get("upstream"), edge.get("downstream")
            if edge.get("status") == "unknown" or not upstream or not downstream:
                continue
            if upstream in self.graph.nodes and downstream in self.graph.nodes:
                self.graph.add_edge(
                    EdgeKind.TASK_DEPENDS_ON,
                    upstream,
                    downstream,
                    self._evidence(edge["location"], edge["confidence"]),
                    transformation=edge.get("how"),
                )
        # A task in a group finishes before what follows the group
        for node in [n for n in self.graph.nodes.values() if n.kind is NodeKind.AIRFLOW_TASK]:
            parent = node.id.rsplit(".", 1)[0]
            if parent != airflow_dag_id(node.properties["dag"]) and parent in self.graph.nodes:
                location = self.tasks.get(
                    (node.properties["dag"], node.name), {"location": None}
                ).get("location")
                if location:
                    self.graph.add_edge(
                        EdgeKind.TASK_DEPENDS_ON,
                        node.id,
                        parent,
                        self._evidence(location, Confidence.DECLARED.value),
                        transformation="in group",
                    )
