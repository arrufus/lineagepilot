"""The metadata graph (design section 5).

Nodes are dbt nodes, physical relations and, from Phase 5, Airflow DAGs and tasks.
Columns belong to nodes. Edges point the way data or control flows, from ``upstream`` to
``downstream``:

| Kind              | Upstream -> downstream                                    |
|-------------------|-----------------------------------------------------------|
| ``depends_on``    | the dbt node depended on -> the dbt node that depends on it |
| ``derives``       | source column -> derived column                           |
| ``materialises``  | dbt node -> the physical relation it builds or reads      |
| ``orchestrates``  | Airflow task -> the dbt node it runs                      |
| ``task_depends_on`` | upstream Airflow task -> downstream Airflow task        |

Every edge carries evidence: where it came from, how sure it is, which artifact version
it was read from, and where to navigate to. An edge found by more than one source keeps
every piece of evidence. References that cannot be resolved are ``Unresolved`` records,
shown to the user and never dropped (review finding F07).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class NodeKind(str, Enum):
    MODEL = "model"
    SEED = "seed"
    SNAPSHOT = "snapshot"
    SOURCE = "source"
    EXPOSURE = "exposure"
    RELATION = "relation"
    AIRFLOW_DAG = "airflow_dag"
    AIRFLOW_TASK = "airflow_task"


DBT_KINDS = frozenset(
    {NodeKind.MODEL, NodeKind.SEED, NodeKind.SNAPSHOT, NodeKind.SOURCE, NodeKind.EXPOSURE}
)


class EdgeKind(str, Enum):
    DEPENDS_ON = "depends_on"
    DERIVES = "derives"
    MATERIALISES = "materialises"
    ORCHESTRATES = "orchestrates"
    TASK_DEPENDS_ON = "task_depends_on"


class Origin(str, Enum):
    """Where a fact was read from."""

    MANIFEST = "manifest"
    COMPILED_SQL = "compiled_sql"
    CATALOG = "catalog"
    DAG_STATIC = "dag_static"
    ANNOTATION = "annotation"


class Confidence(str, Enum):
    """How sure we are: the artifact states it, parsing derived it, or a pattern guessed it."""

    DECLARED = "declared"
    INFERRED = "inferred"
    HEURISTIC = "heuristic"


@dataclass(frozen=True, slots=True)
class ArtifactVersion:
    """The file a fact was read from, and which version of it."""

    path: str
    sha256: str | None = None
    generated_at: str | None = None  # dbt artifacts only
    dbt_version: str | None = None  # dbt artifacts only


@dataclass(frozen=True, slots=True)
class Location:
    """A place to navigate to. Lines and characters are 0-based, as in LSP."""

    path: str
    start_line: int | None = None
    start_character: int | None = None
    end_line: int | None = None
    end_character: int | None = None


@dataclass(frozen=True, slots=True)
class Evidence:
    origin: Origin
    confidence: Confidence
    artifact: ArtifactVersion | None = None
    location: Location | None = None


@dataclass(slots=True)
class Node:
    """A node of the graph. Fields that do not apply to a kind stay empty."""

    id: str
    kind: NodeKind
    name: str
    package: str | None = None
    database: str | None = None
    schema: str | None = None
    relation_id: str | None = None  # the physical relation a dbt node materialises
    alias: str | None = None
    version: str | None = None
    layer: str | None = None
    layer_source: str | None = None  # meta, tag, pattern or default
    domain: str | None = None
    description: str | None = None
    materialization: str | None = None
    owner: str | None = None
    group: str | None = None
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    test_count: int = 0
    test_types: list[str] = field(default_factory=list)
    file_path: str | None = None  # the file that defines it, relative to the project
    patch_path: str | None = None  # the YAML file that documents it
    properties: dict[str, Any] = field(default_factory=dict)
    language: str | None = None  # sql or python, for models
    compiled_path: str | None = None  # where dbt writes the compiled SQL
    source_sha256: str | None = None  # of a model's source text, as the manifest saw it
    in_catalog: bool = False  # catalog.json has an entry for it
    primary_key: list[str] = field(default_factory=list)  # contract primary-key columns

    @property
    def has_tests(self) -> bool:
        return self.test_count > 0


@dataclass(slots=True)
class Column:
    id: str
    node_id: str
    name: str  # as written in the manifest or catalog
    data_type: str | None = None
    ordinal: int | None = None
    description: str | None = None
    semantic_type: str | None = None
    pii_type: str | None = None
    pii_detected_by: str | None = None
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    test_count: int = 0
    test_types: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    in_catalog: bool = False  # catalog.json lists it, so it exists in the warehouse

    @property
    def has_tests(self) -> bool:
        return self.test_count > 0


@dataclass(slots=True)
class Edge:
    kind: EdgeKind
    upstream: str
    downstream: str
    evidence: list[Evidence] = field(default_factory=list)
    transformation: str | None = None  # source, ref or dependency; for derives, the expression kind

    @property
    def confidence(self) -> Confidence:
        """The strongest confidence among the evidence."""
        order = (Confidence.DECLARED, Confidence.INFERRED, Confidence.HEURISTIC)
        return min((e.confidence for e in self.evidence), key=order.index)


@dataclass(frozen=True, slots=True)
class Unresolved:
    """A reference that could not be resolved, at ``subject``."""

    subject: str  # the node or column where the reference appears
    reference: str  # what it points at
    reason: str
    evidence: Evidence


class LineageState(str, Enum):
    RESOLVED = "resolved"  # every output column was traced
    PARTIAL = "partial"  # some references could not be resolved; see Unresolved records
    UNKNOWN = "unknown"  # not analysed, for the reason given; never "no lineage"


@dataclass(frozen=True, slots=True)
class ColumnLineageStatus:
    state: LineageState
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class AirflowSummary:
    """What a DAG scan looked at and found (task 5.3). Not part of a snapshot."""

    analysed: bool
    reason: str | None = None  # why not, or why nothing was found
    folders: tuple[str, ...] = ()
    dag_files: int = 0
    dags: int = 0  # those that touch this project
    tasks: int = 0
    links: int = 0
    unknowns: int = 0
    opaque_tasks: int = 0  # commands that cannot be read and do not mention dbt
    annotation_file: str | None = None
    problems: tuple[str, ...] = ()


class GraphError(ValueError):
    """An operation that would make the graph inconsistent."""


class MetadataGraph:
    """An in-memory graph of nodes, columns, evidence-carrying edges and unresolved records."""

    def __init__(self, adapter_type: str | None = None) -> None:
        self.adapter_type = adapter_type
        # The manifest the dbt part of the graph was built from, for evidence records
        self.manifest: ArtifactVersion | None = None
        self.project_name: str | None = None
        self.schema_version: int | None = None  # the manifest's dbt_schema_version
        self.nodes: dict[str, Node] = {}
        self.columns: dict[str, Column] = {}
        self.unresolved: list[Unresolved] = []
        self._columns_by_node: defaultdict[str, list[str]] = defaultdict(list)
        self._edges: dict[tuple[EdgeKind, str, str], Edge] = {}
        self._out: defaultdict[str, list[Edge]] = defaultdict(list)
        self._in: defaultdict[str, list[Edge]] = defaultdict(list)
        self._files: defaultdict[str, set[str]] = defaultdict(set)
        # Column lineage state per model or snapshot; absent means not computed yet
        self.column_lineage: dict[str, ColumnLineageStatus] = {}
        # What each model's compiled SQL selects, in order, as written there. Not the same as
        # its columns in the graph, which also hold what the catalog and the docs list: the
        # Change Impact Review compares this with a baseline. Absent when it cannot be known.
        self.output_columns: dict[str, list[str]] = {}
        # What the last DAG scan found; None until one has been applied
        self.airflow: AirflowSummary | None = None

    # Building

    def add_node(self, node: Node) -> Node:
        """Add a node. Adding an existing relation returns the one already there."""
        existing = self.nodes.get(node.id)
        if existing is not None:
            if node.kind is NodeKind.RELATION and existing.kind is NodeKind.RELATION:
                return existing
            raise GraphError(f"duplicate node: {node.id}")
        self.nodes[node.id] = node
        for path in (node.file_path, node.patch_path):
            if path:
                self._files[_normalise_path(path)].add(node.id)
        return node

    def add_column(self, column: Column) -> Column:
        if column.node_id not in self.nodes:
            raise GraphError(f"column {column.id} belongs to an unknown node")
        if column.id in self.columns:
            raise GraphError(f"duplicate column: {column.id}")
        self.columns[column.id] = column
        self._columns_by_node[column.node_id].append(column.id)
        return column

    def add_edge(
        self,
        kind: EdgeKind,
        upstream: str,
        downstream: str,
        evidence: Evidence,
        transformation: str | None = None,
    ) -> Edge:
        """Add an edge, or add evidence to the same edge if it exists.

        Both ends must already be in the graph; a reference that cannot be resolved is an
        ``Unresolved`` record instead.
        """
        for end in (upstream, downstream):
            if end not in self.nodes and end not in self.columns:
                raise GraphError(f"edge {kind.value} {upstream} -> {downstream}: {end} is unknown")
        key = (kind, upstream, downstream)
        edge = self._edges.get(key)
        if edge is None:
            edge = Edge(kind, upstream, downstream, [evidence], transformation)
            self._edges[key] = edge
            self._out[upstream].append(edge)
            self._in[downstream].append(edge)
        elif evidence not in edge.evidence:
            edge.evidence.append(evidence)
        return edge

    def add_unresolved(self, record: Unresolved) -> None:
        self.unresolved.append(record)

    def remove_edge(self, edge: Edge) -> None:
        key = (edge.kind, edge.upstream, edge.downstream)
        if self._edges.pop(key, None) is None:
            raise GraphError(
                f"no such edge: {edge.kind.value} {edge.upstream} -> {edge.downstream}"
            )
        self._out[edge.upstream].remove(edge)
        self._in[edge.downstream].remove(edge)

    def remove_node(self, node_id: str) -> None:
        """Remove a node, with its edges. Its columns must be gone first."""
        node = self.nodes.pop(node_id)
        for edge in (*self._out.pop(node_id, ()), *self._in.pop(node_id, ())):
            if self._edges.pop((edge.kind, edge.upstream, edge.downstream), None) is not None:
                other = edge.downstream if edge.upstream == node_id else edge.upstream
                for side in (self._in, self._out):
                    if other in side and edge in side[other]:
                        side[other].remove(edge)
        for path in (node.file_path, node.patch_path):
            if path:
                self._files[_normalise_path(path)].discard(node_id)

    def remove_unresolved(self, subjects: Iterable[str], origin: Origin | None = None) -> None:
        """Drop the unresolved records of these subjects, optionally only from one origin."""
        wanted = set(subjects)
        self.unresolved = [
            u
            for u in self.unresolved
            if u.subject not in wanted or (origin is not None and u.evidence.origin is not origin)
        ]

    # Queries

    def node(self, node_id: str) -> Node:
        try:
            return self.nodes[node_id]
        except KeyError:
            raise KeyError(f"unknown node: {node_id}") from None

    def columns_of(self, node_id: str) -> list[Column]:
        return [self.columns[c] for c in self._columns_by_node.get(node_id, ())]

    @property
    def edges(self) -> Iterator[Edge]:
        return iter(self._edges.values())

    def edge(self, kind: EdgeKind, upstream: str, downstream: str) -> Edge | None:
        return self._edges.get((kind, upstream, downstream))

    def out_edges(self, ref: str, kinds: Iterable[EdgeKind] | None = None) -> list[Edge]:
        """Edges leaving a node or column, optionally of the given kinds."""
        return _of_kinds(self._out.get(ref, ()), kinds)

    def in_edges(self, ref: str, kinds: Iterable[EdgeKind] | None = None) -> list[Edge]:
        """Edges entering a node or column, optionally of the given kinds."""
        return _of_kinds(self._in.get(ref, ()), kinds)

    def nodes_for_file(self, path: str) -> list[Node]:
        """Nodes defined or documented by a file (a path relative to the project)."""
        return [self.nodes[n] for n in sorted(self._files.get(_normalise_path(path), ()))]

    def logical_nodes_for(self, relation: str) -> list[Node]:
        """The dbt nodes that materialise a physical relation."""
        return [self.nodes[e.upstream] for e in self.in_edges(relation, (EdgeKind.MATERIALISES,))]

    def unresolved_for(self, subject: str) -> list[Unresolved]:
        return [u for u in self.unresolved if u.subject == subject]

    def stats(self) -> dict[str, Any]:
        node_kinds: dict[str, int] = defaultdict(int)
        for node in self.nodes.values():
            node_kinds[node.kind.value] += 1
        edge_kinds: dict[str, int] = defaultdict(int)
        for edge in self._edges.values():
            edge_kinds[edge.kind.value] += 1
        return {
            "nodes": len(self.nodes),
            "columns": len(self.columns),
            "edges": len(self._edges),
            "unresolved": len(self.unresolved),
            "node_kinds": dict(sorted(node_kinds.items())),
            "edge_kinds": dict(sorted(edge_kinds.items())),
        }


def _of_kinds(edges: Iterable[Edge], kinds: Iterable[EdgeKind] | None) -> list[Edge]:
    if kinds is None:
        return list(edges)
    wanted = frozenset(kinds)
    return [e for e in edges if e.kind in wanted]


def _normalise_path(path: str) -> str:
    path = path.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    return path
