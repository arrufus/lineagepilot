"""The dbt semantic layer in the graph (task 7.8; design_next_phase sections 6.3 and 7.1).

Semantic models, metrics and saved queries are consumer nodes with the tool ``dbt_semantic`` and
their dbt ``unique_id`` as their ID. ``build_dbt_graph`` adds them with the rest of the manifest,
and turns their ``depends_on`` into ``consumes`` edges: a semantic model reads its model, a metric
its semantic model or the metrics it is built from, a saved query its metrics. This module adds
what they read column by column:

- each entity, dimension and measure of a semantic model is a column under it, and consumes the
  model's columns its ``expr`` names (parsed with sqlglot, ``inferred``), or the column of its own
  name (``declared``);
- a metric consumes the measures it uses. One written in the newer YAML has no measure: it
  consumes the model columns its ``expr`` names. A metric's filters consume the dimensions and
  entities they name;
- a saved query consumes the dimensions and entities it groups and filters by.

A read from a node into a column also gets a ``consumes`` edge between the two nodes, so traversal
at model level sees it. Whatever cannot be read is an unknown, never a silent gap: an expression
sqlglot cannot parse, a column the model does not list, a name no semantic model has.
``graph.consumer_lineage`` says how completely each node's reads are known, and impact uses it as
it uses a model's column lineage.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass

from lineagepilot_core.graph.ids import column_id
from lineagepilot_core.graph.model import (
    Column,
    ColumnLineageStatus,
    Confidence,
    EdgeKind,
    Evidence,
    LineageState,
    Location,
    MetadataGraph,
    Origin,
    Unresolved,
)
from lineagepilot_core.parsers.base import ParseResult, RawMetric, RawSavedQuery, RawSemanticModel
from lineagepilot_core.parsers.sql_lineage import short_error, sqlglot_dialect

# An expr longer than this is not parsed: it is an unknown
MAX_EXPR_CHARS = 10_000
# How much of an expression or a template an unknown quotes
_QUOTED = 200

# MetricFlow's names in filters and group-bys: {{ Dimension('order__order_size') }},
# TimeDimension('order__ordered_at', 'week'), Entity('customer'), Metric('revenue', ...)
_REFERENCE = re.compile(
    r"""\b(Dimension|TimeDimension|Entity|Metric)\s*\(\s*(['"])([^'"\n]{1,500})\2"""
)
_CALL = re.compile(r"\b(Dimension|TimeDimension|Entity|Metric)\s*\(")
_ROLES = {"Dimension": "dimension", "TimeDimension": "dimension", "Entity": "entity"}
# Every metric has this time dimension: MetricFlow's own, not a column of a model
METRIC_TIME = "metric_time"
# A time dimension can be named with its grain: order__ordered_at__week
GRAINS = frozenset(
    {
        "nanosecond",
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
    }
)
_TEMPLATED = ("{{", "{%")
_ARTICLES = {"entity": "an", "dimension": "a", "measure": "a"}


@dataclass(frozen=True, slots=True)
class _Field:
    column: str  # the field's column ID
    owner: str  # its semantic model


class _Reads:
    """How completely one node's reads are known."""

    def __init__(self) -> None:
        self.unknown: str | None = None
        self.unresolved = 0

    def status(self) -> ColumnLineageStatus:
        if self.unknown is not None:
            return ColumnLineageStatus(LineageState.UNKNOWN, self.unknown)
        if self.unresolved:
            count = f"{self.unresolved} reference" + ("" if self.unresolved == 1 else "s")
            return ColumnLineageStatus(LineageState.PARTIAL, f"{count} did not resolve")
        return ColumnLineageStatus(LineageState.RESOLVED)


def add_semantic_layer(graph: MetadataGraph, result: ParseResult) -> None:
    """Add the semantic models' fields, and the column-level reads of the whole layer."""
    linker = _Linker(graph)
    for semantic_model in result.semantic_models:
        if semantic_model.node_id in graph.nodes:
            linker.semantic_model(semantic_model)
    for metric in result.metrics:
        if metric.node_id in graph.nodes:
            linker.metric(metric)
    for query in result.saved_queries:
        if query.node_id in graph.nodes:
            linker.saved_query(query)


class _Linker:
    def __init__(self, graph: MetadataGraph) -> None:
        self.graph = graph
        self.adapter = graph.adapter_type
        self.dialect = sqlglot_dialect(graph.adapter_type)
        # (role, name folded) -> the fields of that name; MetricFlow's names are case-insensitive
        self.fields: defaultdict[tuple[str, str], list[_Field]] = defaultdict(list)
        self.entities: defaultdict[str, set[str]] = defaultdict(set)  # semantic model -> entities
        self.by_name: defaultdict[str, list[str]] = defaultdict(list)  # semantic models by name
        self.model_of: dict[str, str | None] = {}  # semantic model -> its model, if in the graph

    # Nodes

    def semantic_model(self, raw: RawSemanticModel) -> None:
        node_id = raw.node_id
        reads = _Reads()
        model = raw.model_id if raw.model_id in self.graph.nodes else None
        self.model_of[node_id] = model
        self.by_name[self.graph.nodes[node_id].name.lower()].append(node_id)
        if model is None:
            # A model the manifest names but the graph lacks is already an unresolved dependency
            reads.unknown = "its model is not in the graph"
            if raw.model_id is None:
                self._unresolved(node_id, "model", "the manifest names no model for it")
        for field in raw.fields:
            column = column_id(node_id, field.name, self.adapter)
            if column not in self.graph.columns:
                self.graph.add_column(
                    Column(
                        id=column,
                        node_id=node_id,
                        name=field.name,
                        description=field.description,
                        semantic_type=field.role,
                        data_type=field.type,
                    )
                )
            self.fields[(field.role, field.name.lower())].append(_Field(column, node_id))
            if field.role == "entity":
                self.entities[node_id].add(field.name.lower())
            if model is not None:
                self._expression(node_id, column, model, field.expr, field.name, reads)
        self.graph.consumer_lineage[node_id] = reads.status()

    def metric(self, raw: RawMetric) -> None:
        node_id = raw.node_id
        reads = _Reads()
        read_whole = {e.downstream for e in self.graph.out_edges(node_id, (EdgeKind.CONSUMES,))}
        for measure in raw.measures:
            self._field(node_id, "measure", measure, (), read_whole, reads)
        if raw.semantic_model is not None:
            found = self.by_name.get(raw.semantic_model.lower(), [])
            chosen = [s for s in found if s in read_whole] or found
            model = self.model_of.get(chosen[0]) if len(chosen) == 1 else None
            if len(chosen) != 1:
                reason = (
                    "no semantic model has this name"
                    if not chosen
                    else "several semantic models have this name"
                )
                reads.unknown = f"its semantic model {raw.semantic_model!r}: {reason}"
                self._unresolved(node_id, raw.semantic_model, reason)
            elif model is None:
                reads.unknown = "its semantic model's model is not in the graph"
            else:
                self._expression(node_id, node_id, model, raw.expr, None, reads)
        self._references(node_id, raw.references, read_whole, reads)
        self.graph.consumer_lineage[node_id] = reads.status()

    def saved_query(self, raw: RawSavedQuery) -> None:
        reads = _Reads()
        read_whole = {e.downstream for e in self.graph.out_edges(raw.node_id, (EdgeKind.CONSUMES,))}
        self._references(raw.node_id, raw.references, read_whole, reads)
        self.graph.consumer_lineage[raw.node_id] = reads.status()

    # Reads

    def _expression(
        self,
        node_id: str,
        reader: str,
        model: str,
        expr: str | None,
        name: str | None,
        reads: _Reads,
    ) -> None:
        """``reader`` (a field, or a metric itself) reads the model columns ``expr`` names."""
        names, confidence, reason = self._columns_in(expr, name)
        if names is None:
            label = name or self.graph.nodes[node_id].name
            reads.unknown = f"the expression of {label} cannot be read"
            self._unresolved(reader, _quoted(expr or ""), reason or "")
            return
        for column_name in names:
            target = column_id(model, column_name, self.adapter)
            if target not in self.graph.columns:
                reads.unresolved += 1
                self._unresolved(
                    reader,
                    f"{self.graph.nodes[model].name}.{column_name}",
                    f"{model} has no column {column_name!r} in the catalog or the docs; run dbt "
                    "docs generate",
                )
                continue
            self._consumes(node_id, reader, target, model, confidence)

    def _columns_in(
        self, expr: str | None, name: str | None
    ) -> tuple[list[str] | None, Confidence, str | None]:
        """The columns an expression names, and how sure that is; or None and why not."""
        if expr is None:
            # MetricFlow reads the column of the field's own name
            return ([name] if name else []), Confidence.DECLARED, None
        if any(mark in expr for mark in _TEMPLATED):
            return None, Confidence.HEURISTIC, "the expression is templated"
        if len(expr) > MAX_EXPR_CHARS:
            return (
                None,
                Confidence.HEURISTIC,
                f"the expression is {len(expr):,} characters, over the {MAX_EXPR_CHARS:,} limit",
            )
        from sqlglot import exp, parse_one

        try:
            tree = parse_one(expr, dialect=self.dialect)
        except Exception as e:  # sqlglot raises several types; hostile SQL can recurse deeply
            return (
                None,
                Confidence.HEURISTIC,
                f"the expression could not be parsed: {short_error(e)}",
            )
        if tree.find(exp.Query) is not None:
            return None, Confidence.HEURISTIC, "the expression has a subquery"
        names = list(dict.fromkeys(c.name for c in tree.find_all(exp.Column) if c.name))
        # A bare column name says which column as plainly as no expr does
        plain = isinstance(tree, exp.Column) and not tree.table
        return names, Confidence.DECLARED if plain else Confidence.INFERRED, None

    def _field(
        self,
        node_id: str,
        role: str,
        element: str,
        path: tuple[str, ...],
        read_whole: set[str],
        reads: _Reads,
        reference: str | None = None,
    ) -> None:
        """``node_id`` reads the ``role`` called ``element``, joined through the entities in
        ``path``. Several candidates are each read, as ``heuristic``."""
        candidates = self.fields.get((role, element.lower()), [])
        if path:
            via = path[-1].lower()
            joined = [f for f in candidates if via in self.entities[f.owner]]
            chosen = joined or candidates
            sure = bool(joined) and len(joined) == 1
        else:
            chosen = [f for f in candidates if f.owner in read_whole] or candidates
            sure = len(chosen) == 1
        if not chosen:
            reads.unresolved += 1
            self._unresolved(
                node_id,
                reference or element,
                f"no semantic model has {_ARTICLES[role]} {role} named {element!r}",
            )
            return
        confidence = Confidence.DECLARED if sure else Confidence.HEURISTIC
        for field in chosen:
            self._consumes(node_id, node_id, field.column, field.owner, confidence)

    def _references(
        self, node_id: str, templates: list[str], read_whole: set[str], reads: _Reads
    ) -> None:
        """The dimensions and entities filters and group-bys name. Metrics they name are the
        node's ``depends_on`` already."""
        for template in templates:
            found = _REFERENCE.findall(template)
            if len(found) < len(_CALL.findall(template)):
                reads.unknown = "a filter or group-by names something that cannot be read"
                self._unresolved(
                    node_id, _quoted(template), "a reference here cannot be read as a name"
                )
            for call, _, name in found:
                if call == "Metric":
                    continue
                role = _ROLES[call]
                parts = tuple(name.lower().split("__"))
                element, path = parts[-1], parts[:-1]
                if role == "dimension" and element in GRAINS and path:
                    if (role, element) not in self.fields:
                        element, path = path[-1], path[:-1]
                if role == "dimension" and element == METRIC_TIME:
                    continue
                self._field(node_id, role, element, path, read_whole, reads, name)

    # Graph

    def _consumes(
        self, node_id: str, reader: str, target: str, owner: str, confidence: Confidence
    ) -> None:
        """``reader`` consumes the column ``target`` of ``owner``; ``node_id`` consumes ``owner``
        as a node, unless it already does."""
        evidence = self._evidence(node_id, confidence)
        self.graph.add_edge(EdgeKind.CONSUMES, reader, target, evidence)
        if owner != node_id and self.graph.edge(EdgeKind.CONSUMES, node_id, owner) is None:
            self.graph.add_edge(EdgeKind.CONSUMES, node_id, owner, evidence)

    def _evidence(self, node_id: str, confidence: Confidence) -> Evidence:
        path = self.graph.nodes[node_id].file_path
        return Evidence(
            origin=Origin.MANIFEST,
            confidence=confidence,
            artifact=self.graph.manifest,
            location=Location(path) if path else None,
        )

    def _unresolved(self, subject: str, reference: str, reason: str) -> None:
        node_id = subject.partition("#")[0]
        self.graph.add_unresolved(
            Unresolved(
                subject=subject,
                reference=reference,
                reason=reason,
                evidence=self._evidence(node_id, Confidence.DECLARED),
            )
        )


def _quoted(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _QUOTED else text[: _QUOTED - 1] + "…"
