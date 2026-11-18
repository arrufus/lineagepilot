"""Pipelines, tasks and consumers in the graph (task 7.1, decision D-9).

Generic ``pipeline``, ``task`` and ``consumer`` kinds, each with a ``tool``; the edges ``loads``,
``consumes`` and ``part_of``. An Airflow DAG and task keep their IDs, and are written out as
``airflow_dag`` and ``airflow_task`` until the protocol has the generic kinds (task 7.5).
"""

from __future__ import annotations

import json

import pytest

from lineagepilot_core.analysis import Direction, Granularity, impact, steps, traverse
from lineagepilot_core.analysis.pii import pii_exposure, pii_index
from lineagepilot_core.analysis.review import KIND_ORDER
from lineagepilot_core.export import export_graph, graph_view
from lineagepilot_core.export.graph import ExportFormat
from lineagepilot_core.graph import (
    DBT_KINDS,
    TOOL_KINDS,
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    kind_name,
    legacy_kind,
)
from lineagepilot_core.graph.model import AirflowSummary
from lineagepilot_core.rules import Category, Outcome, Result, Rule, Scope, Severity, evaluate
from lineagepilot_core.snapshot import from_dict, to_dict

DOWN, UP = Direction.DOWNSTREAM, Direction.UPSTREAM
DECLARED = Evidence(Origin.MANIFEST, Confidence.DECLARED)
NOTED = Evidence(Origin.ANNOTATION, Confidence.DECLARED)

ORDERS = "model.shop.orders"
RAW = "source.shop.raw.orders"
TABLE = "relation:db.raw.orders"
VIEW = "looker:view.orders"
EXPLORE = "looker:shop.orders"
METRIC = "metric.shop.revenue"
LOADER = "dagster:ingest.load_orders"
JOB = "dagster:ingest"


def tool_graph() -> MetadataGraph:
    """A source, a model on it with PII, a loader upstream and consumers downstream.

    ingest (pipeline) -- load_orders (task) --loads--> raw.orders (table) <- source
    source -> orders (model) <-consumes- orders view (consumer) -part_of-> orders explore
    orders#email <-consumes- view#customer_email ; orders <-consumes- revenue (metric)
    """
    graph = MetadataGraph()
    graph.add_node(Node(TABLE, NodeKind.RELATION, "orders"))
    graph.add_node(Node(RAW, NodeKind.SOURCE, "orders", relation_id=TABLE))
    graph.add_node(Node(ORDERS, NodeKind.MODEL, "orders", layer="marts"))
    graph.add_node(Node(JOB, NodeKind.PIPELINE, "ingest", tool="dagster", subkind="job"))
    graph.add_node(Node(LOADER, NodeKind.TASK, "load_orders", tool="dagster", subkind="asset"))
    for node_id, name, subkind in ((VIEW, "orders", "view"), (EXPLORE, "orders", "explore")):
        graph.add_node(Node(node_id, NodeKind.CONSUMER, name, tool="looker", subkind=subkind))
    graph.add_node(Node(METRIC, NodeKind.CONSUMER, "revenue", tool="dbt_semantic"))

    graph.add_column(Column(f"{ORDERS}#email", ORDERS, "email", pii_type="email"))
    graph.add_column(Column(f"{ORDERS}#amount", ORDERS, "amount"))
    # A consumer's column, with a name that would be PII in a model
    graph.add_column(Column(f"{VIEW}#customer_email", VIEW, "customer_email", pii_type="email"))

    graph.add_edge(EdgeKind.MATERIALISES, RAW, TABLE, DECLARED)
    graph.add_edge(EdgeKind.DEPENDS_ON, RAW, ORDERS, DECLARED)
    graph.add_edge(EdgeKind.TASK_DEPENDS_ON, JOB, LOADER, NOTED)
    graph.add_edge(EdgeKind.LOADS, LOADER, TABLE, NOTED)
    graph.add_edge(EdgeKind.CONSUMES, VIEW, ORDERS, NOTED)
    graph.add_edge(EdgeKind.CONSUMES, f"{VIEW}#customer_email", f"{ORDERS}#email", NOTED)
    graph.add_edge(EdgeKind.PART_OF, VIEW, EXPLORE, NOTED)
    graph.add_edge(EdgeKind.CONSUMES, METRIC, ORDERS, NOTED)
    return graph


def refs(graph: MetadataGraph, ref: str, direction: Direction, by: Granularity) -> set[str]:
    return {s.ref for s in steps(graph, ref, direction, by)}


class TestKinds:
    @pytest.mark.parametrize(
        ("kind", "tool", "name", "legacy"),
        [
            (NodeKind.PIPELINE, "airflow", "pipeline", "airflow_dag"),
            (NodeKind.TASK, "airflow", "task", "airflow_task"),
            (NodeKind.PIPELINE, "dagster", "pipeline", "pipeline"),
            (NodeKind.TASK, "dagster", "task", "task"),
            (NodeKind.CONSUMER, "looker", "consumer", "consumer"),
            (NodeKind.CONSUMER, "airflow", "consumer", "consumer"),
            (NodeKind.MODEL, None, "model", "model"),
        ],
    )
    def test_how_a_kind_is_written_out(self, kind, tool, name, legacy):
        """Protocol 2 writes the kind, with the tool beside it; protocol 1 wrote Airflow's
        own kinds, and had no others."""
        assert kind_name(kind, tool) == name
        assert Node("x", kind, "x", tool=tool).kind_name == name
        assert legacy_kind(name, tool) == legacy

    def test_tool_kinds_are_not_dbt_kinds(self):
        assert not TOOL_KINDS & DBT_KINDS

    def test_every_kind_written_out_has_a_place_in_the_review(self):
        tools = (None, "airflow", "dagster", "looker")
        written = {kind_name(k, t) for k in NodeKind if k is not NodeKind.RELATION for t in tools}
        legacy = {legacy_kind(k, t) for k in written for t in tools}
        assert written | legacy <= set(KIND_ORDER)

    def test_stats_count_kinds_as_written_out(self):
        stats = tool_graph().stats()
        assert stats["node_kinds"]["consumer"] == 3 and stats["node_kinds"]["task"] == 1
        graph = MetadataGraph()
        graph.add_node(Node("airflow:d.t", NodeKind.TASK, "t", tool="airflow"))
        assert graph.stats()["node_kinds"] == {"task": 1}
        assert graph.stats(legacy=True)["node_kinds"] == {"airflow_task": 1}
        assert stats["edge_kinds"]["consumes"] == 3 and stats["edge_kinds"]["loads"] == 1


class TestSummaries:
    def test_one_summary_per_reader_and_airflow_is_one_of_them(self):
        graph = MetadataGraph()
        assert graph.readers == {} and graph.airflow is None
        summary = AirflowSummary(analysed=True, dag_files=2)
        graph.airflow = summary
        assert graph.readers == {"airflow": summary} and graph.airflow is summary
        graph.airflow = None
        assert graph.readers == {}


class TestSnapshots:
    def test_tool_and_subkind_survive_a_snapshot(self):
        graph = tool_graph()
        restored = from_dict(json.loads(json.dumps(to_dict(graph))))
        node = restored.nodes[LOADER]
        assert (node.kind, node.tool, node.subkind) == (NodeKind.TASK, "dagster", "asset")
        assert restored.edge(EdgeKind.LOADS, LOADER, TABLE) is not None

    def test_a_dbt_node_is_stored_as_before(self):
        """Nothing new in a snapshot of a dbt project: the fields stay at their defaults."""
        graph = MetadataGraph()
        graph.add_node(Node(ORDERS, NodeKind.MODEL, "orders"))
        [record] = to_dict(graph)["nodes"]
        assert "tool" not in record and "subkind" not in record


class TestTraversal:
    def test_a_consumer_is_downstream_of_what_it_reads(self):
        graph = tool_graph()
        assert {VIEW, METRIC} <= refs(graph, ORDERS, DOWN, Granularity.MODEL)
        assert refs(graph, METRIC, UP, Granularity.MODEL) == {ORDERS}

    def test_what_a_consumer_is_part_of_is_downstream_of_it(self):
        graph = tool_graph()
        assert refs(graph, VIEW, DOWN, Granularity.MODEL) == {EXPLORE}
        assert refs(graph, EXPLORE, UP, Granularity.MODEL) == {VIEW}
        found = traverse(graph, [ORDERS], DOWN)
        assert found.reached[EXPLORE].depth == 2

    def test_consumers_are_leaves(self):
        graph = tool_graph()
        assert refs(graph, METRIC, DOWN, Granularity.MODEL) == set()

    def test_a_consumers_column_is_downstream_of_the_column_it_reads(self):
        graph = tool_graph()
        email, field = f"{ORDERS}#email", f"{VIEW}#customer_email"
        assert refs(graph, email, DOWN, Granularity.COLUMN) == {field}
        assert refs(graph, field, UP, Granularity.COLUMN) == {email}
        assert refs(graph, f"{ORDERS}#amount", DOWN, Granularity.COLUMN) == set()

    def test_a_consumer_that_reads_a_whole_node_is_not_a_column_step(self):
        graph = tool_graph()
        graph.add_edge(EdgeKind.CONSUMES, METRIC, f"{ORDERS}#amount", NOTED)
        assert refs(graph, f"{ORDERS}#amount", DOWN, Granularity.COLUMN) == set()

    def test_a_task_loads_a_table_that_a_source_reads(self):
        graph = tool_graph()
        [step] = steps(graph, LOADER, DOWN, Granularity.MODEL)
        assert step.ref == RAW
        assert [e.kind for e in step.edges] == [EdgeKind.LOADS, EdgeKind.MATERIALISES]
        found = traverse(graph, [JOB], DOWN)
        assert (found.reached[LOADER].depth, found.reached[RAW].depth) == (1, 2)
        assert found.reached[ORDERS].depth == 3 and TABLE not in found.reached

    def test_upstream_of_a_source_is_what_loads_its_table(self):
        graph = tool_graph()
        assert LOADER in refs(graph, RAW, UP, Granularity.MODEL)
        assert traverse(graph, [ORDERS], UP).reached[JOB].depth == 3

    def test_a_task_can_load_a_source_directly(self):
        graph = tool_graph()
        graph.add_node(Node("source.shop.raw.items", NodeKind.SOURCE, "items"))
        graph.add_edge(EdgeKind.LOADS, LOADER, "source.shop.raw.items", NOTED)
        assert refs(graph, LOADER, DOWN, Granularity.MODEL) == {RAW, "source.shop.raw.items"}
        assert refs(graph, "source.shop.raw.items", UP, Granularity.MODEL) == {LOADER}


class TestImpact:
    def test_a_changed_column_reaches_the_consumers_that_read_it(self):
        graph = tool_graph()
        found = impact(graph, [f"{ORDERS}#email"])
        assert found.nodes[VIEW].columns == [f"{VIEW}#customer_email"]
        assert found.nodes[VIEW].kind_name == "consumer"
        assert {METRIC, EXPLORE} <= set(found.nodes)
        assert found.counts()["consumer"] == 3

    def test_a_changed_source_reaches_nothing_upstream(self):
        found = impact(tool_graph(), [RAW])
        assert LOADER not in found.nodes and JOB not in found.nodes


class TestRulesAndPII:
    """No rule and no PII indicator applies to a pipeline, task or consumer."""

    def test_no_builtin_rule_applies(self):
        graph = tool_graph()
        subjects = {f.subject.partition("#")[0] for f in evaluate(graph).findings}
        assert not subjects & {n.id for n in graph.nodes.values() if n.kind in TOOL_KINDS}

    def test_not_even_a_rule_that_names_them(self):
        def always_fails(graph, node, column):
            return Outcome(Result.FAIL, "no")

        rule = Rule(
            id="X001",
            name="everything fails",
            description="",
            severity=Severity.ERROR,
            category=Category.STRUCTURAL,
            scope=Scope.NODE,
            check=always_fails,
            resource_types=frozenset(NodeKind),
        )
        subjects = {f.subject for f in evaluate(tool_graph(), rules=[rule]).findings}
        assert subjects == {RAW, ORDERS}

    def test_pii_does_not_flow_into_a_consumers_column(self):
        graph = tool_graph()
        index = pii_index(graph)
        assert set(index.indicators) == {f"{ORDERS}#email"}
        assert index.nodes() == {ORDERS}

    def test_the_consumers_of_pii_are_listed(self):
        report = pii_exposure(tool_graph())
        assert report.consumers[ORDERS] == sorted([METRIC, VIEW])


class TestExports:
    def test_every_format_exports_the_new_kinds_and_edges(self):
        graph = tool_graph()
        view = graph_view(graph, columns=True, relations=True)
        kinds = {e.kind for e in view.edges}
        assert {"consumes", "loads", "part_of", "task_depends_on"} <= kinds
        column_edges = {(e.upstream, e.downstream) for e in view.edges if e.kind == "consumes"}
        assert (f"{VIEW}#customer_email", f"{ORDERS}#email") in column_edges
        assert {n.kind for n in view.nodes} >= {"consumer", "task", "pipeline"}
        for fmt in ExportFormat:
            assert export_graph(view, fmt)

    def test_cypher_names_them(self):
        text = export_graph(graph_view(tool_graph(), relations=True), ExportFormat.CYPHER)
        for name in (":Consumer", ":Task", ":Pipeline", ":LOADS", ":CONSUMES", ":PART_OF"):
            assert name in text
