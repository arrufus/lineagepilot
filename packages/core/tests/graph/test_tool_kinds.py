"""D-9: pipeline, task and consumer nodes beside the dbt graph (task 7.1).

Readers of other tools add them, each with its tool, and three edges: ``consumes``, ``part_of``
and ``loads``. Rules and PII indicators stay with the dbt nodes. Airflow keeps its public kinds,
``airflow_dag`` and ``airflow_task``, until the protocol gets the generic ones (task 7.5).
"""

from __future__ import annotations

import json

import pytest
from support import project_config

from lineagepilot_core.analysis import Direction, Granularity, traverse
from lineagepilot_core.analysis.pii import pii_exposure, pii_index
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
    build_dbt_graph,
    public_kind,
)
from lineagepilot_core.parsers import DbtParser
from lineagepilot_core.rules import builtin_rules, evaluate, load_rule_pack
from lineagepilot_core.snapshot import from_dict, to_dict

DOWN, UP = Direction.DOWNSTREAM, Direction.UPSTREAM
DECLARED = Evidence(Origin.MANIFEST, Confidence.DECLARED)
ANNOTATED = Evidence(Origin.ANNOTATION, Confidence.DECLARED)

VIEW, EXPLORE, DASHBOARD = "looker:view.orders", "looker:shop.orders", "looker:dashboard.sales"


def consumer(node_id: str, subkind: str, tool: str = "looker") -> Node:
    return Node(node_id, NodeKind.CONSUMER, node_id.rpartition(".")[2], tool=tool, subkind=subkind)


@pytest.fixture
def consumers() -> MetadataGraph:
    """A mart, a view that reads it, in an explore, on a dashboard; and a column of each."""
    graph = MetadataGraph()
    graph.add_node(Node("model.p.mart", NodeKind.MODEL, "mart", layer="marts"))
    for node_id, subkind in ((VIEW, "view"), (EXPLORE, "explore"), (DASHBOARD, "dashboard")):
        graph.add_node(consumer(node_id, subkind))
    graph.add_edge(EdgeKind.CONSUMES, "model.p.mart", VIEW, ANNOTATED)
    graph.add_edge(EdgeKind.PART_OF, VIEW, EXPLORE, ANNOTATED)
    graph.add_edge(EdgeKind.PART_OF, EXPLORE, DASHBOARD, ANNOTATED)
    graph.add_column(Column("model.p.mart#customer", "model.p.mart", "customer"))
    graph.add_column(Column(f"{VIEW}#customer_name", VIEW, "customer_name"))
    graph.add_edge(EdgeKind.CONSUMES, "model.p.mart#customer", f"{VIEW}#customer_name", ANNOTATED)
    return graph


def test_airflow_keeps_its_public_kinds():
    assert public_kind(NodeKind.PIPELINE, "airflow") == "airflow_dag"
    assert public_kind(NodeKind.TASK, "airflow") == "airflow_task"
    assert public_kind(NodeKind.TASK, "dagster") == "task"
    assert public_kind(NodeKind.CONSUMER, "dbt_semantic") == "consumer"
    assert public_kind(NodeKind.MODEL) == "model"
    assert TOOL_KINDS.isdisjoint(DBT_KINDS)


def test_stats_count_public_kinds():
    graph = MetadataGraph()
    graph.add_node(Node("airflow:d", NodeKind.PIPELINE, "d", tool="airflow", subkind="dag"))
    graph.add_node(Node("airflow:d.t", NodeKind.TASK, "t", tool="airflow", subkind="task"))
    graph.add_node(consumer("dbt_semantic:metric.revenue", "metric", tool="dbt_semantic"))
    assert graph.stats()["node_kinds"] == {"airflow_dag": 1, "airflow_task": 1, "consumer": 1}


# Traversal


def test_consumers_are_downstream_and_leaves(consumers):
    assert traverse(consumers, ["model.p.mart"], DOWN).found() == [VIEW, EXPLORE, DASHBOARD]
    assert traverse(consumers, [DASHBOARD], UP).found() == [EXPLORE, VIEW, "model.p.mart"]
    assert traverse(consumers, [DASHBOARD], DOWN).found() == []


def test_a_consumer_column_is_reached_at_column_level_only(consumers):
    column = Granularity.COLUMN
    assert traverse(consumers, ["model.p.mart#customer"], DOWN, column).found() == [
        f"{VIEW}#customer_name"
    ]
    assert traverse(consumers, [f"{VIEW}#customer_name"], UP, column).found() == [
        "model.p.mart#customer"
    ]
    assert f"{VIEW}#customer_name" not in traverse(consumers, ["model.p.mart"], DOWN).found()


def test_a_task_loads_a_source_directly_or_through_its_table():
    graph = MetadataGraph()
    graph.add_node(Node("relation:db.raw.orders", NodeKind.RELATION, "orders"))
    graph.add_node(
        Node("source.p.raw.orders", NodeKind.SOURCE, "orders", relation_id="relation:db.raw.orders")
    )
    graph.add_edge(EdgeKind.MATERIALISES, "source.p.raw.orders", "relation:db.raw.orders", DECLARED)
    graph.add_node(Node("model.p.stg", NodeKind.MODEL, "stg"))
    graph.add_edge(EdgeKind.DEPENDS_ON, "source.p.raw.orders", "model.p.stg", DECLARED)
    graph.add_node(Node("dlt:shop.orders", NodeKind.TASK, "orders", tool="dlt", subkind="resource"))
    graph.add_edge(EdgeKind.LOADS, "dlt:shop.orders", "relation:db.raw.orders", ANNOTATED)

    down = traverse(graph, ["dlt:shop.orders"], DOWN)
    assert down.found() == ["source.p.raw.orders", "model.p.stg"]  # never the table itself
    assert [e.kind for e in down.reached["source.p.raw.orders"].via[0][1].edges] == [
        EdgeKind.LOADS,
        EdgeKind.MATERIALISES,
    ]
    assert traverse(graph, ["model.p.stg"], UP).found() == [
        "source.p.raw.orders",
        "dlt:shop.orders",
    ]

    # A task that names the source itself
    graph.add_node(Node("meltano:tap.orders", NodeKind.TASK, "orders", tool="meltano"))
    graph.add_edge(EdgeKind.LOADS, "meltano:tap.orders", "source.p.raw.orders", ANNOTATED)
    assert traverse(graph, ["meltano:tap.orders"], DOWN).found() == [
        "source.p.raw.orders",
        "model.p.stg",
    ]
    assert set(traverse(graph, ["source.p.raw.orders"], UP).found()) == {
        "dlt:shop.orders",
        "meltano:tap.orders",
    }


# Rules and PII stay with the dbt nodes


def test_no_rule_applies_to_a_pipeline_task_or_consumer():
    graph = build_dbt_graph(DbtParser().parse(project_config("pilot-1.12")))
    mart = next(n for n in graph.nodes.values() if n.kind is NodeKind.MODEL)
    added = {
        "airflow:etl": Node("airflow:etl", NodeKind.PIPELINE, "etl", tool="airflow", subkind="dag"),
        "airflow:etl.run": Node("airflow:etl.run", NodeKind.TASK, "run", tool="airflow"),
        VIEW: consumer(VIEW, "view"),
    }
    for node in added.values():  # no owner, description or tests: every rule would fail them
        graph.add_node(node)
    graph.add_column(Column(f"{VIEW}#email", VIEW, "email"))
    graph.add_edge(EdgeKind.ORCHESTRATES, "airflow:etl.run", mart.id, ANNOTATED)
    graph.add_edge(EdgeKind.CONSUMES, mart.id, VIEW, ANNOTATED)

    assert all(rule.resource_types <= DBT_KINDS for rule in builtin_rules())
    findings = evaluate(graph).findings
    assert findings  # the rules ran
    assert not {f.subject.partition("#")[0] for f in findings} & set(added)


@pytest.mark.parametrize("kind", ["pipeline", "task", "consumer", "airflow_task", "airflow_dag"])
def test_a_rule_pack_cannot_target_them(tmp_path, kind):
    path = tmp_path / "rules.yml"
    path.write_text(
        f"rules:\n- {{id: ACME_1, name: x, resource_types: [{kind}], require: {{owner: true}}}}\n",
        encoding="utf-8",
    )
    pack = load_rule_pack(path, {rule.id for rule in builtin_rules()})
    assert pack.rules == []
    assert any("resource_types" in p for p in pack.problems), pack.problems


def test_pii_is_no_indicator_on_a_consumer_but_the_consumer_is_named(consumers):
    customer = consumers.columns["model.p.mart#customer"]
    customer.pii_type, customer.pii_detected_by = "name", "tag"
    # A consumer's own column marked as PII is not an indicator either
    consumers.columns[f"{VIEW}#customer_name"].pii_type = "name"

    index = pii_index(consumers)
    assert set(index.indicators) == {"model.p.mart#customer"}
    assert index.nodes() == {"model.p.mart"}
    assert pii_exposure(consumers, index).consumers == {"model.p.mart": [VIEW]}


def test_a_snapshot_keeps_the_tool_and_the_subkind(consumers):
    restored = from_dict(json.loads(json.dumps(to_dict(consumers))))
    assert list(restored.nodes.values()) == list(consumers.nodes.values())
    assert (restored.nodes[VIEW].tool, restored.nodes[VIEW].subkind) == ("looker", "view")
    assert list(restored.edges) == list(consumers.edges)
