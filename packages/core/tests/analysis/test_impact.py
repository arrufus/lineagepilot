"""The impact query: column-precise where lineage is known, model level where it is not."""

from __future__ import annotations

import pytest
from support import project_config

from lineagepilot_core.analysis import Granularity, impact
from lineagepilot_core.graph import (
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
    build_dbt_graph,
)
from lineagepilot_core.graph.column_lineage import add_column_lineage, compiled_sql
from lineagepilot_core.graph.model import ColumnLineageStatus, LineageState
from lineagepilot_core.parsers import DbtParser

DECLARED = Evidence(Origin.MANIFEST, Confidence.DECLARED)
INFERRED = Evidence(Origin.COMPILED_SQL, Confidence.INFERRED)
GUESSED = Evidence(Origin.DAG_STATIC, Confidence.HEURISTIC)
RESOLVED = ColumnLineageStatus(LineageState.RESOLVED)
JAFFLE = "model.jaffle_shop"


@pytest.fixture
def graph() -> MetadataGraph:
    """a -> b -> c, a -> d, b -> exposure, a task runs c; columns a#x, a#y, b#x, c#x, d#z."""
    g = MetadataGraph()
    for node_id, kind in (
        ("a", NodeKind.MODEL),
        ("b", NodeKind.MODEL),
        ("c", NodeKind.MODEL),
        ("d", NodeKind.MODEL),
        ("e", NodeKind.EXPOSURE),
        ("airflow:dag.t", NodeKind.AIRFLOW_TASK),
    ):
        g.add_node(Node(node_id, kind, node_id))
    for up, down in (("a", "b"), ("b", "c"), ("a", "d"), ("b", "e")):
        g.add_edge(EdgeKind.DEPENDS_ON, up, down, DECLARED)
    g.add_edge(EdgeKind.ORCHESTRATES, "airflow:dag.t", "c", GUESSED)
    for column in ("a#x", "a#y", "b#x", "c#x", "d#z"):
        node_id, name = column.split("#")
        g.add_column(Column(column, node_id, name))
    g.add_edge(EdgeKind.DERIVES, "a#x", "b#x", INFERRED, transformation="passthrough")
    g.add_edge(EdgeKind.DERIVES, "b#x", "c#x", INFERRED, transformation="aggregate")
    g.add_edge(EdgeKind.DERIVES, "a#y", "d#z", INFERRED)
    return g


def summary(result):
    return {
        n.id: (n.depth, n.granularity.value, n.confidence.value, n.columns, n.reason)
        for n in result.nodes.values()
    }


def test_node_targets_are_model_level_and_declared(graph):
    result = impact(graph, ["a"])
    assert summary(result) == {
        "b": (1, "model", "declared", [], None),
        "d": (1, "model", "declared", [], None),
        "c": (2, "model", "declared", [], None),
        "e": (2, "model", "declared", [], None),
        "airflow:dag.t": (3, "model", "heuristic", [], None),
    }
    assert result.counts() == {"airflow_task": 1, "exposure": 1, "model": 3}


def test_column_targets_follow_derives_where_lineage_is_known(graph):
    for node_id in ("b", "c", "d"):
        graph.column_lineage[node_id] = RESOLVED
    result = impact(graph, ["a#x"])
    assert summary(result) == {
        "b": (1, "column", "inferred", ["b#x"], None),
        "c": (2, "column", "inferred", ["c#x"], None),
        "e": (2, "model", "inferred", [], "uses the whole node"),
        "airflow:dag.t": (3, "model", "heuristic", [], "uses the whole node"),
    }
    # d does not use a#x, and its lineage is known, so it is not impacted
    assert "d" not in impact(graph, ["a#x"]).nodes
    assert summary(impact(graph, ["a#y"])) == {"d": (1, "column", "inferred", ["d#z"], None)}


def test_unknown_or_missing_lineage_falls_back_to_model_level(graph):
    graph.column_lineage["b"] = RESOLVED
    graph.column_lineage["d"] = ColumnLineageStatus(LineageState.UNKNOWN, "no catalog.json")
    # c has no status: not computed yet
    result = impact(graph, ["a#x"])
    assert summary(result)["d"] == (
        1,
        "model",
        "inferred",
        [],
        "column lineage unknown: no catalog.json",
    )
    # c is reached through b#x, and also at model level, because its lineage was never
    # computed: the model-level fallback wins, and the column it reached is still listed
    assert summary(result)["c"] == (2, "model", "inferred", ["c#x"], "column lineage not computed")


def test_a_partial_status_still_counts_as_known(graph):
    for node_id in ("b", "c", "d"):
        graph.column_lineage[node_id] = ColumnLineageStatus(
            LineageState.PARTIAL, "1 did not resolve"
        )
    assert "d" not in impact(graph, ["a#x"]).nodes


def test_column_lineage_callback_can_compute_on_demand(graph):
    asked = []

    def status(node_id: str):
        asked.append(node_id)
        return RESOLVED

    impact(graph, ["a#x"], column_lineage=status)
    assert sorted(set(asked)) == ["b", "c", "d"]


def test_confidence_takes_the_strongest_path(graph):
    graph.add_node(Node("f", NodeKind.MODEL, "f"))
    graph.add_edge(EdgeKind.DEPENDS_ON, "airflow:dag.t", "f", GUESSED)  # a weak path
    graph.add_edge(EdgeKind.DEPENDS_ON, "a", "f", DECLARED)  # and a strong one
    assert impact(graph, ["a"]).nodes["f"].confidence is Confidence.DECLARED


def test_max_depth(graph):
    result = impact(graph, ["a"], max_depth=1)
    assert set(result.nodes) == {"b", "d"} and result.truncated
    assert not impact(graph, ["a"], max_depth=3).truncated


def test_unknowns_cover_targets_impacted_nodes_and_their_columns(graph):
    for subject in ("a", "b#x", "d", "zzz"):
        graph.add_unresolved(Unresolved(subject, "ref", "reason", INFERRED))
    graph.add_column(Column("b#other", "b", "other"))
    graph.add_unresolved(Unresolved("b#other", "ref", "reason", INFERRED))
    result = impact(graph, ["a"])
    assert [u.subject for u in result.unknowns] == ["a", "b#other", "b#x", "d"]


def test_unknown_target(graph):
    with pytest.raises(KeyError, match="unknown node or column: nope"):
        impact(graph, ["nope"])


def test_other_columns_of_a_changed_node_are_not_impacted(graph):
    graph.add_edge(EdgeKind.DERIVES, "a#x", "a#y", INFERRED)  # not produced by real lineage
    graph.column_lineage.update(b=RESOLVED, c=RESOLVED, d=RESOLVED)
    result = impact(graph, ["a#x"])
    assert "a" not in result.nodes
    assert "d" in result.nodes  # reached through a#y, which a#x feeds


def test_sources_reading_a_changed_table(graph):
    graph.add_node(Node("relation:db.s.a", NodeKind.RELATION, "a"))
    graph.nodes["a"].relation_id = "relation:db.s.a"
    graph.add_node(Node("source.p.s.a", NodeKind.SOURCE, "a", relation_id="relation:db.s.a"))
    for node_id in ("a", "source.p.s.a"):
        graph.add_edge(EdgeKind.MATERIALISES, node_id, "relation:db.s.a", DECLARED)
    graph.add_column(Column("source.p.s.a#x", "source.p.s.a", "x"))
    graph.column_lineage.update(b=RESOLVED, c=RESOLVED, d=RESOLVED)
    assert impact(graph, ["a#x"]).nodes["source.p.s.a"].columns == ["source.p.s.a#x"]
    fallback = impact(graph, ["a#y"]).nodes["source.p.s.a"]
    assert (fallback.granularity, fallback.reason) == (
        Granularity.MODEL,
        "the source does not list this column",
    )


@pytest.fixture(scope="module")
def jaffle():
    result = DbtParser().parse(project_config("jaffle-1.12"))
    graph = build_dbt_graph(result)
    add_column_lineage(graph, compiled_sql(result))
    return graph


def test_jaffle_column_impact_is_precise(jaffle):
    """order_items does not use order_total, so it is not impacted (checked by hand)."""
    result = impact(jaffle, [f"{JAFFLE}.stg_orders#order_total"])
    assert {n.id: n.columns for n in result.nodes.values()} == {
        f"{JAFFLE}.orders": [f"{JAFFLE}.orders#order_total"],
        f"{JAFFLE}.customers": [f"{JAFFLE}.customers#lifetime_spend"],
    }
    assert result.unknowns == []


def test_jaffle_lineage_computed_on_demand_is_followed(jaffle):
    """The language server computes models on demand while the indexer is still busy:
    the derives edges a callback adds must be followed in the same query."""
    result = DbtParser().parse(project_config("jaffle-1.12"))
    graph = build_dbt_graph(result)
    sql = compiled_sql(result)

    def on_demand(node_id: str):
        if node_id not in graph.column_lineage:
            add_column_lineage(graph, sql, [node_id])
        return graph.column_lineage[node_id]

    target = f"{JAFFLE}.stg_orders#order_total"
    found = impact(graph, [target], column_lineage=on_demand)
    expected = impact(jaffle, [target])
    assert {n.id: (n.columns, n.granularity) for n in found.nodes.values()} == {
        n.id: (n.columns, n.granularity) for n in expected.nodes.values()
    }


def test_jaffle_model_impact(jaffle):
    result = impact(jaffle, [f"{JAFFLE}.stg_orders"])
    assert set(result.nodes) == {f"{JAFFLE}.orders", f"{JAFFLE}.order_items", f"{JAFFLE}.customers"}
    assert {n.confidence for n in result.nodes.values()} == {Confidence.DECLARED}


def test_jaffle_seed_change_reaches_marts_through_the_source(jaffle):
    result = impact(jaffle, ["seed.jaffle_shop.raw_orders"])
    assert result.nodes["source.jaffle_shop.ecom.raw_orders"].depth == 1
    assert f"{JAFFLE}.customers" in result.nodes
