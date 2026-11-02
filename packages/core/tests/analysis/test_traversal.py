"""Traversal on hand-built graphs (edge cases) and on jaffle-shop (real lineage)."""

from __future__ import annotations

import pytest
from support import project_config

from lineagepilot_core.analysis import Direction, Granularity, steps, traverse
from lineagepilot_core.graph import (
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    build_dbt_graph,
)
from lineagepilot_core.parsers import DbtParser

DOWN, UP = Direction.DOWNSTREAM, Direction.UPSTREAM
DECLARED = Evidence(Origin.MANIFEST, Confidence.DECLARED)
INFERRED = Evidence(Origin.COMPILED_SQL, Confidence.INFERRED)
GUESSED = Evidence(Origin.DAG_STATIC, Confidence.HEURISTIC)


def chain(*ids: str, kind: NodeKind = NodeKind.MODEL) -> MetadataGraph:
    """a -> b -> c ... over depends_on."""
    graph = MetadataGraph()
    for node_id in ids:
        graph.add_node(Node(node_id, kind, node_id))
    for up, down in zip(ids, ids[1:], strict=False):
        graph.add_edge(EdgeKind.DEPENDS_ON, up, down, DECLARED)
    return graph


def test_downstream_and_upstream_with_depths():
    graph = chain("a", "b", "c", "d")
    down = traverse(graph, ["b"], DOWN)
    assert [(r.ref, r.depth) for r in down.reached.values()] == [("b", 0), ("c", 1), ("d", 2)]
    assert down.found() == ["c", "d"]
    assert traverse(graph, ["c"], UP).found() == ["b", "a"]
    assert down.at_depth(2) == ["d"] and not down.truncated


def test_depth_cap_marks_truncation():
    graph = chain("a", "b", "c", "d")
    capped = traverse(graph, ["a"], DOWN, max_depth=2)
    assert capped.found() == ["b", "c"] and capped.truncated
    exact = traverse(graph, ["a"], DOWN, max_depth=3)
    assert exact.found() == ["b", "c", "d"] and not exact.truncated


def test_node_cap_marks_truncation():
    graph = chain("a", "b", "c", "d")
    capped = traverse(graph, ["a"], DOWN, max_nodes=2)
    assert capped.found() == ["b"] and capped.truncated


def test_cycles_terminate_and_record_every_way_in():
    graph = chain("a", "b", "c")
    graph.add_edge(EdgeKind.DEPENDS_ON, "c", "a", DECLARED)  # only a crafted manifest does this
    graph.add_edge(EdgeKind.DEPENDS_ON, "a", "c", DECLARED)
    result = traverse(graph, ["a"], DOWN)
    assert result.found() == ["b", "c"]
    assert sorted(origin for origin, _ in result.reached["c"].via) == ["a", "b"]
    assert result.reached["a"].via == []  # starts record no way in


def test_diamond_keeps_the_shortest_depth():
    graph = chain("a", "b", "c", "d")
    graph.add_edge(EdgeKind.DEPENDS_ON, "a", "d", DECLARED)
    assert traverse(graph, ["a"], DOWN).reached["d"].depth == 1


def test_several_starts():
    graph = chain("a", "b", "c")
    graph.add_node(Node("x", NodeKind.MODEL, "x"))
    graph.add_edge(EdgeKind.DEPENDS_ON, "x", "c", DECLARED)
    result = traverse(graph, ["a", "x", "a"], DOWN)
    assert result.start == ("a", "x")
    assert result.reached["c"].depth == 1


def test_unknown_start():
    with pytest.raises(KeyError, match="unknown model: nope"):
        traverse(chain("a"), ["nope"], DOWN)
    with pytest.raises(KeyError, match="unknown column: a#x"):
        traverse(chain("a"), ["a#x"], DOWN, Granularity.COLUMN)


@pytest.fixture
def junction() -> MetadataGraph:
    """seed -> table <- source -> model; and the model's columns."""
    graph = MetadataGraph()
    graph.add_node(Node("relation:db.raw.t", NodeKind.RELATION, "t"))
    graph.add_node(Node("seed.p.t", NodeKind.SEED, "t", relation_id="relation:db.raw.t"))
    graph.add_node(Node("source.p.raw.t", NodeKind.SOURCE, "t", relation_id="relation:db.raw.t"))
    graph.add_node(Node("model.p.stg", NodeKind.MODEL, "stg"))
    for node_id in ("seed.p.t", "source.p.raw.t"):
        graph.add_edge(EdgeKind.MATERIALISES, node_id, "relation:db.raw.t", DECLARED)
    graph.add_edge(EdgeKind.DEPENDS_ON, "source.p.raw.t", "model.p.stg", DECLARED)
    for node_id, column in (("seed.p.t", "id"), ("source.p.raw.t", "id"), ("model.p.stg", "id")):
        graph.add_column(Column(f"{node_id}#{column}", node_id, column))
    graph.add_column(Column("seed.p.t#only_in_seed", "seed.p.t", "only_in_seed"))
    graph.add_edge(EdgeKind.DERIVES, "source.p.raw.t#id", "model.p.stg#id", INFERRED)
    return graph


def test_a_source_is_downstream_of_the_seed_that_builds_its_table(junction):
    result = traverse(junction, ["seed.p.t"], DOWN)
    assert result.found() == ["source.p.raw.t", "model.p.stg"]
    ((origin, step),) = result.reached["source.p.raw.t"].via
    assert [e.kind for e in step.edges] == [EdgeKind.MATERIALISES, EdgeKind.MATERIALISES]
    assert traverse(junction, ["model.p.stg"], UP).found() == ["source.p.raw.t", "seed.p.t"]
    # Relations are never reported, and the junction does not run backwards
    assert traverse(junction, ["source.p.raw.t"], DOWN).found() == ["model.p.stg"]
    assert traverse(junction, ["seed.p.t"], UP).found() == []


def test_column_junction_matches_columns_by_name(junction):
    result = traverse(junction, ["seed.p.t#id"], DOWN, Granularity.COLUMN)
    assert result.found() == ["source.p.raw.t#id", "model.p.stg#id"]
    assert traverse(junction, ["model.p.stg#id"], UP, Granularity.COLUMN).found() == [
        "source.p.raw.t#id",
        "seed.p.t#id",
    ]
    assert traverse(junction, ["seed.p.t#only_in_seed"], DOWN, Granularity.COLUMN).found() == []


def test_two_builders_of_one_table_are_not_each_others_lineage(junction):
    junction.add_node(Node("model.p.writer", NodeKind.MODEL, "w", relation_id="relation:db.raw.t"))
    junction.add_edge(EdgeKind.MATERIALISES, "model.p.writer", "relation:db.raw.t", DECLARED)
    assert traverse(junction, ["seed.p.t"], DOWN).found() == ["source.p.raw.t", "model.p.stg"]
    assert set(traverse(junction, ["source.p.raw.t"], UP).found()) == {"seed.p.t", "model.p.writer"}


def test_airflow_tasks_that_run_a_node_are_downstream_of_it():
    graph = chain("model.p.a", "model.p.b")
    graph.add_node(Node("airflow:dag.run_b", NodeKind.AIRFLOW_TASK, "run_b"))
    graph.add_edge(EdgeKind.ORCHESTRATES, "airflow:dag.run_b", "model.p.b", GUESSED)
    assert traverse(graph, ["model.p.a"], DOWN).found() == ["model.p.b", "airflow:dag.run_b"]
    assert traverse(graph, ["model.p.b"], UP).found() == ["model.p.a"]


def test_steps_are_exposed_for_callers(junction):
    assert [s.ref for s in steps(junction, "source.p.raw.t", DOWN, Granularity.MODEL)] == [
        "model.p.stg"
    ]
    assert list(steps(junction, "relation:db.raw.t", DOWN, Granularity.MODEL)) == []


def test_jaffle_shop_lineage():
    graph = build_dbt_graph(DbtParser().parse(project_config("jaffle-1.12")))
    down = traverse(graph, ["seed.jaffle_shop.raw_customers"], DOWN)
    assert [(r.ref, r.depth) for r in down.reached.values()] == [
        ("seed.jaffle_shop.raw_customers", 0),
        ("source.jaffle_shop.ecom.raw_customers", 1),
        ("model.jaffle_shop.stg_customers", 2),
        ("model.jaffle_shop.customers", 3),
    ]
    up = traverse(graph, ["model.jaffle_shop.customers"], UP)
    assert len(up.found()) == 17
    assert {graph.node(r).kind for r in up.found()} == {
        NodeKind.MODEL,
        NodeKind.SOURCE,
        NodeKind.SEED,
    }
