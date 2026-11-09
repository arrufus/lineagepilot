"""MetadataGraph operations."""

from __future__ import annotations

import pytest

from lineagepilot_core.graph import (
    ArtifactVersion,
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    GraphError,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
)

MANIFEST = Evidence(Origin.MANIFEST, Confidence.DECLARED, ArtifactVersion("manifest.json", "abc"))
PARSED = Evidence(Origin.COMPILED_SQL, Confidence.INFERRED, location=Location("models/b.sql", 3, 4))
GUESSED = Evidence(Origin.DAG_STATIC, Confidence.HEURISTIC)


@pytest.fixture
def graph() -> MetadataGraph:
    g = MetadataGraph(adapter_type="duckdb")
    g.add_node(Node("model.p.a", NodeKind.MODEL, "a", file_path="models/a.sql"))
    g.add_node(
        Node("model.p.b", NodeKind.MODEL, "b", file_path="models/b.sql", patch_path="models/s.yml")
    )
    g.add_node(Node("model.p.c", NodeKind.MODEL, "c", file_path="models\\c.sql", test_count=2))
    g.add_column(Column("model.p.a#id", "model.p.a", "id"))
    g.add_column(Column("model.p.b#id", "model.p.b", "id"))
    g.add_edge(EdgeKind.DEPENDS_ON, "model.p.a", "model.p.b", MANIFEST, transformation="ref")
    g.add_edge(EdgeKind.DEPENDS_ON, "model.p.b", "model.p.c", MANIFEST)
    g.add_edge(EdgeKind.DERIVES, "model.p.a#id", "model.p.b#id", PARSED)
    return g


def test_duplicate_nodes_are_refused(graph):
    with pytest.raises(GraphError, match="duplicate node"):
        graph.add_node(Node("model.p.a", NodeKind.MODEL, "a"))


def test_relations_are_shared():
    g = MetadataGraph()
    first = g.add_node(Node("relation:db.s.t", NodeKind.RELATION, "t"))
    again = g.add_node(Node("relation:db.s.t", NodeKind.RELATION, "t", schema="other"))
    assert again is first and len(g.nodes) == 1


def test_columns_need_their_node_and_a_unique_id(graph):
    with pytest.raises(GraphError, match="unknown node"):
        graph.add_column(Column("model.p.x#id", "model.p.x", "id"))
    with pytest.raises(GraphError, match="duplicate column"):
        graph.add_column(Column("model.p.a#id", "model.p.a", "ID"))
    assert [c.id for c in graph.columns_of("model.p.a")] == ["model.p.a#id"]
    assert graph.columns_of("model.p.c") == []


def test_edges_need_both_ends(graph):
    with pytest.raises(GraphError, match="model.p.ghost is unknown"):
        graph.add_edge(EdgeKind.DEPENDS_ON, "model.p.ghost", "model.p.a", MANIFEST)
    with pytest.raises(GraphError, match="model.p.a#nope is unknown"):
        graph.add_edge(EdgeKind.DERIVES, "model.p.b#id", "model.p.a#nope", PARSED)


def test_the_same_edge_collects_evidence(graph):
    edge = graph.add_edge(EdgeKind.DEPENDS_ON, "model.p.a", "model.p.b", GUESSED)
    again = graph.add_edge(EdgeKind.DEPENDS_ON, "model.p.a", "model.p.b", MANIFEST)
    assert edge is again
    assert edge.evidence == [MANIFEST, GUESSED]
    assert edge.transformation == "ref"
    assert len(graph.out_edges("model.p.a")) == 1


def test_confidence_is_the_strongest_evidence():
    g = MetadataGraph()
    g.add_node(Node("a", NodeKind.MODEL, "a"))
    g.add_node(Node("b", NodeKind.MODEL, "b"))
    edge = g.add_edge(EdgeKind.DEPENDS_ON, "a", "b", GUESSED)
    assert edge.confidence is Confidence.HEURISTIC
    g.add_edge(EdgeKind.DEPENDS_ON, "a", "b", PARSED)
    assert edge.confidence is Confidence.INFERRED
    g.add_edge(EdgeKind.DEPENDS_ON, "a", "b", MANIFEST)
    assert edge.confidence is Confidence.DECLARED


def test_adjacency_by_kind(graph):
    assert [e.downstream for e in graph.out_edges("model.p.a")] == ["model.p.b"]
    assert [e.upstream for e in graph.in_edges("model.p.c")] == ["model.p.b"]
    assert graph.out_edges("model.p.a", [EdgeKind.MATERIALISES]) == []
    assert [e.downstream for e in graph.out_edges("model.p.a#id", [EdgeKind.DERIVES])] == [
        "model.p.b#id"
    ]
    assert graph.in_edges("model.p.zzz") == []
    assert graph.edge(EdgeKind.DEPENDS_ON, "model.p.b", "model.p.c") is not None
    assert graph.edge(EdgeKind.DEPENDS_ON, "model.p.c", "model.p.b") is None
    assert len(list(graph.edges)) == 3


def test_files_map_to_nodes(graph):
    assert [n.id for n in graph.nodes_for_file("models/a.sql")] == ["model.p.a"]
    assert [n.id for n in graph.nodes_for_file("./models/s.yml")] == ["model.p.b"]
    assert [n.id for n in graph.nodes_for_file("models/c.sql")] == ["model.p.c"]
    assert [n.id for n in graph.nodes_for_file("models\\c.sql")] == ["model.p.c"]
    assert graph.nodes_for_file("models/none.sql") == []


def test_logical_nodes_for_a_relation(graph):
    graph.add_node(Node("relation:db.s.a", NodeKind.RELATION, "a"))
    graph.add_node(Node("source.p.raw.a", NodeKind.SOURCE, "a"))
    graph.add_edge(EdgeKind.MATERIALISES, "model.p.a", "relation:db.s.a", MANIFEST)
    graph.add_edge(EdgeKind.MATERIALISES, "source.p.raw.a", "relation:db.s.a", MANIFEST)
    assert {n.id for n in graph.logical_nodes_for("relation:db.s.a")} == {
        "model.p.a",
        "source.p.raw.a",
    }


def test_unresolved_and_lookup(graph):
    record = Unresolved("model.p.c", "model.p.ghost", "not found", MANIFEST)
    graph.add_unresolved(record)
    assert graph.unresolved_for("model.p.c") == [record]
    assert graph.unresolved_for("model.p.a") == []
    assert graph.node("model.p.a").name == "a"
    with pytest.raises(KeyError, match="unknown node: nope"):
        graph.node("nope")


def test_has_tests(graph):
    assert graph.node("model.p.c").has_tests
    assert not graph.node("model.p.a").has_tests
    assert not graph.columns["model.p.a#id"].has_tests


def test_stats(graph):
    assert graph.stats() == {
        "nodes": 3,
        "columns": 2,
        "edges": 3,
        "unresolved": 0,
        "node_kinds": {"model": 3},
        "edge_kinds": {"depends_on": 2, "derives": 1},
    }


def test_remove_edge(graph):
    edge = graph.edge(EdgeKind.DERIVES, "model.p.a#id", "model.p.b#id")
    graph.remove_edge(edge)
    assert graph.edge(EdgeKind.DERIVES, "model.p.a#id", "model.p.b#id") is None
    assert graph.out_edges("model.p.a#id") == [] and graph.in_edges("model.p.b#id") == []
    with pytest.raises(GraphError, match="no such edge"):
        graph.remove_edge(edge)


def test_remove_unresolved_by_subject_and_origin(graph):
    from_sql = Unresolved("model.p.b", "x", "r", PARSED)
    from_manifest = Unresolved("model.p.b", "y", "r", MANIFEST)
    elsewhere = Unresolved("model.p.c", "z", "r", PARSED)
    for record in (from_sql, from_manifest, elsewhere):
        graph.add_unresolved(record)
    graph.remove_unresolved(["model.p.b"], origin=Origin.COMPILED_SQL)
    assert graph.unresolved == [from_manifest, elsewhere]
    graph.remove_unresolved(["model.p.b", "model.p.c"])
    assert graph.unresolved == []


def test_many_pieces_of_evidence_on_one_edge_stay_cheap():
    """Audit LP-SEC-05: a list search per piece made a DAG scan's links quadratic."""
    import time

    from lineagepilot_core.graph.model import Confidence, Evidence, Location, Origin

    graph = MetadataGraph()
    for name in ("a", "b"):
        graph.add_node(Node(name, NodeKind.MODEL, name))

    def piece(i: int) -> Evidence:
        return Evidence(Origin.DAG_STATIC, Confidence.INFERRED, location=Location(f"d{i}.py", i))

    started = time.perf_counter()
    for _ in range(2):  # the second round adds nothing
        for i in range(20_000):
            graph.add_edge(EdgeKind.DEPENDS_ON, "a", "b", piece(i))
    assert time.perf_counter() - started < 2
    edge = graph.edge(EdgeKind.DEPENDS_ON, "a", "b")
    assert len(edge.evidence) == 20_000
    # Evidence appended directly, as a snapshot loads it, is still seen
    edge.evidence.append(piece(-1))
    graph.add_edge(EdgeKind.DEPENDS_ON, "a", "b", piece(-1))
    assert len(edge.evidence) == 20_001
    graph.remove_edge(edge)
    graph.add_edge(EdgeKind.DEPENDS_ON, "a", "b", piece(1))
    assert len(graph.edge(EdgeKind.DEPENDS_ON, "a", "b").evidence) == 1
