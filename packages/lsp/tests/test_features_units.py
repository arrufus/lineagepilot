"""Task 2.2 units: lens titles, ref resolution, Markdown escaping and diagnostic mapping."""

from __future__ import annotations

from pathlib import Path

import pytest
from lineagepilot_core.graph.model import (
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
)
from lineagepilot_core.rules.model import (
    Category,
    Evaluation,
    Finding,
    Result,
    Rule,
    Scope,
    Severity,
)

from lineagepilot_lsp import features
from lineagepilot_lsp.context import GraphContext
from lineagepilot_lsp.project import read_project

DECLARED = Evidence(Origin.MANIFEST, Confidence.DECLARED)


def test_lens_titles():
    assert features.lens_title({}, False) == "↓ nothing downstream"
    assert features.lens_title({"model": 1}, True) == "↓ 1 model · PII"
    counts = {"model": 14, "airflow_task": 3, "exposure": 1, "airflow_dag": 1, "seed": 2}
    assert features.lens_title(counts, False) == (
        "↓ 14 models · 2 seeds · 1 exposure · 3 Airflow tasks · 1 Airflow DAG"
    )


def test_markdown_escaping():
    assert features.md("a_b*c") == "a\\_b\\*c"
    assert features.md("line\nbreak  and   spaces") == "line break and spaces"
    assert features.md(None) == ""
    assert features.md("<script>&") == "\\<script\\>\\&"


@pytest.fixture
def project_graph(tmp_path) -> tuple[MetadataGraph, GraphContext]:
    (tmp_path / "dbt_project.yml").write_text("name: shop\n")
    graph = MetadataGraph()
    graph.project_name = "shop"
    nodes = [
        Node("model.shop.orders.v1", NodeKind.MODEL, "orders", package="shop", version="1"),
        Node("model.shop.orders.v2", NodeKind.MODEL, "orders", package="shop", version="2"),
        Node("model.other.orders", NodeKind.MODEL, "orders", package="other"),
        Node("model.other.lonely", NodeKind.MODEL, "lonely", package="other"),
        Node("source.other.raw.t", NodeKind.SOURCE, "t", package="other"),
        Node("source.shop.raw.t", NodeKind.SOURCE, "t", package="shop"),
        Node("model.shop.hub", NodeKind.MODEL, "hub", package="shop", file_path="models/hub.sql"),
    ]
    for node in nodes:
        graph.add_node(node)
    for i in range(12):
        leaf = graph.add_node(
            Node(f"model.shop.leaf{i}", NodeKind.MODEL, f"leaf{i}", package="shop")
        )
        graph.add_edge(EdgeKind.DEPENDS_ON, "model.shop.hub", leaf.id, DECLARED, "ref")
        graph.add_column(Column(f"{leaf.id}#x", leaf.id, "x"))
    graph.add_column(Column("model.shop.hub#x", "model.shop.hub", "X", data_type="int"))
    for i in range(12):
        graph.add_edge(
            EdgeKind.DERIVES, "model.shop.hub#x", f"model.shop.leaf{i}#x", DECLARED, "passthrough"
        )
    return graph, GraphContext(graph, read_project(tmp_path))


def test_refs_resolve_like_dbt(project_graph):
    _, ctx = project_graph
    assert features.resolve_ref(ctx, "orders", None, None).id == "model.shop.orders.v2"
    assert features.resolve_ref(ctx, "orders", None, "1").id == "model.shop.orders.v1"
    assert features.resolve_ref(ctx, "other", "orders", None).id == "model.other.orders"
    assert features.resolve_ref(ctx, "lonely", None, None).id == "model.other.lonely"
    assert features.resolve_ref(ctx, "missing", None, None) is None
    assert features.resolve_source(ctx, "raw", "t").id == "source.shop.raw.t"
    assert features.resolve_source(ctx, "raw", "nope") is None


def test_long_neighbour_lists_are_cut(project_graph, tmp_path):
    graph, ctx = project_graph
    text = features.node_markdown(ctx, graph.nodes["model.shop.hub"], [])
    assert "**Downstream** (12 direct)" in text and "- and 2 more" in text
    column = features.column_markdown(ctx, "model.shop.hub#x")
    assert "column · int" in column and "**Feeds** (12)" in column and "- and 2 more" in column
    assert "Column lineage: not computed yet" in column


def test_column_hover_computes_on_demand_while_columns_are_computing(project_graph):
    graph, ctx = project_graph
    asked = []
    ctx._columns_computing = lambda: True
    ctx.on_demand.status = lambda node_id: asked.append(node_id)
    features.column_markdown(ctx, "model.shop.hub#x")
    assert asked[0] == "model.shop.hub" and len(asked) == 13


def test_hover_on_a_word_that_is_no_column(project_graph, tmp_path):
    _, ctx = project_graph
    sql = tmp_path / "models" / "hub.sql"
    sql.parent.mkdir()
    sql.write_text("select x, y from t")
    assert features.hover(ctx, sql, sql.read_text(), 0, 10) is None  # y
    found = features.hover(ctx, sql, sql.read_text(), 0, 7)  # x
    assert found["contents"]["value"].startswith("### hub.X")
    assert features.hover(ctx, Path(tmp_path / "x.csv"), "", 0, 0) is None


def finding(rule: Rule, subject: str, result: Result, path: str = "models/hub.sql") -> Finding:
    return Finding(
        rule.id,
        subject,
        result,
        rule.severity,
        "message",
        Evidence(Origin.MANIFEST, Confidence.DECLARED, location=Location(path)),
        "do this",
    )


def test_diagnostic_severities_and_skipped_findings(project_graph, tmp_path):
    graph, ctx = project_graph
    (tmp_path / "models").mkdir(exist_ok=True)
    (tmp_path / "models" / "hub.sql").write_text("select 1")
    rule = Rule(
        "X_001",
        "x",
        "x",
        Severity.INFO,
        Category.CUSTOM,
        Scope.NODE,
        lambda *a: None,
        source=".lineagepilot/rules.yml",
    )
    evaluation = Evaluation(rules={rule.id: rule})
    evaluation.findings = [
        finding(rule, "model.shop.hub", Result.FAIL),
        finding(rule, "model.shop.hub", Result.UNKNOWN),
        finding(rule, "model.shop.hub", Result.UNSUPPORTED),
        finding(rule, "model.shop.gone", Result.FAIL),  # not in the graph
        finding(rule, "model.shop.leaf0", Result.FAIL, "models/leaf0.sql"),  # no such file
    ]
    found = features.diagnostics(ctx, evaluation, None, None)
    [diagnostics] = found.values()
    assert sorted((d["severity"], d["message"]) for d in diagnostics) == [
        (features.INFORMATION, "Not checked: message"),
        (features.INFORMATION, "message\ndo this"),  # an info-severity failure
        (features.HINT, "Cannot be checked: message"),
    ]
    assert all("codeDescription" not in d for d in diagnostics)  # custom rules
