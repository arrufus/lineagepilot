"""Tasks 2.2 and 2.3 units: positions in YAML, the graph context and on-demand columns."""

from __future__ import annotations

import json

import pytest
from lineagepilot_core.analysis.impact import impact
from lineagepilot_core.graph.model import LineageState, Node, NodeKind
from lineagepilot_core.indexer import index_project

from lineagepilot_lsp.context import GraphContext, OnDemandColumns, clip
from lineagepilot_lsp.locate import Locator, entry_key, yaml_entries
from lineagepilot_lsp.project import read_project

J = "model.pilot_shop"

# YAML positions


def test_yaml_entries():
    text = """\
version: 2
models:
  - name: orders
    columns:
      - name: Order_ID
      - name: [not, a, name]
      - description: no name
sources:
  - name: ecom
    tables:
      - name: raw_orders
        columns:
          - name: id
exposures:
  - name: dashboard
"""
    entries = yaml_entries(text)
    orders = entries.get("models", "orders")
    assert (orders.start_line, orders.start_character, orders.end_character) == (2, 10, 16)
    assert entries.get("models", "orders", "columns", "order_id").start_line == 4
    assert entries.get("sources", "ecom", "tables", "raw_orders").start_line == 10
    assert entries.get("sources", "ecom", "tables", "raw_orders", "columns", "id").start_line == 12
    assert entries.get("exposures", "dashboard").start_line == 14
    assert len(entries.spans) == 5


@pytest.mark.parametrize(
    "text",
    [
        "- a list",
        "models: {not: a list}",
        "models: [unclosed",
        "",
        "x" * 2_000_001,
        "seeds:\n  - just a string\n",
    ],
)
def test_yaml_without_entries(text):
    assert yaml_entries(text).spans == {}


def test_entry_keys():
    source = Node("source.pkg.ecom.raw", NodeKind.SOURCE, "raw", package="pkg")
    assert entry_key(source) == ("sources", "ecom", "tables", "raw")
    odd = Node("source.other.ecom.raw", NodeKind.SOURCE, "raw", package="pkg")
    assert entry_key(odd) is None
    assert entry_key(Node("relation:db.s.t", NodeKind.RELATION, "t")) is None
    assert entry_key(Node("seed.pkg.s", NodeKind.SEED, "s")) == ("seeds", "s")


# The graph context


@pytest.fixture
def model_graph(workspace):
    """The fixture's model-level graph, with target/compiled written as dbt compile does."""
    manifest = json.loads((workspace / "target" / "manifest.json").read_text())
    for node in manifest["nodes"].values():
        if node.get("compiled_path") and node.get("compiled_code"):
            path = workspace / node["compiled_path"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(node["compiled_code"])
    graph = index_project(workspace, column_lineage=False).graph
    return graph, read_project(workspace)


def test_on_demand_columns_match_the_indexer(model_graph, workspace):
    graph, project = model_graph
    ctx = GraphContext(graph, project, columns_computing=lambda: True)
    found = ctx.impact([f"{J}.stg_orders#order_total"])
    full = index_project(workspace).graph
    expected = impact(full, [f"{J}.stg_orders#order_total"])
    assert {n: h.columns for n, h in found.nodes.items()} == {
        n: h.columns for n, h in expected.nodes.items()
    }
    assert ctx.on_demand.computed >= 2
    assert graph.column_lineage[f"{J}.orders"].state is LineageState.RESOLVED


def test_on_demand_stops_at_its_budget(model_graph):
    graph, project = model_graph
    ctx = GraphContext(graph, project, columns_computing=lambda: True)
    ctx.on_demand.budget_s = 0
    found = ctx.impact([f"{J}.stg_orders#order_total"])
    assert ctx.on_demand.computed == 0
    orders = found.nodes[f"{J}.orders"]
    assert orders.granularity.value == "model" and orders.confidence.value == "inferred"


def test_on_demand_needs_a_compiled_file_inside_the_project(model_graph, workspace):
    graph, project = model_graph
    on_demand = OnDemandColumns(graph, project)
    on_demand.start_request()
    graph.nodes[f"{J}.orders"].compiled_path = "../../../etc/passwd"
    assert on_demand.status(f"{J}.orders") is None
    (workspace / graph.nodes[f"{J}.customers"].compiled_path).unlink()
    assert on_demand.status(f"{J}.customers") is None
    graph.nodes[f"{J}.locations"].compiled_path = None
    assert on_demand.status(f"{J}.locations") is None
    assert on_demand.status("seed.pilot_shop.raw_orders") is None  # no SQL
    assert on_demand.computed == 0
    # Remembered: the next request does not try again
    assert f"{J}.orders" in on_demand._unavailable


def test_a_slow_trace_costs_a_request_its_budget_and_no_more(model_graph, monkeypatch):
    """Audit LP-SEC-05: sqlglot runs off the event loop, one model at a time."""
    import threading
    import time

    from lineagepilot_lsp import context

    graph, project = model_graph
    release = threading.Event()
    real_trace = context.trace

    def slow_trace(*args):
        release.wait(10)
        return real_trace(*args)

    monkeypatch.setattr(context, "trace", slow_trace)
    on_demand = OnDemandColumns(graph, project, budget_s=0.1)
    on_demand.start_request()
    started = time.perf_counter()
    assert on_demand.status(f"{J}.orders") is None
    assert time.perf_counter() - started < 1
    # Still tracing orders: nothing else is started meanwhile
    on_demand.start_request()
    assert on_demand.status(f"{J}.customers") is None
    release.set()
    for _ in range(100):
        if f"{J}.orders" in on_demand._late:
            break
        time.sleep(0.05)
    # The next request that asks gets the late result, with no budget left
    on_demand._deadline = 0.0
    assert on_demand.status(f"{J}.orders").state is LineageState.RESOLVED
    assert on_demand.computed == 1


def test_a_trace_that_fails_is_not_tried_again(model_graph, monkeypatch):
    from lineagepilot_lsp import context

    graph, project = model_graph

    def broken(*args):
        raise RecursionError

    monkeypatch.setattr(context, "trace", broken)
    on_demand = OnDemandColumns(graph, project)
    on_demand.start_request()
    assert on_demand.status(f"{J}.orders") is None
    assert f"{J}.orders" in on_demand._unavailable


def test_without_computing_columns_impact_uses_the_graph(model_graph):
    graph, project = model_graph
    ctx = GraphContext(graph, project)
    found = ctx.impact([f"{J}.stg_orders#order_total"])
    assert ctx.on_demand.computed == 0
    assert found.nodes[f"{J}.orders"].granularity.value == "model"


def test_downstream_counts_are_cached(model_graph):
    graph, project = model_graph
    ctx = GraphContext(graph, project)
    first = ctx.downstream_counts(f"{J}.stg_orders")
    assert first == {"model": 3}  # orders, order_items, customers
    assert ctx.downstream_counts(f"{J}.stg_orders") is first


def test_package_files(model_graph, workspace):
    graph, project = model_graph
    node = graph.add_node(
        Node(
            "model.dbt_utils.helper",
            NodeKind.MODEL,
            "helper",
            package="dbt_utils",
            file_path="models/helper.sql",
        )
    )
    path = workspace / "dbt_packages" / "dbt_utils" / "models" / "helper.sql"
    path.parent.mkdir(parents=True)
    path.write_text("select 1")
    ctx = GraphContext(graph, project)
    assert [n.id for n in ctx.nodes_for_file(path)] == [node.id]
    assert ctx.location(node.id) == {"uri": path.as_uri(), "range": None}
    assert ctx.nodes_for_file(workspace / "dbt_packages" / "other" / "models" / "helper.sql") == []


def test_a_column_without_documentation_opens_its_model(model_graph, workspace):
    graph, project = model_graph
    ctx = GraphContext(graph, project)
    # locations.yml holds a semantic model, not the model's properties (no patch_path)
    undocumented = graph.columns_of(f"{J}.locations")[0]
    assert ctx.location(undocumented.id) == {
        "uri": (workspace / "models" / "marts" / "locations.sql").as_uri(),
        "range": None,
    }
    assert ctx.location("model.pilot_shop.nothing") is None


def test_locator_caches_and_forgets(workspace):
    locator = Locator(read_project(workspace))
    path = workspace / "models" / "marts" / "orders.yml"
    first = locator.entries(path)
    assert locator.entries(path) is first
    locator.forget(path)
    assert locator.entries(path) is not first
    assert locator.entries(workspace / "missing.yml").spans == {}


def test_clip():
    assert clip(None) is None and clip("short") == "short"
    assert clip("x" * 10, 5) == "xxxx…"


# Audit LP-SEC-04: paths from the manifest stay inside the project, or for a DAG file the workspace


def test_a_manifest_path_cannot_leave_the_project(tmp_path, workspace):
    import os
    import sys

    outside = tmp_path / "secret"
    outside.mkdir()
    (outside / "schema.yml").write_text("models:\n  - name: stolen\n")
    project = read_project(workspace)
    locator = Locator(project, workspace)
    model = Node("model.pilot_shop.x", NodeKind.MODEL, "x", package="pilot_shop")
    escapes = [
        "../secret/schema.yml",
        str(outside / "schema.yml"),
        "//evil-host/share/schema.yml",
    ]
    if sys.platform != "win32":
        os.symlink(outside / "schema.yml", workspace / "models" / "linked.yml")
        escapes.append("models/linked.yml")
    for relative in escapes:
        assert locator.file(model, relative) is None, relative
    package = Node("model.evil.x", NodeKind.MODEL, "x", package="../../secret")
    assert locator.file(package, "schema.yml") is None
    assert locator.file(model, "models/marts/orders.yml") == workspace / "models/marts/orders.yml"


def test_a_dag_file_may_be_outside_the_project_but_not_the_workspace(tmp_path, workspace):
    (tmp_path / "dags").mkdir()
    (tmp_path / "dags" / "d.py").write_text("# a DAG\n")
    task = Node("airflow:d.t", NodeKind.AIRFLOW_TASK, "t")
    inside = Locator(read_project(workspace), tmp_path)
    assert inside.file(task, "../dags/d.py") == tmp_path / "dags" / "d.py"
    assert inside.file(task, "../../etc/passwd") is None
    # With no workspace, the project is the boundary
    alone = Locator(read_project(workspace))
    assert alone.file(task, "../dags/d.py") is None
    # A project outside the workspace folder is its own workspace
    elsewhere = Locator(read_project(workspace), tmp_path / "dags")
    assert elsewhere.workspace == read_project(workspace).root


def test_yaml_entries_bound_aliases():
    import time

    def aliased(models: int, columns: int) -> str:
        names = "\n".join(f"      - name: c{i}" for i in range(columns))
        return "x: &m\n  name: m\n  columns:\n" + names + "\nmodels:\n" + "  - *m\n" * models

    # 2,000 uses of a model with 2,000 columns: refused, quickly, rather than walked
    started = time.perf_counter()
    assert yaml_entries(aliased(2000, 2000)).spans == {}
    assert time.perf_counter() - started < 2
    # Within the budget, each aliased node is read once
    entries = yaml_entries(aliased(200, 200))
    assert entries.get("models", "m") is not None
    assert entries.get("models", "m", "columns", "c199") is not None


def test_yaml_entries_survive_hostile_yaml():
    lol = "a: &a [x, x, x, x, x, x, x, x, x]\n" + "".join(
        f"{c}: &{c} [*{p}, *{p}, *{p}, *{p}, *{p}, *{p}, *{p}, *{p}, *{p}]\n"
        for p, c in zip("abcdefgh", "bcdefghi", strict=True)
    )
    assert yaml_entries(lol).spans == {}
    assert yaml_entries("models: " + "[" * 5000 + "]" * 5000).spans == {}
