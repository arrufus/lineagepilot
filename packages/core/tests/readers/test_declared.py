"""Consumers declared in the annotation file, in the graph and the review (task 7.4's second half).

``lineage.yml``'s ``consumers`` say what reads a model when no file says so. Each becomes a
consumer node with its tool, and ``consumes`` edges, down to columns, from origin ``annotation``.
"""

from __future__ import annotations

import io
import json
import shutil
import textwrap

import pytest
from support import IMPACT_SHOP, apply_rename

from lineagepilot_core.analysis import Granularity, impact
from lineagepilot_core.analysis.review import ChangedFile, FileStatus, review_changes
from lineagepilot_core.cli import main
from lineagepilot_core.graph.model import Confidence, EdgeKind, LineageState, NodeKind, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.project import read_project
from lineagepilot_core.readers.airflow.link import link_airflow, mark_not_analysed
from lineagepilot_core.readers.airflow.scan import scan_airflow
from lineagepilot_core.readers.airflow.view import reader_summary_json
from lineagepilot_core.readers.common.declared import read_declared
from lineagepilot_core.readers.views import consumers_view

ORDERS = "model.impact_shop.orders"
REVENUE = "model.impact_shop.customer_revenue"
LINEAGE = ".lineagepilot/lineage.yml"
DECLARED = """
consumers:
  - id: tableau:finance/orders_overview
    owner: finance-analytics
    consumes:
      - model.impact_shop.orders#order_total     # a column
  - id: tableau:ops/customers
    consumes:
      - model.impact_shop.orders#customer_id     # another column of the same model
  - id: notebook:churn
    tool: notebook
    consumes:
      - customer_revenue                         # a model, by its name, read whole
      - main.order_counts                        # a table, by part of its name
      - model.impact_shop.nowhere                # nothing
      - model.impact_shop.orders#no_such_column  # a column the model does not list
"""


@pytest.fixture(scope="module")
def shop(tmp_path_factory):
    root = tmp_path_factory.mktemp("shop") / "shop"
    shutil.copytree(IMPACT_SHOP, root, ignore=shutil.ignore_patterns("changes", "variants"))
    return root


@pytest.fixture
def project(tmp_path, shop):
    root = tmp_path / "shop"
    shutil.copytree(shop, root)
    (root / ".lineagepilot").mkdir()
    (root / LINEAGE).write_text(textwrap.dedent(DECLARED), encoding="utf-8")
    return root


def linked(project):
    graph = index_project(project).graph
    link_airflow(graph, scan_airflow(project, project, ["dags"]))
    return graph


def reads(graph, consumer):
    return {e.downstream: e.confidence for e in graph.out_edges(consumer, (EdgeKind.CONSUMES,))}


def test_each_declared_consumer_is_a_node_of_its_tool(project):
    graph = linked(project)
    overview = graph.nodes["tableau:finance/orders_overview"]
    assert (overview.kind, overview.tool, overview.name) == (
        NodeKind.CONSUMER,
        "tableau",
        "finance/orders_overview",
    )
    assert (overview.owner, overview.file_path) == ("finance-analytics", LINEAGE)
    assert graph.nodes["notebook:churn"].tool == "notebook"
    # Declared, from the annotation file, down to the column named
    assert reads(graph, "tableau:finance/orders_overview") == {
        ORDERS: Confidence.DECLARED,
        f"{ORDERS}#order_total": Confidence.DECLARED,
    }
    edge = graph.edge(EdgeKind.CONSUMES, "tableau:finance/orders_overview", ORDERS)
    assert {e.origin for e in edge.evidence} == {Origin.ANNOTATION}
    # A table matched by part of its name is a guess; a model's name is not
    churn = reads(graph, "notebook:churn")
    assert churn[REVENUE] is Confidence.DECLARED
    assert churn["model.impact_shop.order_counts"] is Confidence.HEURISTIC
    assert graph.airflow.consumers == 3
    assert reader_summary_json(graph.airflow)["consumers"] == 3


def test_what_matches_nothing_is_an_unknown_at_the_file(project):
    graph = linked(project)
    found = {(u.subject, u.reference): u.reason for u in graph.unresolved_for("notebook:churn")}
    assert found == {
        ("notebook:churn", "model.impact_shop.nowhere"): (
            ".lineagepilot/lineage.yml says it reads this, and no dbt node matches"
        ),
        ("notebook:churn", f"{ORDERS}#no_such_column"): (
            f"{ORDERS} has no column 'no_such_column' in the catalog or the docs; it is read as "
            "a whole"
        ),
    }
    assert {u.evidence.location.path for u in graph.unresolved_for("notebook:churn")} == {LINEAGE}


def test_a_consumer_of_columns_is_reached_only_by_them(project):
    graph = linked(project)
    assert graph.consumer_lineage["tableau:finance/orders_overview"].state is LineageState.RESOLVED
    assert "notebook:churn" not in graph.consumer_lineage  # it reads models whole
    found = impact(graph, [f"{ORDERS}#order_total"])
    overview = found.nodes["tableau:finance/orders_overview"]
    assert (overview.granularity, overview.reads) == (Granularity.COLUMN, [f"{ORDERS}#order_total"])
    assert "tableau:ops/customers" not in found.nodes  # it reads customer_id
    churn = found.nodes["notebook:churn"]
    assert (churn.granularity, churn.reason) == (Granularity.MODEL, "uses the whole node")
    assert found.counts_by_tool()[("consumer", "tableau")] == 1


def test_the_consumers_view_and_a_relink(project):
    graph = linked(project)
    view = consumers_view(graph, [ORDERS])
    by_id = {c["id"]: c for c in view["consumers"]}
    assert by_id["tableau:finance/orders_overview"]["reads"][0]["columns"] == ["order_total"]
    assert by_id["notebook:churn"]["reads"][0]["columns"] == []  # the whole node
    # Linking again replaces them, and a scan that is not analysed takes them out
    link_airflow(graph, scan_airflow(project, project, ["dags"]))
    assert (
        sum(
            1
            for n in graph.nodes.values()
            if n.kind is NodeKind.CONSUMER and n.tool != "dbt_semantic"
        )
        == 3
    )
    mark_not_analysed(graph, "turned off")
    assert not any(n.id.startswith(("tableau:", "notebook:")) for n in graph.nodes.values())
    assert graph.unresolved_for("notebook:churn") == []
    assert "tableau:finance/orders_overview" not in graph.consumer_lineage


def test_the_review_lists_them_by_tool_with_their_owners(project):
    apply_rename(project)
    graph = linked(project)
    review = review_changes(
        graph,
        [ChangedFile("models/marts/orders.sql", FileStatus.MODIFIED)],
        read_project(project),
    )
    rows = {n["id"]: n for n in review["impacted"]}
    overview = rows["tableau:finance/orders_overview"]
    assert (overview["kind"], overview["tool"], overview["reach"], overview["columns"]) == (
        "consumer",
        "tableau",
        "column",
        ["order_total"],
    )
    assert overview["owner"] == "finance-analytics"
    assert "tableau:ops/customers" in rows and rows["tableau:ops/customers"]["reach"] == "model"
    markdown = review["markdown"]
    consumers = markdown.split("### Consumers")[1].split("### Owners")[0]
    assert "#### Tableau (2)" in consumers and "#### notebook (1)" in consumers
    assert "| `finance/orders_overview` | Field `order_total` |" in consumers
    # Its unknowns are under its tool
    unknowns = markdown.split("### Unknowns")[1]
    assert "#### notebook (2)" in unknowns


def test_the_cli_reads_them_without_the_dag_files(project):
    out = io.StringIO()
    code = main(
        [
            "impact",
            f"{ORDERS}#order_total",
            "--project-dir",
            str(project),
            "--no-cache",
            "--no-airflow",
            "--json",
        ],
        out=out,
    )
    found = json.loads(out.getvalue())
    assert code == 0
    assert "tableau:finance/orders_overview" in {n["id"] for n in found["nodes"]}
    declared = read_declared(project)
    assert declared["file"] == LINEAGE and len(declared["consumers"]) == 3
