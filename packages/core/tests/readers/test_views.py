"""Protocol 2's views of pipelines, consumers and readers (task 7.5)."""

from __future__ import annotations

from lineagepilot_core.graph.model import (
    PIPELINE_KINDS,
    AirflowSummary,
    Column,
    Confidence,
    EdgeKind,
    Evidence,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
)
from lineagepilot_core.readers.views import consumers_view, pipelines_view, reader_summaries

ORDERS = "model.shop.orders"
ITEMS = "model.shop.items"
RAW = "source.shop.raw.orders"
DAG = "airflow:daily"
RUN = "airflow:daily.run_orders"
LOAD = "dagster:ingest.load_orders"
VIEW = "looker:view.orders"
METRIC = "metric.shop.revenue"
SCANNED = Evidence(Origin.DAG_STATIC, Confidence.INFERRED, location=Location("dags/d.py"))
DECLARED = Evidence(Origin.ANNOTATION, Confidence.DECLARED)
GUESSED = Evidence(Origin.ANNOTATION, Confidence.HEURISTIC)


def graph() -> MetadataGraph:
    g = MetadataGraph()
    for node_id, kind in (
        (ORDERS, NodeKind.MODEL),
        (ITEMS, NodeKind.MODEL),
        (RAW, NodeKind.SOURCE),
    ):
        g.add_node(Node(node_id, kind, node_id.rsplit(".", 1)[1]))
    g.add_node(Node(DAG, NodeKind.PIPELINE, "daily", tool="airflow", subkind="dag", owner="etl"))
    g.add_node(
        Node(
            RUN,
            NodeKind.TASK,
            "run_orders",
            tool="airflow",
            subkind="task",
            properties={"dag": "daily", "operator": "BashOperator", "notes": ["n"]},
        )
    )
    g.add_node(Node(LOAD, NodeKind.TASK, "load_orders", tool="dagster", subkind="asset"))
    g.add_node(Node(VIEW, NodeKind.CONSUMER, "orders", tool="looker", subkind="view"))
    g.add_node(Node(METRIC, NodeKind.CONSUMER, "revenue", tool="dbt_semantic", subkind="metric"))
    g.add_column(Column(f"{ORDERS}#amount", ORDERS, "amount"))
    g.add_column(Column(f"{ORDERS}#id", ORDERS, "id"))
    g.add_column(Column(f"{VIEW}#total", VIEW, "total"))
    g.add_column(Column(f"{VIEW}#order_id", VIEW, "order_id"))
    g.add_edge(EdgeKind.ORCHESTRATES, RUN, ORDERS, SCANNED, transformation="dbt run")
    g.add_edge(EdgeKind.TASK_DEPENDS_ON, DAG, RUN, SCANNED)
    g.add_edge(EdgeKind.LOADS, LOAD, RAW, DECLARED)
    g.add_edge(EdgeKind.CONSUMES, VIEW, ORDERS, DECLARED)
    g.add_edge(EdgeKind.CONSUMES, f"{VIEW}#total", f"{ORDERS}#amount", GUESSED)
    g.add_edge(EdgeKind.CONSUMES, f"{VIEW}#order_id", f"{ORDERS}#id", DECLARED)
    g.add_edge(EdgeKind.CONSUMES, METRIC, ORDERS, GUESSED)
    g.add_unresolved(Unresolved(RUN, "loop", "dags/d.py:3: a loop", SCANNED))
    g.add_unresolved(
        Unresolved(
            ORDERS, "column lineage", "no SQL", Evidence(Origin.MANIFEST, Confidence.HEURISTIC)
        )
    )
    return g


class TestPipelines:
    def test_every_pipeline_and_task(self):
        view = pipelines_view(graph())
        assert [p["id"] for p in view["pipelines"]] == [DAG, "dagster:ingest"]
        [run] = view["pipelines"][0]["tasks"]
        assert run["id"] == RUN and (run["kind"], run["tool"], run["subkind"]) == (
            "task",
            "airflow",
            "task",
        )
        assert run["nodes"] == [ORDERS] and run["how"] == ["dbt run"]
        assert run["confidence"] == "inferred" and run["origins"] == ["dag_static"]
        assert view["pipelines"][0]["owner"] == "etl" and view["pipelines"][0]["subkind"] == "dag"
        # A task with no pipeline node of its own still has a pipeline, by its ID
        dagster = view["pipelines"][1]
        assert dagster["name"] == "ingest" and dagster["subkind"] is None
        assert dagster["tasks"][0]["nodes"] == [RAW]  # what it loads

    def test_only_what_touches_some_nodes(self):
        view = pipelines_view(graph(), nodes=[RAW])
        assert [t["id"] for p in view["pipelines"] for t in p["tasks"]] == [LOAD]
        assert pipelines_view(graph(), nodes=[ITEMS])["pipelines"] == []

    def test_only_some_tools(self):
        view = pipelines_view(graph(), tools=["airflow"])
        assert [p["id"] for p in view["pipelines"]] == [DAG]
        assert [u["subject"] for u in view["unknowns"]] == [RUN]
        assert pipelines_view(graph(), tools=["meltano"]) == {"pipelines": [], "unknowns": []}


class TestConsumers:
    def test_what_reads_a_node_and_which_columns(self):
        view = consumers_view(graph(), [ORDERS])
        assert [c["id"] for c in view["consumers"]] == [VIEW, METRIC]  # by ID
        looker, metric = view["consumers"]
        assert metric["reads"] == [
            {"node": ORDERS, "columns": [], "confidence": "heuristic", "origins": ["annotation"]}
        ]
        assert (looker["tool"], looker["subkind"]) == ("looker", "view")
        # The view reads the whole model, and two of its columns: the strongest evidence counts
        assert looker["reads"] == [
            {
                "node": ORDERS,
                "columns": ["amount", "id"],
                "confidence": "declared",
                "origins": ["annotation"],
            }
        ]

    def test_a_node_nothing_reads(self):
        assert consumers_view(graph(), [ITEMS, ITEMS]) == {"consumers": []}


class TestReaderSummaries:
    def test_each_reader_of_pipelines_says_what_it_read(self):
        g = graph()
        [airflow] = reader_summaries(g, PIPELINE_KINDS)
        assert airflow["tool"] == "airflow" and not airflow["analysed"]
        assert airflow["reason"] == "the DAG files were not analysed"
        g.airflow = AirflowSummary(
            analysed=True, dag_files=3, dags=2, tasks=5, folders=("**/dags",)
        )
        [airflow] = reader_summaries(g, PIPELINE_KINDS)
        assert (airflow["files"], airflow["pipelines"], airflow["tasks"]) == (3, 2, 5)
        assert airflow["paths"] == ["**/dags"] and airflow["consumers"] == 0

    def test_no_reader_reads_consumers_yet(self):
        assert reader_summaries(graph(), {NodeKind.CONSUMER}) == []
