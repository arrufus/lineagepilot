"""A DAG scan in the graph: the acceptance fixture, annotations, the path map (tasks 5.2 and 5.3)."""

from __future__ import annotations

import copy
import json
import shutil
import textwrap
from pathlib import Path

import pytest
from support import IMPACT_SHOP, copy_impact_shop

from lineagepilot_core.airflow.link import link_airflow, mark_not_analysed, unlink_airflow
from lineagepilot_core.airflow.view import airflow_view, summary_json
from lineagepilot_core.airflow.workspace import default_workspace, scan_airflow, scan_workspace
from lineagepilot_core.analysis.impact import impact
from lineagepilot_core.analysis.pii import pii_index
from lineagepilot_core.analysis.traversal import Direction, Granularity, steps
from lineagepilot_core.export import export_graph, graph_view
from lineagepilot_core.export.graph import ExportFormat
from lineagepilot_core.graph.model import Confidence, EdgeKind, NodeKind, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.rules import evaluate
from lineagepilot_core.snapshot import from_dict, to_dict

ORDERS = "model.impact_shop.orders"
REVENUE = "model.impact_shop.customer_revenue"
COUNTS = "model.impact_shop.order_counts"
GROUP = "airflow:daily_marts.marts"
BASH = "airflow:operations.refresh_counts"
SQL = "airflow:operations.export_orders"
NOTIFY = "airflow:daily_marts.notify_finance"


@pytest.fixture(scope="module")
def indexed():
    return index_project(IMPACT_SHOP).graph


@pytest.fixture(scope="module")
def scan():
    return scan_airflow(IMPACT_SHOP, IMPACT_SHOP)


@pytest.fixture(scope="module")
def linked(tmp_path_factory):
    root = copy_impact_shop(tmp_path_factory.mktemp("linked") / "impact_shop")
    graph = index_project(root).graph
    link_airflow(graph, scan_airflow(root, root))
    return graph


class TestAcceptanceFixture:
    def test_the_cosmos_group_is_one_node_with_derived_task_ids(self, linked):
        group = linked.nodes[GROUP]
        assert group.kind is NodeKind.AIRFLOW_TASK and group.owner == "data-platform"
        assert group.properties["operator"] == "DbtTaskGroup"
        assert group.properties["tasks"][ORDERS] == ["marts.orders.run", "marts.orders.test"]
        assert group.properties["tasks"][REVENUE] == ["marts.customer_revenue_run"]

    def test_the_group_selects_what_its_selector_says(self, linked):
        runs = {e.downstream for e in linked.out_edges(GROUP, (EdgeKind.ORCHESTRATES,))}
        assert ORDERS in runs and REVENUE in runs
        assert "model.impact_shop.broken_report" not in runs  # excluded in the DAG
        assert "model.impact_shop.stg_orders" not in runs

    def test_evidence_points_at_the_dag_file_and_its_version(self, linked):
        edge = linked.edge(EdgeKind.ORCHESTRATES, GROUP, ORDERS)
        evidence = edge.evidence[0]
        assert evidence.origin is Origin.DAG_STATIC
        assert evidence.confidence is Confidence.INFERRED
        assert evidence.location.path == "dags/daily_marts.py"
        assert evidence.location.start_line == 21  # 0-based, the DbtTaskGroup call
        assert len(evidence.artifact.sha256) == 64

    def test_a_shell_task_and_a_sql_task(self, linked):
        assert linked.edge(EdgeKind.ORCHESTRATES, BASH, COUNTS).transformation == "dbt run"
        edge = linked.edge(EdgeKind.ORCHESTRATES, SQL, ORDERS)
        assert edge.transformation == "reads"
        # main.orders names two parts of three, so it is a heuristic match
        assert edge.confidence is Confidence.HEURISTIC

    def test_tasks_that_run_after_are_downstream(self, linked):
        assert linked.edge(EdgeKind.TASK_DEPENDS_ON, GROUP, NOTIFY) is not None
        found = impact(linked, [ORDERS])
        assert {GROUP, NOTIFY, SQL, BASH} <= set(found.nodes)
        assert found.nodes[NOTIFY].depth == found.nodes[GROUP].depth + 1

    def test_upstream_of_a_task_are_what_it_runs_and_what_it_follows(self, linked):
        upstream = {s.ref for s in steps(linked, NOTIFY, Direction.UPSTREAM, Granularity.MODEL)}
        assert upstream == {GROUP}
        upstream = {s.ref for s in steps(linked, GROUP, Direction.UPSTREAM, Granularity.MODEL)}
        assert ORDERS in upstream

    def test_a_task_created_in_a_loop_is_an_unknown_at_its_call_site(self, linked):
        found = [u for u in linked.unresolved if u.subject == "airflow:backfill"]
        assert found and found[0].reference == "loop or factory"
        assert found[0].reason.startswith("dags/backfill.py:12:")
        assert ".lineagepilot/airflow.yml" in found[0].reason
        assert found[0].evidence.location.start_line == 11

    def test_every_impact_lists_the_unknowns_from_the_dag_files(self, linked):
        unknowns = impact(linked, ["model.impact_shop.stg_customers"]).unknowns
        assert any(u.subject == "airflow:backfill" for u in unknowns)

    def test_a_task_that_does_not_touch_this_project_is_not_a_node(self, linked):
        ids = {n.id for n in linked.nodes.values() if n.kind is NodeKind.AIRFLOW_TASK}
        assert GROUP in ids and "airflow:unrelated.t" not in ids

    def test_the_summary(self, linked):
        summary = linked.airflow
        assert summary.analysed and summary.dag_files == 3 and summary.dags == 3
        assert summary.tasks >= 4 and summary.links >= 5 and summary.unknowns == 1

    def test_the_view_of_one_node(self, linked):
        view = airflow_view(linked, [COUNTS])
        assert [d["name"] for d in view["dags"]] == ["daily_marts", "operations"]
        by_id = {t["id"]: t for d in view["dags"] for t in d["tasks"]}
        assert set(by_id) == {GROUP, BASH}
        assert by_id[BASH]["how"] == ["dbt run"] and by_id[BASH]["confidence"] == "heuristic"
        assert by_id[GROUP]["derivedTasks"] == ["marts.order_counts_run"]
        assert by_id[BASH]["owner"] == "operations-eng"

    def test_the_view_of_everything(self, linked):
        view = airflow_view(linked)
        assert {d["name"] for d in view["dags"]} == {"daily_marts", "operations", "backfill"}
        assert view["summary"]["analysed"] and view["unknowns"]


class TestReLinking:
    def test_unlinking_leaves_the_dbt_graph_as_it_was(self, indexed, scan):
        graph = from_dict(json.loads(json.dumps(to_dict(indexed))))
        before = to_dict(graph)
        link_airflow(graph, scan)
        assert graph.stats() != from_dict(copy.deepcopy(before)).stats()
        unlink_airflow(graph)
        assert to_dict(graph) == before
        assert graph.airflow is None
        assert graph.nodes_for_file("dags/daily_marts.py") == []

    def test_linking_twice_is_linking_once(self, indexed, scan):
        graph = from_dict(json.loads(json.dumps(to_dict(indexed))))
        link_airflow(graph, scan)
        first = (graph.stats(), to_dict(graph))
        link_airflow(graph, scan)
        assert (graph.stats(), to_dict(graph)) == first

    def test_rules_pii_and_exports_are_not_disturbed(self, indexed, scan):
        graph = from_dict(json.loads(json.dumps(to_dict(indexed))))
        before = evaluate(graph)
        pii_before = pii_index(graph).nodes()
        link_airflow(graph, scan)
        after = evaluate(graph)
        assert [f.subject for f in after.findings] == [f.subject for f in before.findings]
        assert pii_index(graph).nodes() == pii_before
        for fmt in ExportFormat:
            assert export_graph(graph_view(graph, None), fmt)

    def test_not_analysed_says_why(self, indexed):
        graph = from_dict(json.loads(json.dumps(to_dict(indexed))))
        mark_not_analysed(graph, "still reading")
        assert summary_json(graph.airflow)["reason"] == "still reading"
        assert summary_json(None)["analysed"] is False


def workspace_with(tmp_path, dag: str, annotation: str | None = None, project: str = "dbt"):
    """A repository with dbt in a folder of its own, and DAGs beside it."""
    root = tmp_path / "repo"
    (root / "dags").mkdir(parents=True)
    project_root = root / project
    shutil.copytree(IMPACT_SHOP, project_root, ignore=shutil.ignore_patterns("changes", "dags"))
    (root / "dags" / "d.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n" + textwrap.dedent(dag),
        encoding="utf-8",
    )
    if annotation is not None:
        (project_root / ".lineagepilot").mkdir()
        (project_root / ".lineagepilot" / "airflow.yml").write_text(annotation, encoding="utf-8")
    return root, project_root


def linked_graph(root, project):
    graph = index_project(project).graph
    scan = scan_airflow(project, root, ["dags"])
    link_airflow(graph, scan)
    return graph, scan


class TestPaths:
    def test_paths_are_relative_to_the_project_and_can_leave_it(self, tmp_path):
        root, project = workspace_with(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --select orders --project-dir dbt")
            """,
        )
        graph, scan = linked_graph(root, project)
        assert scan["project"] == "dbt"
        assert graph.nodes["airflow:d.t"].file_path == "../dags/d.py"

    def test_a_container_path_is_matched_by_its_tail_as_a_heuristic(self, tmp_path):
        root, project = workspace_with(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(
                    task_id="t",
                    bash_command="dbt run --select orders --project-dir /usr/local/airflow/dbt",
                )
            """,
        )
        graph, _ = linked_graph(root, project)
        edge = graph.edge(EdgeKind.ORCHESTRATES, "airflow:d.t", ORDERS)
        assert edge.confidence is Confidence.HEURISTIC
        assert any("last 1 path" in n for n in graph.nodes["airflow:d.t"].properties["notes"])

    def test_an_explicit_path_map_beats_the_tail_and_is_inferred(self, tmp_path):
        root, project = workspace_with(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(
                    task_id="t",
                    bash_command="dbt run --select orders --project-dir /opt/elsewhere/analytics",
                )
            """,
            "path_map:\n  /opt/elsewhere/analytics: .\n",
        )
        graph, _ = linked_graph(root, project)
        edge = graph.edge(EdgeKind.ORCHESTRATES, "airflow:d.t", ORDERS)
        assert edge.confidence is Confidence.INFERRED
        assert any("path map" in n for n in graph.nodes["airflow:d.t"].properties["notes"])

    def test_a_path_map_cannot_leave_the_workspace(self, tmp_path):
        root, project = workspace_with(tmp_path, "pass\n", "path_map:\n  /x: ../../..\n")
        graph, scan = linked_graph(root, project)
        assert "leaves the workspace" in scan["annotations"]["problems"][0]
        assert graph.airflow.problems == tuple(scan["annotations"]["problems"])

    def test_a_link_to_another_project_is_counted_and_not_read(self, tmp_path):
        root, project = workspace_with(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --project-dir other")
            """,
        )
        other = root / "other"
        other.mkdir()
        (other / "dbt_project.yml").write_text("name: other\n")
        graph, scan = linked_graph(root, project)
        assert scan["other_project_links"] == 1 and scan["links"] == []
        assert "airflow:d.t" not in graph.nodes

    def test_a_workspace_that_does_not_hold_the_project_is_ignored(self, tmp_path):
        root, project = workspace_with(tmp_path, "pass\n")
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        assert scan_airflow(project, elsewhere)["project"] == "."

    def test_the_default_workspace_is_the_repository(self, tmp_path):
        root, project = workspace_with(tmp_path, "pass\n")
        assert default_workspace(project) == Path(project.resolve())
        (root / ".git").mkdir()
        assert default_workspace(project) == Path(root.resolve())

    def test_every_project_can_be_scanned_without_a_focus(self, tmp_path):
        root, _ = workspace_with(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --select orders")
            """,
        )
        scan = scan_workspace(root, ["dags"])
        assert scan["project"] is None and scan["links"][0]["status"] == "resolved"


class TestAnnotations:
    def annotated(self, tmp_path, annotation, dag="pass\n"):
        return linked_graph(*workspace_with(tmp_path, dag, annotation))

    def test_a_declared_task_gets_declared_edges(self, tmp_path):
        graph, _ = self.annotated(
            tmp_path,
            """
            pipelines:
              - pipeline_id: loader
                owner: ingest
                tasks:
                  - task_id: load
                    consumes:
                      - model.impact_shop.orders
                      - urn: impact_shop.main.customer_revenue
                        access_pattern: incremental
                    validates: [order_counts]
                    produces:
                      - urn: main.stg_orders
                        operation: insert
            """,
        )
        task = "airflow:loader.load"
        assert graph.nodes[task].owner == "ingest"
        edge = graph.edge(EdgeKind.ORCHESTRATES, task, ORDERS)
        assert edge.transformation == "consumes" and edge.confidence is Confidence.DECLARED
        assert edge.evidence[0].origin is Origin.ANNOTATION
        assert edge.evidence[0].location.path == ".lineagepilot/airflow.yml"
        assert graph.edge(EdgeKind.ORCHESTRATES, task, REVENUE) is not None
        assert graph.edge(EdgeKind.ORCHESTRATES, task, COUNTS).transformation == "validates"
        assert (
            graph.edge(EdgeKind.ORCHESTRATES, task, "model.impact_shop.stg_orders").transformation
            == "produces"
        )

    def test_a_reference_nothing_matches_is_an_unknown_not_dropped(self, tmp_path):
        graph, _ = self.annotated(
            tmp_path,
            """
            pipelines:
              - pipeline_id: loader
                tasks:
                  - task_id: load
                    consumes: [no_such_model, also.no.such]
            """,
        )
        found = {u.reference for u in graph.unresolved if u.subject == "airflow:loader.load"}
        assert found == {"no_such_model", "also.no.such"}

    def test_a_task_the_scanner_could_not_read_is_answered_by_its_annotation(self, tmp_path):
        dag = """
        with DAG("d") as d:
            BashOperator(task_id="t", bash_command="dbt run --select {{ params.m }}")
        """
        annotation = """
        pipelines:
          - pipeline_id: d
            tasks:
              - task_id: t
                consumes: [model.impact_shop.orders]
        """
        graph, scan = self.annotated(tmp_path, annotation, dag)
        assert scan["unresolved"]
        assert not [u for u in graph.unresolved if u.evidence.origin is Origin.DAG_STATIC]
        assert graph.edge(EdgeKind.ORCHESTRATES, "airflow:d.t", ORDERS) is not None
        # It keeps the task's place in the DAG file
        assert graph.nodes["airflow:d.t"].file_path == "../dags/d.py"

    @pytest.mark.parametrize(
        "entry",
        ["file: dags/d.py", "dag_id: d", "{dag_id: d, task_id: t}", "{file: dags/d.py, line: 5}"],
    )
    def test_an_unknown_can_be_acknowledged(self, tmp_path, entry):
        dag = """
        with DAG("d") as d:
            BashOperator(task_id="t", bash_command="dbt run --select {{ params.m }}")
        """
        graph, _ = self.annotated(tmp_path, f"acknowledged:\n  - {entry}\n", dag)
        assert not [u for u in graph.unresolved if u.evidence.origin is Origin.DAG_STATIC]

    @pytest.mark.parametrize(
        "entry",
        [
            "file: dags/other.py",
            "dag_id: other",
            "{dag_id: d, task_id: u}",
            "{file: dags/d.py, line: 1}",
        ],
    )
    def test_acknowledging_something_else_leaves_it(self, tmp_path, entry):
        dag = """
        with DAG("d") as d:
            BashOperator(task_id="t", bash_command="dbt run --select {{ params.m }}")
        """
        graph, _ = self.annotated(tmp_path, f"acknowledged:\n  - {entry}\n", dag)
        assert [u for u in graph.unresolved if u.evidence.origin is Origin.DAG_STATIC]

    @pytest.mark.parametrize(
        ("text", "problem"),
        [
            ("path_map: [a]\n", "path_map is not a mapping"),
            ("path_map:\n  /x: 3\n", "not a path to a path"),
            ("acknowledged: 3\n", "acknowledged is not a list"),
            ("acknowledged:\n  - nope\n", "names no file"),
            ("acknowledged:\n  - {other: 1}\n", "names no file"),
            ("a: [\n", "cannot be read"),
        ],
    )
    def test_what_cannot_be_used_is_reported(self, tmp_path, text, problem):
        graph, _ = self.annotated(tmp_path, text)
        assert any(problem in p for p in graph.airflow.problems)

    def test_a_huge_annotation_file_is_refused(self, tmp_path, monkeypatch):
        from lineagepilot_core.parsers import data_flow_annotations

        monkeypatch.setattr(data_flow_annotations, "MAX_ANNOTATION_BYTES", 5)
        graph, _ = self.annotated(tmp_path, "path_map:\n  /x: .\n")
        assert any("under 5 bytes" in p for p in graph.airflow.problems)


class TestWhatTheGraphSays:
    def test_a_selector_that_matches_nothing_is_flagged(self, tmp_path):
        graph, _ = linked_graph(
            *workspace_with(
                tmp_path,
                """
                with DAG("d") as d:
                    BashOperator(task_id="t", bash_command="dbt run --select renamed_away")
                """,
            )
        )
        found = [u for u in graph.unresolved if u.reference == "selects nothing"]
        assert "renamed_away matches no node" in found[0].reason

    def test_a_dbt_test_with_nothing_to_test_is_not_flagged(self, tmp_path):
        graph, _ = linked_graph(
            *workspace_with(
                tmp_path,
                """
                with DAG("d") as d:
                    BashOperator(task_id="t", bash_command="dbt test --select stg_customers")
                """,
            )
        )
        assert not [u for u in graph.unresolved if u.reference == "selects nothing"]

    def test_a_selected_node_the_graph_does_not_have_is_flagged(self, tmp_path):
        root, project = workspace_with(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --select orders")
            """,
        )
        scan = scan_airflow(project, root, ["dags"])
        graph = index_project(project).graph
        scan["links"][0]["resolved_nodes"] = ["model.impact_shop.gone"]
        link_airflow(graph, scan)
        found = [u for u in graph.unresolved if u.reference == "model.impact_shop.gone"]
        assert "it may be stale" in found[0].reason

    def test_a_dag_id_used_twice_is_one_node(self, tmp_path):
        root, project = workspace_with(
            tmp_path,
            """
            with DAG("d") as d:
                BashOperator(task_id="t", bash_command="dbt run --select orders")
            """,
        )
        (root / "dags" / "e.py").write_text(
            "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
            'with DAG("d") as d:\n'
            '    BashOperator(task_id="t", bash_command="dbt run --select customer_revenue")\n'
        )
        graph, _ = linked_graph(root, project)
        task = "airflow:d.t"
        assert {e.downstream for e in graph.out_edges(task, (EdgeKind.ORCHESTRATES,))} == {
            ORDERS,
            REVENUE,
        }

    def test_a_task_in_a_group_is_followed_by_what_follows_the_group(self, tmp_path):
        graph, _ = linked_graph(
            *workspace_with(
                tmp_path,
                """
                from airflow.utils.task_group import TaskGroup
                with DAG("d") as d:
                    with TaskGroup("g") as g:
                        BashOperator(task_id="t", bash_command="dbt run --select orders")
                    after = BashOperator(task_id="after", bash_command="echo")
                    g >> after
                """,
            )
        )
        assert graph.edge(EdgeKind.TASK_DEPENDS_ON, "airflow:d.g.t", "airflow:d.g") is not None
        assert "airflow:d.after" in impact(graph, [ORDERS]).nodes

    def test_no_dag_files_says_where_it_looked(self, tmp_path):
        root, project = workspace_with(tmp_path, "pass\n")
        (root / "dags" / "d.py").unlink()
        graph, _ = linked_graph(root, project)
        assert (
            not graph.airflow.analysed and "no DAG files were found in dags" in graph.airflow.reason
        )
