"""The annotation file for every tool: ``lineage.yml``, and ``airflow.yml`` still read (task 7.4).

Decision D-14: ``.lineagepilot/lineage.yml`` is the file. ``.lineagepilot/airflow.yml`` keeps
working, and when both exist ``lineage.yml`` is read and the status says the other was ignored.
"""

from __future__ import annotations

import os
import shutil
import textwrap

import pytest
from support import IMPACT_SHOP

from lineagepilot_core.graph.model import EdgeKind, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.airflow.link import link_airflow
from lineagepilot_core.readers.airflow.scan import scan_airflow

ORDERS = "model.impact_shop.orders"
REVENUE = "model.impact_shop.customer_revenue"
LINEAGE = ".lineagepilot/lineage.yml"
AIRFLOW = ".lineagepilot/airflow.yml"
IGNORED = f"{AIRFLOW} was ignored: {LINEAGE} is read instead"


def declares(task: str, model: str) -> str:
    return f"""
    pipelines:
      - pipeline_id: loader
        tasks:
          - task_id: {task}
            consumes: [{model}]
    """


@pytest.fixture(scope="module")
def shop(tmp_path_factory):
    """The dbt project without its DAG files, copied once for the module."""
    root = tmp_path_factory.mktemp("shop") / "shop"
    shutil.copytree(IMPACT_SHOP, root, ignore=shutil.ignore_patterns("changes", "dags"))
    return root


@pytest.fixture
def project(tmp_path, shop):
    """A project with no DAG files, so what the graph gets comes from the annotation file."""
    root = tmp_path / "shop"
    shutil.copytree(shop, root)
    (root / ".lineagepilot").mkdir()
    return root


def write(project, name: str, text: str) -> None:
    (project / name).write_text(textwrap.dedent(text), encoding="utf-8")


def scanned(project):
    scan = scan_airflow(project, project, ["dags"])
    graph = index_project(project).graph
    link_airflow(graph, scan)
    return graph, scan


class TestWhichFile:
    def test_lineage_yml_alone(self, project):
        write(project, LINEAGE, declares("load", ORDERS))
        graph, scan = scanned(project)
        assert scan["annotations"]["file"] == LINEAGE
        edge = graph.edge(EdgeKind.ORCHESTRATES, "airflow:loader.load", ORDERS)
        assert edge.evidence[0].origin is Origin.ANNOTATION
        assert edge.evidence[0].location.path == LINEAGE
        assert graph.airflow.annotation_file == LINEAGE and graph.airflow.problems == ()

    def test_airflow_yml_alone_still_works(self, project):
        write(project, AIRFLOW, declares("load", ORDERS))
        graph, scan = scanned(project)
        assert scan["annotations"]["file"] == AIRFLOW
        edge = graph.edge(EdgeKind.ORCHESTRATES, "airflow:loader.load", ORDERS)
        assert edge.evidence[0].location.path == AIRFLOW
        assert graph.airflow.problems == ()

    def test_with_both_lineage_yml_wins_and_the_status_says_so(self, project):
        write(project, LINEAGE, declares("from_lineage", ORDERS))
        write(project, AIRFLOW, declares("from_airflow", REVENUE))
        graph, scan = scanned(project)
        assert scan["annotations"]["file"] == LINEAGE
        assert "airflow:loader.from_lineage" in graph.nodes
        assert "airflow:loader.from_airflow" not in graph.nodes
        assert IGNORED in graph.airflow.problems

    def test_an_empty_lineage_yml_still_wins(self, project):
        """Which file counts does not depend on what it holds."""
        write(project, LINEAGE, "")
        write(project, AIRFLOW, declares("from_airflow", REVENUE))
        graph, _ = scanned(project)
        assert "airflow:loader.from_airflow" not in graph.nodes
        assert IGNORED in graph.airflow.problems

    def test_a_lineage_yml_that_cannot_be_read_does_not_fall_back(self, project):
        write(project, LINEAGE, "pipelines: [\n")
        write(project, AIRFLOW, declares("from_airflow", REVENUE))
        graph, _ = scanned(project)
        assert "airflow:loader.from_airflow" not in graph.nodes
        assert IGNORED in graph.airflow.problems
        assert any("lineage.yml cannot be read" in p for p in graph.airflow.problems)

    @pytest.mark.skipif(os.name == "nt", reason="symlinks need privileges on Windows")
    def test_a_lineage_yml_outside_the_workspace_is_not_read(self, project, tmp_path):
        secret = tmp_path / "secret.yml"
        secret.write_text(textwrap.dedent(declares("secret", ORDERS)), encoding="utf-8")
        (project / LINEAGE).symlink_to(secret)
        write(project, AIRFLOW, declares("from_airflow", REVENUE))
        graph, scan = scanned(project)
        assert scan["annotations"]["file"] is None
        assert "airflow:loader.secret" not in graph.nodes
        assert "airflow:loader.from_airflow" not in graph.nodes
        assert f"{LINEAGE} is not a regular file in the workspace" in graph.airflow.problems

    def test_neither(self, project):
        graph, scan = scanned(project)
        assert scan["annotations"]["file"] is None
        assert scan["annotations"]["consumers"] == [] and graph.airflow.problems == ()


class TestTools:
    def test_a_pipeline_of_another_tool_is_kept_out_and_said_so(self, project):
        write(
            project,
            LINEAGE,
            f"""
            pipelines:
              - pipeline_id: nightly
                tool: dlt
                tasks:
                  - {{task_id: a, consumes: [{ORDERS}]}}
                  - {{task_id: b, consumes: [{ORDERS}]}}
              - pipeline_id: orch
                tool: dagster
                tasks:
                  - {{task_id: c, consumes: [{ORDERS}]}}
              - pipeline_id: loader
                tool: airflow
                tasks:
                  - {{task_id: load, consumes: [{ORDERS}]}}
            """,
        )
        graph, scan = scanned(project)
        assert [t["task"] for t in scan["annotations"]["tasks"]] == ["load"]
        assert scan["annotations"]["tasks"][0]["tool"] == "airflow"
        assert "airflow:loader.load" in graph.nodes
        assert not any(n.startswith(("dagster:", "dlt:")) for n in graph.nodes)
        assert "2 tasks with tool dlt: no reader for dlt yet; ignored" in graph.airflow.problems
        # Dagster has a reader, which links its own (task 8.8)
        assert not any("dagster" in problem for problem in graph.airflow.problems)


class TestConsumers:
    def test_a_consumer_on_a_column(self, project):
        write(
            project,
            LINEAGE,
            f"""
            consumers:
              - id: tableau:finance/orders_overview
                owner: finance-analytics
                consumes:
                  - {ORDERS}
                  - {ORDERS}#amount
            """,
        )
        graph, scan = scanned(project)
        assert scan["annotations"]["consumers"] == [
            {
                "id": "tableau:finance/orders_overview",
                "tool": "tableau",
                "name": "finance/orders_overview",
                "owner": "finance-analytics",
                "consumes": [
                    {"node": ORDERS, "column": None},
                    {"node": ORDERS, "column": "amount"},
                ],
            }
        ]
        assert graph.airflow.problems == ()

    def test_consumers_are_read_from_airflow_yml_too(self, project):
        write(project, AIRFLOW, f"consumers:\n  - {{id: tableau:gl, consumes: [{ORDERS}]}}\n")
        _, scan = scanned(project)
        assert [c["id"] for c in scan["annotations"]["consumers"]] == ["tableau:gl"]

    def test_what_cannot_be_used_reaches_the_status(self, project):
        write(project, LINEAGE, "consumers:\n  - {id: gl, consumes: [x]}\n")
        graph, scan = scanned(project)
        assert scan["annotations"]["consumers"] == []
        assert any("names no tool" in p for p in graph.airflow.problems)
