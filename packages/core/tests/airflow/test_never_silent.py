"""Two silences the fresh held-out corpus found (LP-aax, corpus/README.md "After the freeze").

Both only add unknowns: what was left out in silence is now reported at its line.
"""

from __future__ import annotations

import shutil
import textwrap

import pytest
from airflow_support import SHOP, scan

HEADER = """\
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from cosmos import DbtTaskGroup, ProjectConfig
"""


@pytest.fixture
def ws(tmp_path):
    shutil.copytree(SHOP, tmp_path / "dbt" / "shop")
    (tmp_path / "dags").mkdir()
    return tmp_path


def run(ws, body):
    dag = ws / "dags" / "d.py"
    dag.write_text(HEADER + textwrap.dedent(body))
    return scan([str(dag)], workspace=str(ws))


def skipped(res):
    return [u for u in res["unresolved"] if "decided by an environment variable" in u["reason"]]


def test_a_branch_an_environment_default_skips_is_reported(ws):
    res = run(
        ws,
        """
        with DAG("d") as dag:
            if os.getenv("DBT_ENABLED", "false").lower() == "true":
                BashOperator(task_id="dbt_run", bash_command="dbt run --project-dir /x/dbt/shop")
                EmptyOperator(task_id="cleanup")
            if not os.getenv("SKIP_DBT", ""):
                BashOperator(task_id="kept", bash_command="dbt run --project-dir /x/dbt/shop")
            else:
                DbtTaskGroup(group_id="g", project_config=ProjectConfig("/x/dbt/shop"))
        """,
    )
    lines = sorted(u["line"] for u in skipped(res))
    assert lines == [10, 15], skipped(res)
    assert all(u["mentions_dbt"] and u["category"] == "not_literal" for u in skipped(res))
    assert "test at line 9" in skipped(res)[0]["reason"]
    # what the guess chose is still scanned
    assert [l["task_id"] for l in res["links"]] == ["kept"]


def test_a_branch_a_literal_decides_is_not_reported(ws):
    res = run(
        ws,
        """
        FLAG = False
        with DAG("d") as dag:
            if FLAG:
                BashOperator(task_id="dbt_run", bash_command="dbt run")
            if __name__ == "__main__":
                BashOperator(task_id="dbt_main", bash_command="dbt run")
        """,
    )
    assert not skipped(res) and not res["links"]


def test_a_docker_command_for_a_dbt_image_entrypoint_is_reported(ws):
    res = run(
        ws,
        """
        with DAG("d") as dag:
            DockerOperator(task_id="transform", image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest", command="run --target dev")
            DockerOperator(task_id="other", image="busybox", command="echo hello")
            DockerOperator(task_id="also_other", image="busybox", command="ls -la")
        """,
    )
    (u,) = [u for u in res["unresolved"] if u.get("task_id") == "transform"]
    assert "no dbt command could be read" in u["reason"] and u["mentions_dbt"]
    assert not [u for u in res["unresolved"] if u.get("task_id") in ("other", "also_other")]
