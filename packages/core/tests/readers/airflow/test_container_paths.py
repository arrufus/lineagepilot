"""A path in a container that names no project (LP-c05, task E.3 of the next phase's plan).

When dbt runs in a container (a DockerOperator, a pod, or ``docker run`` in a shell), the project
path it is given is the container's: ``/app`` or ``/usr/app/dbt``. When that matches no project by
its tail, a bind mount that names the host folder is followed; failing that, the workspace's only
dbt project is taken. Both are ``heuristic``. With two or more projects and no mount, the path stays
unknown. A path outside a container gets neither.
"""

from __future__ import annotations

import shutil
import textwrap

import pytest
from airflow_support import MINI, SHOP, scan

HEADER = """\
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from docker.types import Mount
"""


@pytest.fixture
def one(tmp_path):
    """A workspace with one dbt project, under a folder whose name no container path has."""
    shutil.copytree(SHOP, tmp_path / "transform" / "shop")
    (tmp_path / "dags").mkdir()
    return tmp_path


@pytest.fixture
def two(one):
    shutil.copytree(MINI, one / "transform" / "mini")
    return one


def run(ws, body):
    dag = ws / "dags" / "d.py"
    dag.write_text(HEADER + textwrap.dedent(body))
    return {l["task_id"]: l for l in scan([str(dag)], workspace=str(ws))["links"]}


def resolved(link, project="transform/shop"):
    assert link["status"] == "resolved", link["unknown_reasons"]
    assert link["project_path"] == project and link["confidence"] == "heuristic"
    return link


def test_a_bind_mount_names_the_host_folder(two):
    links = run(
        two,
        """
        HOST = "/home/someone/repo/transform/shop"
        with DAG("d") as dag:
            DockerOperator(
                task_id="flag", image="x", command="dbt run --select stg_orders --project-dir /app",
                mounts=[Mount(target="/app/target", source="dbt_target", type="volume"),
                        Mount(source=HOST, target="/app", type="bind")],
            )
            DockerOperator(
                task_id="below", image="x", command="dbt run --project-dir /work/shop",
                mounts=[Mount("/work", "/home/someone/repo/transform", "bind")],
            )
            DockerOperator(
                task_id="cwd", image="x", command="dbt run --select stg_orders", working_dir="/app",
                mounts=[Mount(source=HOST, target="/app", type="bind")],
            )
            DockerOperator(
                task_id="volumes", image="x", command=["dbt", "run", "--project-dir", "/usr/app"],
                volumes=["/srv/repo/transform/mini:/usr/app:ro", "named_volume:/data"],
            )
        """,
    )
    flag = resolved(links["flag"])
    assert [m.split(".")[-1] for m in flag["resolved_models"]] == ["stg_orders"]
    assert any(
        "mounted from the host folder '/home/someone/repo/transform/shop'" in n
        for n in flag["notes"]
    )
    resolved(links["below"])
    resolved(links["cwd"])
    resolved(links["volumes"], "transform/mini")


def test_the_only_project_when_no_mount_names_it(one):
    links = run(
        one,
        """
        with DAG("d") as dag:
            DockerOperator(task_id="docker", image="x", command="dbt run --select stg_orders --project-dir /usr/app/dbt")
            KubernetesPodOperator(task_id="pod", name="p", image="x", cmds=["dbt"], arguments=["run", "--project-dir", "/app"])
            BashOperator(task_id="remote", bash_command="docker run --rm img dbt run --project-dir /app")
            DockerOperator(
                task_id="volume_only", image="x", command="dbt run --project-dir /app",
                mounts=[Mount(source="dbt_vol", target="/app", type="volume")],
            )
        """,
    )
    for task_id in ("docker", "pod", "remote", "volume_only"):
        link = resolved(links[task_id])
        assert any("assumed the only dbt project" in n for n in link["notes"]), task_id


def test_with_two_projects_and_no_mount_the_path_stays_unknown(two):
    links = run(
        two,
        """
        with DAG("d") as dag:
            DockerOperator(task_id="docker", image="x", command="dbt run --project-dir /app")
            KubernetesPodOperator(task_id="pod", name="p", image="x", cmds=["dbt", "run", "--project-dir", "/app"])
            DockerOperator(
                task_id="host_matches_nothing", image="x", command="dbt run --project-dir /app",
                mounts=[Mount(source="/home/someone/elsewhere", target="/app", type="bind")],
            )
            DockerOperator(
                task_id="not_literal", image="x", command="dbt run --project-dir /app",
                mounts=[Mount(source=SOMEWHERE, target="/app", type="bind")],
            )
        """,
    )
    for task_id, link in links.items():
        assert link["status"] == "unknown", task_id
        assert any("matches no dbt project here" in r for r in link["unknown_reasons"]), task_id


def test_a_path_outside_a_container_is_not_guessed(one):
    """A BashOperator runs on the worker: a path there that names no project is not the only
    project's, which could be another repository's."""
    links = run(
        one,
        """
        with DAG("d") as dag:
            BashOperator(task_id="bash", bash_command="dbt run --project-dir /usr/app/dbt")
        """,
    )
    assert links["bash"]["status"] == "unknown"


def test_a_tail_match_still_comes_first(two):
    links = run(
        two,
        """
        with DAG("d") as dag:
            DockerOperator(
                task_id="t", image="x", command="dbt run --project-dir /opt/transform/mini",
                mounts=[Mount(source="/home/someone/repo/transform/shop", target="/opt/transform/mini", type="bind")],
            )
        """,
    )
    resolved(links["t"], "transform/mini")
