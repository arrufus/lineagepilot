"""An environment variable with no default, followed by a path, names a project by that path
(LP-0o1, task E.1 of the next phase's plan).

``f'{os.environ["AIRFLOW_HOME"]}/dags/dbt/x'`` was unknown because the variable has no default,
though the path after it names the project. It is now matched by that path alone, as a container
path is, and the link is ``heuristic``. Anything else done with such a variable stays unknown.
"""

from __future__ import annotations

import shutil
import textwrap

import pytest
from airflow_support import MINI, SHOP, scan

from lineagepilot_core.readers.common.projects import ProjectIndex
from lineagepilot_core.readers.common.safe_io import env_tail
from lineagepilot_core.readers.common.shell import find_dbt_in_shell
from lineagepilot_core.readers.common.values import EnvPath, Unknown, plain, text_of

HEADER = """\
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
"""


@pytest.fixture(scope="module")
def ws(tmp_path_factory):
    root = tmp_path_factory.mktemp("ws")
    shutil.copytree(SHOP, root / "dags" / "dbt" / "shop")
    (root / "dags" / "x").mkdir()
    return root


def run(ws, body, name="t.py"):
    path = ws / "dags" / "x" / name
    path.write_text(HEADER + textwrap.dedent(body))
    return scan([str(path)], workspace=str(ws))


def only_link(res):
    assert len(res["links"]) == 1, res["links"]
    return res["links"][0]


def resolved_to_shop(link, models=("stg_orders",)):
    assert link["status"] == "resolved", link["unknown_reasons"]
    assert link["project_path"] == "dags/dbt/shop"
    assert link["confidence"] == "heuristic"
    assert sorted(m.split(".")[-1] for m in link["resolved_models"]) == sorted(models)


# --- one fixture per form ---------------------------------------------------------------------
PROJECT_FORMS = {
    "subscript": 'os.environ["AIRFLOW_HOME"] + "/dags/dbt/shop"',
    "get": 'os.environ.get("AIRFLOW_HOME") + "/dags/dbt/shop"',
    "getenv": 'os.getenv("AIRFLOW_HOME", None) + "/dags/dbt/shop"',
    "f-string": "f\"{os.environ['AIRFLOW_HOME']}/dags/dbt/shop\"",
    "os.path.join": 'os.path.join(os.environ["AIRFLOW_HOME"], "dags", "dbt", "shop")',
    "Path /": 'str(Path(os.environ["AIRFLOW_HOME"]) / "dags" / "dbt" / "shop")',
    "Path args": 'Path(os.environ["AIRFLOW_HOME"], "dags/dbt/shop")',
    "joinpath": 'Path(os.environ["AIRFLOW_HOME"]).joinpath("dags", "dbt", "shop").as_posix()',
    ".format": '"{}/dags/dbt/shop".format(os.environ["AIRFLOW_HOME"])',
    "%": '"%s/dags/dbt/shop" % os.environ["AIRFLOW_HOME"]',
    "parent": "Path(f\"{os.environ['AIRFLOW_HOME']}/dags/dbt/shop/models\").parent",
    "dirname": 'os.path.dirname(os.environ["AIRFLOW_HOME"] + "/dags/dbt/shop/x")',
    "abspath": 'os.path.join(os.path.abspath(os.environ["AIRFLOW_HOME"]), "dags/dbt/shop")',
    "through a name": "SHOP_DIR",
}


@pytest.mark.parametrize("form", sorted(PROJECT_FORMS))
def test_each_form_names_the_project_by_the_path_after_the_variable(ws, form):
    body = f"""
        DBT_ROOT = os.environ["AIRFLOW_HOME"]
        SHOP_DIR = f"{{DBT_ROOT}}/dags/dbt/shop"
        with DAG("d") as dag:
            BashOperator(task_id="t", bash_command="dbt run --select stg_orders --project-dir " + str({PROJECT_FORMS[form]}))
    """
    link = only_link(run(ws, body))
    resolved_to_shop(link)
    assert any(
        "${AIRFLOW_HOME}" in n and "after the environment variable" in n for n in link["notes"]
    )


def test_cosmos_project_config_and_its_operators(ws):
    body = """
        from cosmos import DbtDag, ProjectConfig, RenderConfig
        from cosmos.operators.local import DbtRunLocalOperator
        DBT = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/shop"
        DbtDag(
            dag_id="cosmos_env",
            project_config=ProjectConfig(dbt_project_path=DBT),
            render_config=RenderConfig(select=["stg_orders"]),
        )
        with DAG("ops") as dag:
            DbtRunLocalOperator(task_id="r", project_dir=DBT, select=["stg_customers"])
    """
    res = run(ws, body)
    dag = next(l for l in res["links"] if l["kind"] == "cosmos_dag")
    assert dag["status"] == "resolved", dag["unknown_reasons"]
    assert dag["project_path"] == "dags/dbt/shop" and dag["confidence"] == "heuristic"
    assert [m.split(".")[-1] for m in dag["resolved_models"]] == ["stg_orders"]
    op = next(l for l in res["links"] if l["kind"] == "cosmos_operator")
    resolved_to_shop(op, ("stg_customers",))


def test_cosmos_legacy_root_and_a_manifest_path(ws):
    body = """
        from cosmos import DbtDag, ProjectConfig
        DbtDag(dag_id="legacy", dbt_root_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt", dbt_project_name="shop", select={"paths": ["models/staging"]})
        DbtDag(
            dag_id="manifest",
            project_config=ProjectConfig(manifest_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/shop/target/manifest.json"),
        )
    """
    res = run(ws, body)
    legacy = next(l for l in res["links"] if l["dag_id"] == "legacy")
    assert legacy["status"] == "resolved", legacy["unknown_reasons"]
    assert legacy["project_path"] == "dags/dbt/shop"
    manifest = next(l for l in res["links"] if l["dag_id"] == "manifest")
    assert manifest["status"] == "resolved", manifest["unknown_reasons"]
    assert manifest["project_path"] == "dags/dbt/shop" and manifest["confidence"] == "heuristic"


def test_cd_working_directory_and_other_operators(ws):
    body = """
        from airflow.providers.docker.operators.docker import DockerOperator
        from airflow_dbt.operators.dbt_operator import DbtRunOperator
        HOME = os.environ["AIRFLOW_HOME"]
        with DAG("d") as dag:
            BashOperator(task_id="cd_py", bash_command=f"cd {HOME}/dags/dbt/shop && dbt run --select stg_orders")
            BashOperator(task_id="cd_sh", bash_command="cd $AIRFLOW_HOME/dags/dbt && cd shop && dbt run --select stg_orders")
            BashOperator(task_id="flag_sh", bash_command="dbt run --select stg_orders --project-dir ${AIRFLOW_HOME}/dags/dbt/shop")
            BashOperator(task_id="cwd", bash_command="dbt run --select stg_orders", cwd=HOME + "/dags/dbt/shop")
            DbtRunOperator(task_id="af", dir=f"{HOME}/dags/dbt/shop", select="stg_orders")
            DockerOperator(task_id="dock", image="x", command="dbt run --select stg_orders", working_dir=f"{HOME}/dags/dbt/shop")
    """
    res = run(ws, body)
    by = {l["task_id"]: l for l in res["links"]}
    assert set(by) == {"cd_py", "cd_sh", "flag_sh", "cwd", "af", "dock"}
    for task_id, link in by.items():
        assert link["status"] == "resolved", (task_id, link["unknown_reasons"])
        assert link["project_path"] == "dags/dbt/shop", task_id
        assert link["confidence"] == "heuristic", task_id
    assert any("AIRFLOW_HOME" in n for n in by["cd_py"]["notes"])


# --- what stays unknown -----------------------------------------------------------------------
@pytest.mark.parametrize(
    "expr, why",
    [
        ('os.environ["DBT_DIR"]', "no default"),
        ('os.environ["AIRFLOW_HOME"].lower() + "/dags/dbt/shop"', "no default"),
        ('os.environ["AIRFLOW_HOME"].split(",")[0] + "/dags/dbt/shop"', "no default"),
        ("f\"{os.environ['AIRFLOW_HOME']!r}/dags/dbt/shop\"", "not resolvable"),
        ("f\"{os.environ['AIRFLOW_HOME']}_suffix/dbt/shop\"", "not resolvable"),
        ("f\"{os.environ['AIRFLOW_HOME']}/dags/dbt/nowhere\"", "matches no dbt project"),
        ('os.path.basename(os.environ["AIRFLOW_HOME"]) + "/dags/dbt/shop"', "resolvable"),
        ('Path(os.environ["AIRFLOW_HOME"]).name + "/dags/dbt/shop"', "no default"),
        ('Path(os.environ["AIRFLOW_HOME"]).parent / "dags/dbt/shop"', "no default"),
        ('os.environ[KEY] + "/dags/dbt/shop"', "resolvable"),
    ],
)
def test_a_path_that_cannot_be_matched_stays_unknown(ws, expr, why):
    body = f"""
        KEY = os.getenv("K")
        with DAG("d") as dag:
            BashOperator(task_id="t", bash_command="dbt run --select stg_orders --project-dir " + {expr})
    """
    res = run(ws, body)
    assert not [l for l in res["links"] if l["status"] == "resolved"], res["links"]
    reasons = " ".join(u["reason"] for u in res["unresolved"])
    assert why in reasons, reasons


def test_an_ambiguous_tail_stays_unknown(tmp_path):
    shutil.copytree(SHOP, tmp_path / "a" / "dbt" / "shop")
    shutil.copytree(SHOP, tmp_path / "b" / "dbt" / "shop")
    (tmp_path / "dags").mkdir()
    dag = tmp_path / "dags" / "t.py"
    dag.write_text(
        HEADER
        + textwrap.dedent("""
        with DAG("d") as dag:
            BashOperator(task_id="t", bash_command=f"dbt run --project-dir {os.environ['H']}/dbt/shop")
        """)
    )
    res = scan([str(dag)], workspace=str(tmp_path))
    link = only_link(res)
    assert link["status"] == "unknown"
    assert any("ambiguous" in r for r in link["unknown_reasons"])


def test_the_variable_never_becomes_a_selector(ws):
    """A selector built on a variable with no default is never read as a literal."""
    body = """
        from cosmos import DbtDag, ProjectConfig, RenderConfig
        DbtDag(
            dag_id="c",
            project_config=ProjectConfig(dbt_project_path="/usr/local/airflow/dags/dbt/shop"),
            render_config=RenderConfig(select=[f"tag:{os.environ['TAG']}"]),
        )
        with DAG("d") as dag:
            BashOperator(task_id="t", bash_command=f"dbt run --select {os.environ['SEL']} --project-dir /x/dags/dbt/shop")
    """
    res = run(ws, body)
    assert res["links"] and all(l["status"] == "unknown" for l in res["links"]), res["links"]


# --- the pieces -------------------------------------------------------------------------------
def test_env_tail():
    assert env_tail("${AIRFLOW_HOME}/dags/dbt") == ("AIRFLOW_HOME", "dags/dbt")
    assert env_tail("$HOME/x/") == ("HOME", "x")
    assert env_tail("/opt/${A}/b/${B}/c") == ("B", "c")
    for no in ("${A}", "${A}/", "/opt/dbt", "${A}x/y", "$(pwd)/$A/x", "`pwd`/$A/x", "${A:-x}/y"):
        assert env_tail(no) is None, no


def test_project_index_matches_after_the_variable(tmp_path):
    shutil.copytree(MINI, tmp_path / "dbt" / "mini")
    index = ProjectIndex(str(tmp_path))
    found = index.match("${AIRFLOW_HOME}/dags/dbt/mini")
    assert found.path and found.conf == "heuristic" and "AIRFLOW_HOME" in (found.note or "")
    # a relative path after an env-rooted working directory joins it
    assert index.match("mini", "${X}/dbt").path == found.path
    # an env-rooted path is not joined to the working directory
    assert index.match("${X}/dbt/mini", "/somewhere/else").path == found.path
    assert index.match("${X}").path is None
    # the variable itself is never compared with a folder name
    assert index.match("${mini}/x").path is None


def test_shell_keeps_a_working_directory_after_a_variable():
    (inv,), _ = find_dbt_in_shell("cd $AIRFLOW_HOME/dbt && dbt run --select a")
    assert inv.cwd == "$AIRFLOW_HOME/dbt" and not inv.unknown_fields
    (inv,), _ = find_dbt_in_shell("cd $AIRFLOW_HOME && dbt run")
    assert inv.cwd is None and any("not known" in n for n in inv.notes)
    (inv,), _ = find_dbt_in_shell("dbt run --project-dir ${X}")
    assert "project_dir" in inv.unknown_fields
    (inv,), _ = find_dbt_in_shell("dbt run $FLAGS")
    assert "arguments" in inv.unknown_fields
    (inv,), _ = find_dbt_in_shell("dbt $CMD")
    assert inv.command == "unknown" and "dbt_command" in inv.unknown_fields


def test_plain_and_text_of():
    env = EnvPath("why", text="${X}/a", names=("X",))
    assert isinstance(plain(env), Unknown) and not isinstance(plain(env), EnvPath)
    assert plain(env).reason == "why"
    assert text_of(env) == "${X}/a" and text_of(Unknown("u")) is None
