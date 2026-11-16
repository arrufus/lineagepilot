"""A helper imported from another workspace file is followed one level (LP-z2x, task E.2 of the
next phase's plan).

A local helper's body was always run by the evaluator; one imported from another file was a named
unknown. It is now run the same way, as syntax, with its own module's names: what it creates is
reported at the call in the DAG file, and where it was built is kept. A helper two levels deep, an
import that leaves the workspace, and a class stay named unknowns. Nothing is ever executed.
"""

from __future__ import annotations

import os
import shutil
import sys
import textwrap

import pytest
from airflow_support import SHOP, scan

needs_symlinks = pytest.mark.skipif(
    sys.platform == "win32", reason="creating symlinks needs Developer Mode on Windows"
)

HELPERS = """\
import os
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

PROJECT = "/usr/local/airflow/dags/dbt/shop"
MODE = os.getenv("DBT_MODE", "bash")


def _command(select):
    return f"dbt run --select {select} --project-dir {PROJECT}"


def build_dbt_task(task_id, select, dag=None):
    if MODE == "docker":
        return DockerOperator(task_id=task_id, image="x", command=_command(select), dag=dag)
    return BashOperator(task_id=task_id, bash_command=_command(select), dag=dag)
"""


def write(root, rel, text):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path


@pytest.fixture
def ws(tmp_path):
    shutil.copytree(SHOP, tmp_path / "dags" / "dbt" / "shop")
    write(tmp_path, "dags/dbt_utils.py", HELPERS)
    return tmp_path


def scan_dag(ws, body, name="dags/pipeline.py"):
    dag = write(ws, name, body)
    return scan([str(dag)], workspace=str(ws))


def models(link):
    return sorted(m.split(".")[-1] for m in link["resolved_models"])


@pytest.mark.parametrize(
    "imports, call",
    [
        ("from dbt_utils import build_dbt_task", "build_dbt_task"),
        ("import dbt_utils", "dbt_utils.build_dbt_task"),
        ("from dags import dbt_utils", "dbt_utils.build_dbt_task"),
        ("from . import dbt_utils", "dbt_utils.build_dbt_task"),
        ("from .dbt_utils import build_dbt_task as make", "make"),
    ],
)
def test_an_imported_helper_links_at_the_call_in_the_dag_file(ws, imports, call):
    write(ws, "dags/__init__.py", "")
    res = scan_dag(
        ws,
        f"""
        from airflow import DAG
        {imports}

        with DAG("pipeline") as dag:
            orders = {call}("orders", "stg_orders")
            customers = {call}("customers", "stg_customers")
            orders >> customers
        """,
    )
    by = {l["task_id"]: l for l in res["links"]}
    assert set(by) == {"orders", "customers"}, res["unresolved"]
    orders = by["orders"]
    assert orders["status"] == "resolved", orders["unknown_reasons"]
    assert models(orders) == ["stg_orders"] and models(by["customers"]) == ["stg_customers"]
    assert orders["project_path"] == "dags/dbt/shop"
    # reported at the call in the DAG file, built in the helper's
    assert orders["location"] == {"file": "dags/pipeline.py", "line": 6, "col": 13}
    assert orders["constructed_at"]["file"] == "dags/dbt_utils.py"
    assert orders["constructed_at"]["line"] == 16
    assert any("build_dbt_task() in dags/dbt_utils.py" in n for n in orders["notes"])
    tasks = {t["task_id"]: t for t in res["tasks"]}
    assert tasks["orders"]["location"]["file"] == "dags/pipeline.py"
    assert tasks["orders"]["location"]["line"] == 6
    assert any(
        d["upstream"] == "airflow:pipeline.orders"
        and d["downstream"] == "airflow:pipeline.customers"
        for d in res["dependencies"]
    )


def test_the_helper_reads_its_own_module_and_its_relative_imports(ws):
    write(ws, "dags/lib/__init__.py", "")
    write(ws, "dags/lib/consts.py", 'SELECT = "stg_payments"\n')
    write(
        ws,
        "dags/lib/factory.py",
        """
        from airflow.operators.bash import BashOperator
        from .consts import SELECT
        HERE = "/opt/airflow/dags/dbt/shop"

        def make(task_id):
            return BashOperator(task_id=task_id, bash_command=f"dbt run -s {SELECT} --project-dir {HERE}")
        """,
    )
    res = scan_dag(
        ws,
        """
        from airflow import DAG
        from lib.factory import make
        SELECT = "not_this_one"
        HERE = "/nowhere"
        with DAG("d") as dag:
            make("t")
        """,
    )
    (link,) = res["links"]
    assert link["status"] == "resolved", link["unknown_reasons"]
    assert models(link) == ["stg_payments"]


def test_a_helper_two_levels_deep_is_a_named_unknown(ws):
    write(
        ws,
        "dags/outer.py",
        """
        from dbt_utils import build_dbt_task

        def outer(task_id):
            return build_dbt_task(task_id, "stg_orders")
        """,
    )
    res = scan_dag(
        ws,
        """
        from airflow import DAG
        from outer import outer
        with DAG("d") as dag:
            outer("t")
        """,
    )
    assert not res["links"]
    (u,) = [u for u in res["unresolved"] if u["file"] == "dags/pipeline.py"]
    assert u["line"] == 5 and u["category"] == "wrapper"
    assert "dags/dbt_utils.py" in u["reason"] and "in outer() in dags/outer.py" in u["reason"]


@needs_symlinks
def test_an_import_that_leaves_the_workspace_is_not_followed(tmp_path):
    outside = tmp_path / "outside"
    ws = tmp_path / "ws"
    shutil.copytree(SHOP, ws / "dags" / "dbt" / "shop")
    write(outside, "dbt_utils.py", HELPERS)
    os.symlink(outside / "dbt_utils.py", ws / "dags" / "dbt_utils.py")
    res = scan_dag(
        ws,
        """
        from airflow import DAG
        from dbt_utils import build_dbt_task
        with DAG("d") as dag:
            build_dbt_task("t", "stg_orders")
        """,
    )
    assert not res["links"]
    (u,) = [u for u in res["unresolved"] if u["file"] == "dags/pipeline.py"]
    assert u["category"] == "wrapper" and "build_dbt_task" in u["reason"]


def test_a_class_is_not_followed(ws):
    write(
        ws,
        "dags/wrapper.py",
        """
        from airflow.operators.bash import BashOperator
        class DbtWrapper:
            def __init__(self, select):
                BashOperator(task_id="t", bash_command=f"dbt run -s {select}")
        """,
    )
    res = scan_dag(
        ws,
        """
        from airflow import DAG
        from wrapper import DbtWrapper
        with DAG("d") as dag:
            DbtWrapper("stg_orders")
        """,
    )
    assert not res["links"]
    assert any(u["category"] == "wrapper" for u in res["unresolved"])


def test_unknowns_inside_the_helper_are_reported_at_the_call(ws):
    write(
        ws,
        "dags/dyn.py",
        """
        import os
        from airflow.operators.bash import BashOperator
        def make(task_id):
            return BashOperator(task_id=task_id, bash_command=os.environ["CMD"])
        """,
    )
    res = scan_dag(
        ws,
        """
        from airflow import DAG
        from dyn import make
        with DAG("d") as dag:
            make("dbt_t")
        """,
    )
    (u,) = [u for u in res["unresolved"] if "CMD" in u["reason"]]
    assert u["file"] == "dags/pipeline.py" and u["line"] == 5
    assert "in make() in dags/dyn.py, line 5" in u["reason"]


def test_a_helper_module_is_never_executed(ws, tmp_path, monkeypatch):
    marker = tmp_path / "marker"
    monkeypatch.setenv("LP_SIDE_EFFECT_MARKER", str(marker))
    write(
        ws,
        "dags/hostile.py",
        """
        import os
        from airflow.operators.bash import BashOperator
        open(os.environ["LP_SIDE_EFFECT_MARKER"], "w").write("imported")

        def make(task_id):
            open(os.environ["LP_SIDE_EFFECT_MARKER"], "w").write("called")
            return BashOperator(task_id=task_id, bash_command="dbt run -s stg_orders --project-dir /x/dbt/shop")
        """,
    )
    res = scan_dag(
        ws,
        """
        from airflow import DAG
        from hostile import make
        with DAG("d") as dag:
            make("t")
        """,
    )
    assert not marker.exists()
    assert "hostile" not in sys.modules
    (link,) = res["links"]
    assert link["status"] == "resolved" and models(link) == ["stg_orders"]


def test_a_sql_template_is_found_next_to_the_dag_file(ws):
    write(ws, "dags/q.sql", "select * from orders")
    write(
        ws,
        "dags/sqlhelp.py",
        """
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
        def query(task_id):
            return SQLExecuteQueryOperator(task_id=task_id, sql="q.sql")
        """,
    )
    res = scan_dag(
        ws,
        """
        from airflow import DAG
        from sqlhelp import query
        with DAG("d") as dag:
            query("q")
        """,
    )
    (sql,) = res["sql"]
    assert sql["location"]["file"] == "dags/pipeline.py" and sql["location"]["line"] == 5
    assert [r[-1] for r in sql["reads"]] == ["orders"]
