"""The static evaluator: constant resolution and the Unknown cases, on small DAG sources written here."""

from __future__ import annotations

import shutil
import sys
import textwrap

import pytest
from airflow_support import SHOP, scan

HEADER = "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\nimport os\nfrom pathlib import Path\n"


@pytest.fixture(scope="module")
def ws(tmp_path_factory):
    root = tmp_path_factory.mktemp("ws")
    shutil.copytree(SHOP, root / "dbt" / "shop")
    (root / "dags").mkdir()
    return root


def run(ws, body, name="t.py"):
    path = ws / "dags" / name
    path.write_text(HEADER + textwrap.dedent(body))
    return scan([str(path)], workspace=str(ws))


def cmd_link(res, task_id="t"):
    return next(l for l in res["links"] if l["task_id"] == task_id)


@pytest.mark.parametrize(
    "expr, models",
    [
        ('f"dbt run --select {SEL} --project-dir {P}"', ["stg_orders"]),
        ('"dbt run --select %s --project-dir %s" % (SEL, P)', ["stg_orders"]),
        ('"dbt run --select {} --project-dir {}".format(SEL, P)', ["stg_orders"]),
        ('"dbt run --select {s} --project-dir {p}".format(s=SEL, p=P)', ["stg_orders"]),
        ('" ".join(["dbt", "run", "--select", SEL, "--project-dir", P])', ["stg_orders"]),
        (
            '"dbt run --select X --project-dir {P}".replace("X", SEL).replace("{P}", P)',
            ["stg_orders"],
        ),
        ('"  dbt run --select stg_orders --project-dir " + P.strip()', ["stg_orders"]),
        ('"dbt run --select " + SELS[0] + " --project-dir " + str(PP)', ["stg_orders"]),
        (
            '"dbt run --select " + CONF["sel"] + " --project-dir " + os.path.join(ROOT, "shop")',
            ["stg_orders"],
        ),
        (
            '"dbt run --select " + CONF.get("sel") + " --project-dir " + str(Path(ROOT).joinpath("shop"))',
            ["stg_orders"],
        ),
        ('"dbt run --select " + (SEL if True else "x") + " --project-dir " + P', ["stg_orders"]),
        ('"dbt run --select " + ALIASED + " --project-dir " + P', ["stg_payments"]),
        (
            '"dbt run --select " + (os.getenv("NOPE") or "stg_orders") + " --project-dir " + P',
            ["stg_orders"],
        ),
        (
            '"dbt run --select " + Variable.get("v", default_var="stg_orders") + " --project-dir " + P',
            ["stg_orders"],
        ),
        ('"dbt run --select " + MERGED["b"] + " --project-dir " + P', ["stg_orders"]),
        ('"dbt run --select " + A + " --project-dir " + P', ["stg_orders"]),
        ('"dbt run --select " + TRY + " --project-dir " + P', ["stg_orders"]),
        ('"dbt run --select " + AUG + " --project-dir " + P', ["stg_orders"]),
        ('"dbt run --select " + (w := "stg_orders") + " --project-dir " + P', ["stg_orders"]),
        ('"dbt run --select " + str(PP.parent.name) + " --project-dir " + P', []),
    ],
)
def test_resolvable_expressions(ws, expr, models):
    body = f"""
        from airflow.models import Variable
        ROOT = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "dbt")
        P = os.path.normpath(os.path.join(ROOT, "shop"))
        PP = Path(__file__).resolve().parents[1] / "dbt" / "shop"
        SEL = "stg_orders"
        SELS = [SEL, "x"]
        CONF = {{"sel": SEL}}
        MERGED = {{**CONF, "b": SEL}}
        ALIASED = SEL.replace("orders", "payments")
        A, B = SEL, "x"
        try:
            TRY = SEL
        except ImportError:
            TRY = SEL
        AUG = "stg_"
        AUG += "orders"
        with DAG("d") as dag:
            BashOperator(task_id="t", bash_command={expr})
    """
    link = cmd_link(run(ws, body))
    assert link["status"] == "resolved", link["unknown_reasons"]
    assert sorted(m.split(".")[-1] for m in link["resolved_models"]) == models


@pytest.mark.parametrize(
    "expr, why",
    [
        ('f"dbt run --select {SEL:>10}"', "format spec"),
        ('"dbt run --select " + os.environ["X"]', "os.environ"),
        ('"dbt run --select " + os.getenv("X")', "no default"),
        ('"dbt run --select " + Variable.get("v")', "Airflow Variable"),
        ('"dbt run --select " + (SEL if os.getenv("C") else "other")', "conditional"),
        ('"dbt run --select " + BRANCHY', "branches"),
        ('"dbt run --select " + LOOPED', "loop"),
        ('"dbt run --select " + some_call()', "some_call"),
        ('"dbt run --select " + CONF["missing"]', "not in dict"),
        ('"dbt run --select " + SELS[9]', "out of range"),
        ('"dbt run %s" % os.getenv("X")', "non-literal"),
        ('"dbt run {}".format(os.getenv("X"))', "non-literal"),
        ('"dbt run " + str(Path.cwd())', "runtime environment"),
        ('"dbt run " + os.path.abspath("rel")', "working directory"),
        ('"dbt run " + str(Path())', "working directory"),
        ('"dbt run " + str(lambda: 1)', "lambda"),
        ('"dbt run " + " ".join([x for x in SELS])', "comprehension"),
        ('"dbt run " + str(UNPACKED)', "tuple unpacking"),
    ],
)
def test_unresolvable_expressions(ws, expr, why):
    body = f"""
        from airflow.models import Variable
        SEL = "stg_orders"
        SELS = [SEL]
        CONF = {{"sel": SEL}}
        if os.getenv("FLAG"):
            BRANCHY = "a"
        else:
            BRANCHY = "b"
        for i in SELS:
            LOOPED = i
        UNPACKED, OTHER = some_call()
        with DAG("d") as dag:
            BashOperator(task_id="t", bash_command={expr})
    """
    res = run(ws, body)
    assert not res["links"], res["links"]
    reasons = " ".join(u["reason"] for u in res["unresolved"])
    assert why in reasons, reasons


def test_task_groups_contexts_and_misc_statements(ws):
    body = """
        from airflow.utils.task_group import TaskGroup
        from airflow.decorators import task_group
        P = str(Path(__file__).parent.parent / "dbt" / "shop")

        @task_group(group_id="tg")
        def grouped(sel):
            BashOperator(task_id="inner", bash_command=f"dbt run --select {sel} --project-dir {P}")

        class Ignored:
            def method(self):
                BashOperator(task_id="m", bash_command=f"dbt run --project-dir {self.p}")

        with DAG(dag_id="ctx") as dag:
            with TaskGroup("a") as a, TaskGroup(group_id="b", prefix_group_id=False):
                BashOperator(task_id="t", bash_command="dbt run --select stg_orders --project-dir " + P)
            grouped("stg_customers")
            n = 0
            while n < 2:
                BashOperator(task_id="w", bash_command="dbt run")
                n += 1
            assert P
            del n
        DAG(dag_id=os.getenv("NO_ID"))
    """
    res = run(ws, body)
    t = next(l for l in res["links"] if l["task_id"] == "a.t")
    assert t["status"] == "resolved"
    inner = next(l for l in res["links"] if l["task_id"] == "tg.inner")
    assert inner["resolved_models"] == ["model.shop.stg_customers"]
    reasons = " ".join(u["reason"] for u in res["unresolved"])
    assert "while-loop" in reasons and "never called" in reasons and "dag_id" in reasons


@pytest.mark.skipif(sys.version_info < (3, 10), reason="match needs 3.10")
def test_match_statement_branches(ws):
    body = """
        P = str(Path(__file__).parent.parent / "dbt" / "shop")
        match os.getenv("ENV"):
            case "prod":
                SEL = "stg_orders"
            case _:
                SEL = "stg_orders"
        with DAG("m") as dag:
            BashOperator(task_id="t", bash_command=f"dbt run --select {SEL} --project-dir {P}")
    """
    assert cmd_link(run(ws, body))["resolved_models"] == ["model.shop.stg_orders"]


def test_recursion_and_generators_are_not_followed(ws):
    body = """
        def rec(n):
            return rec(n)

        def gen():
            yield "x"

        def many(*args, **kwargs):
            return BashOperator(task_id=kwargs["tid"], bash_command="dbt run --select " + args[0])

        with DAG("r") as dag:
            rec(1)
            gen()
            many("stg_orders", tid="t")
            many(*SOMETHING)
    """
    res = run(ws, body)
    link = cmd_link(res)
    assert link["select"] == ["stg_orders"]
