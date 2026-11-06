"""Value merging rules, and the airflow-dbt / airflow-dbt-python operator family."""

from __future__ import annotations

import shutil
import textwrap

from airflow_support import SHOP, scan

from lineagepilot_core.airflow.values import (
    CallV,
    Const,
    DictV,
    ListV,
    PathV,
    Sym,
    TaskV,
    Unknown,
    as_str_list,
    collect_meta,
    merge_values,
    same_value,
    weakest,
)


def test_weakest_and_meta():
    assert weakest() == "declared"
    assert weakest("declared", "heuristic", "inferred") == "heuristic"
    v = ListV(
        [Const("a", conf="inferred", notes=("n1",)), DictV({"k": PathV("/p", conf="heuristic")})],
        notes=("n0",),
    )
    conf, notes = collect_meta(
        v, CallV("f", [Const(1)], {"x": Const(2, notes=("n2",))}), Unknown("u"), None
    )
    assert conf == "heuristic" and notes == ("n0", "n1", "n2")
    assert as_str_list(Const(None)) == [] and as_str_list(ListV([Const("a"), Const(1)])) is None
    assert as_str_list(Sym("x")) is None


def test_merge_values():
    assert same_value(Const(1), Const(1)) and not same_value(Const(1), PathV("1"))
    assert same_value(PathV("/a"), PathV("/a")) and same_value(Sym("a"), Sym("a"))
    assert same_value(ListV([Const(1)]), ListV([Const(1)])) and not same_value(
        ListV([]), ListV([Const(1)])
    )
    assert same_value(DictV({"a": Const(1)}), DictV({"a": Const(1)}))
    t = TaskV(None)
    assert same_value(t, t) and not same_value(TaskV(None), TaskV(None))
    assert not same_value(Unknown("a"), Unknown("a"))
    m = merge_values(
        DictV({"a": Const(1), "b": Const(2)}), DictV({"a": Const(1), "c": Const(3)}), "if"
    )
    assert isinstance(m, DictV) and m.items["a"].value == 1 and isinstance(m.items["b"], Unknown)
    assert isinstance(merge_values(Const(1), Const(2), "if"), Unknown)
    assert isinstance(
        merge_values(DictV({"a": Const(1)}), DictV({"a": Const(2)}), "if").items["a"], Unknown
    )


def test_airflow_dbt_operator_families(tmp_path):
    shutil.copytree(SHOP, tmp_path / "dbt" / "shop")
    dag = tmp_path / "dags" / "d.py"
    dag.parent.mkdir()
    dag.write_text(
        textwrap.dedent("""
        from airflow import DAG
        from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator
        from airflow_dbt_python.operators.dbt import DbtBuildOperator
        P = "/usr/local/airflow/dbt/shop"
        with DAG("af") as dag:
            DbtRunOperator(task_id="r", dir=P, models="stg_orders stg_customers", exclude="stg_customers")
            DbtTestOperator(task_id="t", dir=P, select=SOMETHING)
            DbtBuildOperator(task_id="b", project_dir=P, select=["+stg_orders"])
    """)
    )
    res = scan([str(dag)], workspace=str(tmp_path))
    by = {l["task_id"]: l for l in res["links"]}
    assert by["r"]["kind"] == "airflow_dbt_operator" and by["r"]["resolved_models"] == [
        "model.shop.stg_orders"
    ]
    assert by["t"]["status"] == "unknown"
    assert by["b"]["dbt_command"] == "build" and by["b"]["resolved_models"] == [
        "model.shop.stg_orders"
    ]
