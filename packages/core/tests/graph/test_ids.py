"""Canonical IDs (design section 5.1)."""

from __future__ import annotations

import pytest

from lineagepilot_core.graph.ids import (
    airflow_dag_id,
    airflow_task_id,
    column_id,
    normalise_identifier,
    relation_id,
    split_column_id,
    split_relation_name,
)


@pytest.mark.parametrize(
    ("name", "adapter", "quoted", "expected"),
    [
        ("Orders", "snowflake", False, "ORDERS"),
        ("Orders", "snowflake", True, "Orders"),
        ("Orders", "SNOWFLAKE", False, "ORDERS"),
        ("Orders", "postgres", False, "orders"),
        ("Orders", "postgres", True, "orders"),
        ("Orders", "duckdb", False, "orders"),
        ("Orders", "bigquery", True, "orders"),
        ("Orders", None, False, "orders"),
    ],
)
def test_normalise_identifier(name, adapter, quoted, expected):
    assert normalise_identifier(name, adapter, quoted) == expected


@pytest.mark.parametrize(
    ("relation_name", "parts"),
    [
        (
            '"pilot"."raw"."raw_customers"',
            [("pilot", True), ("raw", True), ("raw_customers", True)],
        ),
        ("analytics.dbt.customers", [("analytics", False), ("dbt", False), ("customers", False)]),
        ("`proj`.`ds`.`tbl`", [("proj", True), ("ds", True), ("tbl", True)]),
        ("`proj.ds.tbl`", [("proj", True), ("ds", True), ("tbl", True)]),
        ("[db].[dbo].[orders]", [("db", True), ("dbo", True), ("orders", True)]),
        ('"we""ird".t', [('we"ird', True), ("t", False)]),
        ('"a.b".c', [("a.b", True), ("c", False)]),
        ("schema.table", [("schema", False), ("table", False)]),
        ("  analytics.orders  ", [("analytics", False), ("orders", False)]),
        ("a.b c", [("a", False), ("b c", False)]),
    ],
)
def test_split_relation_name(relation_name, parts):
    assert split_relation_name(relation_name) == parts


@pytest.mark.parametrize(
    "bad", ["", "a..b", "a.", ".a", '"unterminated.b', 'a"b.c', "``", "`proj..tbl`", '""']
)
def test_split_relation_name_rejects(bad):
    assert split_relation_name(bad) is None


def test_relation_id_reconciles_spellings():
    """Spellings of one table get one ID (review finding F09)."""
    pg = {
        relation_id(n, "postgres") for n in ('"Analytics"."Raw"."Orders"', "analytics.raw.orders")
    }
    assert pg == {"relation:analytics.raw.orders"}
    sf = {
        relation_id(n, "snowflake") for n in ("analytics.raw.orders", '"ANALYTICS"."RAW"."ORDERS"')
    }
    assert sf == {"relation:ANALYTICS.RAW.ORDERS"}
    # A quoted lower-case Snowflake name is a different table
    assert relation_id('"analytics".raw.orders', "snowflake") == "relation:analytics.RAW.ORDERS"


def test_relation_id_of_nothing():
    assert relation_id(None, "duckdb") is None
    assert relation_id("", "duckdb") is None
    assert relation_id("a..b", "duckdb") is None


def test_column_ids():
    assert column_id("model.p.orders", "Order_ID", "duckdb") == "model.p.orders#order_id"
    assert column_id("model.p.orders", "order_id", "snowflake") == "model.p.orders#ORDER_ID"
    assert split_column_id("model.p.orders#order_id") == ("model.p.orders", "order_id")
    assert split_column_id("model.p.orders#COL#1") == ("model.p.orders", "COL#1")
    for bad in ("model.p.orders", "#x"):
        with pytest.raises(ValueError):
            split_column_id(bad)


def test_airflow_ids():
    assert airflow_dag_id("daily") == "airflow:daily"
    assert airflow_task_id("daily", "group.run_dbt") == "airflow:daily.group.run_dbt"
