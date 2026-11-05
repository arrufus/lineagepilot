"""Column lineage of one SQL statement (sqlglot.lineage over a catalog schema)."""

from __future__ import annotations

import pytest
from sqlglot import exp
from sqlglot.lineage import Node

import lineagepilot_core.parsers.sql_lineage as sql_lineage
from lineagepilot_core.parsers.sql_lineage import (
    build_schema,
    sql_column_lineage,
    sqlglot_dialect,
)

T = (("db", False), ("s", False), ("t", False))
U = (("db", False), ("s", False), ("u", False))
SCHEMA = build_schema([(T, ["a", "b"]), (U, ["a", "c"])], "duckdb")


def sources(sql: str, schema=SCHEMA, dialect: str | None = "duckdb") -> dict[str, list[tuple]]:
    result = sql_column_lineage(sql, schema, dialect)
    assert not result.unknown, result.reason
    return {
        name: [(s.table.parts[-1][0], s.column, s.transformation) for s in found]
        for name, found in result.columns.items()
    }


def reason(sql: str | None, schema=SCHEMA, **kwargs) -> str:
    result = sql_column_lineage(sql, schema, "duckdb", **kwargs)
    assert result.unknown and result.columns == {}
    return result.reason


@pytest.mark.parametrize(
    ("adapter", "dialect"),
    [
        ("duckdb", "duckdb"),
        ("Snowflake", "snowflake"),
        ("sqlserver", "tsql"),
        ("materialize", "postgres"),
        ("not_an_adapter", None),
        (None, None),
    ],
)
def test_sqlglot_dialect(adapter, dialect):
    assert sqlglot_dialect(adapter) == dialect


def test_transformations():
    assert sources(
        "select a, a as renamed, a + b as total, sum(b) as summed, "
        "row_number() over (order by a) as rn from db.s.t group by a, b"
    ) == {
        "a": [("t", "a", "passthrough")],
        "renamed": [("t", "a", "passthrough")],
        "total": [("t", "a", "transform"), ("t", "b", "transform")],
        "summed": [("t", "b", "aggregate")],
        "rn": [("t", "a", "transform")],
    }


def test_star_expands_through_ctes():
    sql = """
        with base as (select * from db.s.t),
        agg as (select a, count(*) as n, max(b) as top from base group by a),
        final as (select * from agg)
        select * from final
    """
    assert sources(sql) == {
        "a": [("t", "a", "passthrough")],
        "n": [],  # depends on row counts only
        "top": [("t", "b", "aggregate")],
    }


def test_the_strongest_transformation_on_any_path_wins():
    sql = "with x as (select sum(a) as a from db.s.t) select x.a + t.a as both_ from x, db.s.t"
    assert sources(sql) == {"both_": [("t", "a", "aggregate")]}


def test_joins_and_unions():
    assert sources("select t.b, u.c from db.s.t join db.s.u on t.a = u.a") == {
        "b": [("t", "b", "passthrough")],
        "c": [("u", "c", "passthrough")],
    }
    assert sources("select a from db.s.t union all select a from db.s.u") == {
        "a": [("t", "a", "passthrough"), ("u", "a", "passthrough")]
    }


def test_constants_have_no_sources():
    assert sources("select 1 as one, 'x' as label from db.s.t") == {"one": [], "label": []}


def test_table_parts_keep_their_quoting():
    result = sql_column_lineage('select a from "db"."s"."t"', SCHEMA, "duckdb")
    (source,) = result.columns["a"]
    assert [name for name, _ in source.table.parts] == ["db", "s", "t"]


def test_quoted_column_names():
    schema = build_schema([(T, ["Order Date", "id"])], "duckdb")
    assert sources('select "Order Date" as d from db.s.t', schema) == {
        "d": [("t", "order date", "passthrough")]
    }


def test_postgres_quoted_mixed_case_tables_match_only_when_quoted():
    schema = build_schema(
        [((("Analytics", True), ("raw", False), ("orders", False)), ["id"])], "postgres"
    )
    assert sources('select id from "Analytics".raw.orders', schema, "postgres") == {
        "id": [("orders", "id", "passthrough")]
    }
    unquoted = sql_column_lineage("select id from analytics.raw.orders", schema, "postgres")
    assert unquoted.unknown and "not in the catalog" in unquoted.reason


def test_snowflake_folds_unquoted_names_to_upper():
    schema = build_schema(
        [((("ANALYTICS", False), ("RAW", False), ("ORDERS", False)), ["ID", "AMOUNT"])],
        "snowflake",
    )
    assert sources(
        "select id, amount * 2 as doubled from analytics.raw.orders", schema, "snowflake"
    ) == {
        "ID": [("ORDERS", "ID", "passthrough")],
        "DOUBLED": [("ORDERS", "AMOUNT", "transform")],
    }


def test_bigquery_backticks():
    schema = build_schema([((("proj", True), ("ds", True), ("tbl", True)), ["id"])], "bigquery")
    assert sources("select id from `proj.ds.tbl`", schema, "bigquery") == {
        "id": [("tbl", "id", "passthrough")]
    }


def test_schema_keeps_the_common_depth():
    schema = build_schema([(T, ["a"]), ((("s", False), ("v", False)), ["x"])], "duckdb")
    assert sources("select a from db.s.t", schema) == {"a": [("t", "a", "passthrough")]}
    assert "not in the catalog" in reason("select x from s.v", schema)
    assert build_schema([], "duckdb").empty
    assert build_schema([((), ["x"])], "duckdb").empty


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (None, "no compiled SQL"),
        ("   ", "no compiled SQL"),
        ("select * from db.s.missing", "not in the catalog: db.s.missing"),
        ("select a from db.s.t join db.s.nope using (a)", "not in the catalog: db.s.nope"),
        ("select * from read_csv('x.csv')", "not in the catalog: READ_CSV('x.csv')"),
        ("select from where (", "SQL could not be parsed"),
        ("insert into db.s.t select * from db.s.u", "not a query (Insert)"),
    ],
)
def test_unknown_with_a_reason(sql, expected):
    assert expected in reason(sql)


def test_too_large_is_unknown():
    assert "over the 10 limit" in reason("select a from db.s.t", max_chars=10)


def test_cte_names_are_not_tables():
    assert sources("with T as (select a from db.s.t) select a from t") == {
        "a": [("t", "a", "passthrough")]
    }


def test_lineage_failure_is_unknown(monkeypatch):
    def explode(*args, **kwargs):
        raise RecursionError("too deep")

    monkeypatch.setattr(sql_lineage, "lineage", explode)
    assert reason("select a from db.s.t") == "column lineage failed: RecursionError: too deep"


def test_unexpanded_star_is_unknown(monkeypatch):
    table = exp.to_table("db.s.t")
    star_leaf = Node(name="*", source=table, expression=table)
    parent = Node(name="a", source=table, expression=exp.column("a"), downstream=[star_leaf])
    monkeypatch.setattr(sql_lineage, "lineage", lambda *a, **k: {"a": parent})
    assert reason("select a from db.s.t") == "a * feeding column 'a' could not be expanded"
    monkeypatch.setattr(sql_lineage, "lineage", lambda *a, **k: {"*": star_leaf})
    assert reason("select a from db.s.t") == "a * could not be expanded"


def test_timing_is_recorded():
    assert sql_column_lineage("select a from db.s.t", SCHEMA, "duckdb").seconds > 0


class TestOutputColumns:
    def names(self, sql, schema=None, dialect=None):
        from lineagepilot_core.parsers.sql_lineage import sql_output_columns

        return sql_output_columns(sql, schema, dialect)

    def test_a_select_list_needs_no_schema(self):
        assert self.names("select a, b as c, 1 as d from t") == (["a", "c", "d"], None)

    def test_a_star_over_a_cte_with_a_column_list_expands_without_one(self):
        sql = "with final as (select a, b from t) select * from final"
        assert self.names(sql) == (["a", "b"], None)

    def test_a_star_over_a_table_needs_the_catalog(self):
        names, reason = self.names("select * from t")
        assert names is None and "could not be expanded" in reason
        schema = build_schema([([("t", False)], ["a", "b"])], None)
        assert self.names("select * from t", schema) == (["a", "b"], None)

    def test_a_union_takes_its_names_from_the_first_branch(self):
        assert self.names("select a from t union all select b from u") == (["a"], None)

    @pytest.mark.parametrize(
        ("sql", "reason"),
        [
            (None, "no compiled SQL"),
            ("   ", "no compiled SQL"),
            ("select from where ((", "could not be parsed"),
            ("drop table t", "not a query"),
            ("x" * 1_000_001, "over the"),
        ],
    )
    def test_what_cannot_be_read_says_why(self, sql, reason):
        names, why = self.names(sql)
        assert names is None and reason in why

    def test_a_failure_while_qualifying_is_caught(self, monkeypatch):
        def fails(*_args, **_kwargs):
            raise ValueError("boom")

        monkeypatch.setattr("lineagepilot_core.parsers.sql_lineage.qualify", fails)
        names, why = self.names("select a from t")
        assert names is None and "could not be read: ValueError: boom" in why

    def test_terminal_escapes_in_sqlglot_s_errors_do_not_get_through(self):
        _, why = self.names("select order_id from t where ((")
        assert "\x1b" not in why and "[4m" not in why
