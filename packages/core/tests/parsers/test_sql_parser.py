"""SQL table extraction.

Ported from data-capsule ``backend/tests/unit/parsers/test_sql_parser.py``. The URN tests
went with ``match_table_to_capsule_urn``; CTE, CREATE and error tests are new.
"""

from __future__ import annotations

import pytest

from lineagepilot_core.parsers.sql_parser import TableReference, parse_sql, parse_sql_file


class TestSqlParser:
    def test_select_simple(self):
        result = parse_sql("SELECT * FROM customers")
        assert result.is_valid
        assert result.operation_type == "select"
        assert [r.table for r in result.reads] == ["customers"]
        assert result.writes == []

    def test_select_with_schema(self):
        result = parse_sql("SELECT * FROM public.customers")
        (ref,) = result.reads
        assert (ref.schema, ref.table, ref.qualified_name) == (
            "public",
            "customers",
            "public.customers",
        )

    def test_select_with_join(self):
        result = parse_sql(
            """
            SELECT c.*, o.order_id
            FROM public.customers c
            JOIN public.orders o ON c.customer_id = o.customer_id
            """
        )
        assert result.operation_type == "select"
        assert {r.table for r in result.reads} == {"customers", "orders"}
        assert {r.alias for r in result.reads} == {"c", "o"}

    def test_insert_values(self):
        result = parse_sql("INSERT INTO orders (order_id, customer_id) VALUES (1, 100)")
        assert result.operation_type == "insert"
        assert [r.table for r in result.writes] == ["orders"]
        assert result.reads == []

    def test_insert_select(self):
        result = parse_sql(
            """
            INSERT INTO public.orders_staging (order_id, customer_id)
            SELECT order_id, customer_id FROM public.orders_raw
            """
        )
        assert result.operation_type == "insert"
        assert [r.table for r in result.writes] == ["orders_staging"]
        assert [r.table for r in result.reads] == ["orders_raw"]

    def test_insert_without_column_list(self):
        result = parse_sql("INSERT INTO a SELECT * FROM b")
        assert [r.table for r in result.writes] == ["a"]
        assert [r.table for r in result.reads] == ["b"]

    def test_update(self):
        result = parse_sql("UPDATE customers SET status = 'active' WHERE customer_id = 100")
        assert result.operation_type == "update"
        assert [r.table for r in result.writes] == ["customers"]
        # UPDATE both reads and writes its target
        assert [r.table for r in result.reads] == ["customers"]

    def test_update_with_from(self):
        result = parse_sql(
            """
            UPDATE customers c
            SET status = o.status
            FROM orders o
            WHERE c.customer_id = o.customer_id
            """
        )
        assert result.operation_type == "update"
        assert [r.table for r in result.writes] == ["customers"]
        assert {r.table for r in result.reads} == {"customers", "orders"}

    def test_merge(self):
        result = parse_sql(
            """
            MERGE INTO target_table t
            USING source_table s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.value = s.value
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
            """,
            dialect="snowflake",
        )
        assert result.operation_type == "merge"
        assert [r.table for r in result.writes] == ["target_table"]
        assert [r.table for r in result.reads] == ["source_table"]

    def test_delete(self):
        result = parse_sql("DELETE FROM customers WHERE status = 'inactive'")
        assert result.operation_type == "delete"
        assert [r.table for r in result.writes] == ["customers"]
        assert [r.table for r in result.reads] == ["customers"]

    def test_create_table_as(self):
        result = parse_sql("CREATE TABLE new_table AS SELECT * FROM existing_table")
        assert result.operation_type == "create"
        assert [r.table for r in result.writes] == ["new_table"]
        assert [r.table for r in result.reads] == ["existing_table"]

    def test_create_table_with_columns_does_not_read_itself(self):
        result = parse_sql("CREATE TABLE new_table (id INT)")
        assert [r.table for r in result.writes] == ["new_table"]
        assert result.reads == []

    def test_drop(self):
        result = parse_sql("DROP TABLE old_table")
        assert result.operation_type == "drop"
        assert [r.table for r in result.writes] == ["old_table"]

    def test_drop_several(self):
        result = parse_sql("DROP TABLE IF EXISTS s.a, b")
        assert [r.qualified_name for r in result.writes] == ["s.a", "b"]

    def test_union_is_read_best_effort(self):
        result = parse_sql("SELECT a FROM x UNION ALL SELECT a FROM y")
        assert result.operation_type == "unknown"
        assert {r.table for r in result.reads} == {"x", "y"}

    def test_cte_reads_base_tables(self):
        result = parse_sql(
            """
            WITH active_customers AS (
                SELECT * FROM customers WHERE status = 'active'
            )
            SELECT c.*, o.order_id
            FROM active_customers c
            JOIN orders o ON c.customer_id = o.customer_id
            """
        )
        assert result.operation_type == "select"
        assert {r.table for r in result.reads} == {"customers", "orders"}

    def test_cte_name_is_not_a_table_but_a_qualified_namesake_is(self):
        result = parse_sql(
            "WITH orders AS (SELECT * FROM raw.orders) SELECT * FROM orders JOIN ORDERS USING (id)"
        )
        assert [r.qualified_name for r in result.reads] == ["raw.orders"]

    def test_insert_from_cte(self):
        result = parse_sql("INSERT INTO t WITH s AS (SELECT * FROM src) SELECT * FROM s")
        assert [r.table for r in result.writes] == ["t"]
        assert [r.table for r in result.reads] == ["src"]

    def test_snowflake_three_part_name(self):
        result = parse_sql(
            """
            SELECT *
            FROM analytics.raw.customers
            WHERE loaded_at > CURRENT_TIMESTAMP() - INTERVAL '7 DAYS'
            """,
            dialect="snowflake",
        )
        (ref,) = result.reads
        assert (ref.database, ref.schema, ref.table) == ("analytics", "raw", "customers")
        assert ref.qualified_name == "analytics.raw.customers"

    def test_absent_parts_are_none(self):
        (ref,) = parse_sql("SELECT * FROM customers").reads
        assert (ref.database, ref.schema, ref.alias) == (None, None, None)

    def test_duplicates_removed(self):
        result = parse_sql(
            """
            SELECT c1.*, c2.*
            FROM customers c1
            JOIN customers c2 ON c1.parent_id = c2.customer_id
            """
        )
        assert [r.table for r in result.reads] == ["customers"]

    def test_invalid_sql(self):
        result = parse_sql("SELECT * FORM customers")  # FORM, not FROM
        assert not result.is_valid
        assert result.errors

    def test_tokenizer_error_is_a_parse_error(self):
        result = parse_sql("SELECT 'unterminated")
        assert not result.is_valid
        assert result.errors[0].startswith("SQL parse error")

    @pytest.mark.parametrize("sql", ["", "   \n"])
    def test_empty_sql(self, sql):
        result = parse_sql(sql)
        assert not result.is_valid
        assert result.errors == ["Empty SQL statement"]

    def test_unexpected_errors_are_contained(self, monkeypatch):
        import lineagepilot_core.parsers.sql_parser as sql_parser

        def explode(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(sql_parser, "parse_one", explode)
        result = sql_parser.parse_sql("SELECT 1")
        assert not result.is_valid
        assert result.errors == ["Unexpected error: boom"]


class TestParseSqlFile:
    def test_reads_the_file(self, tmp_path):
        path = tmp_path / "q.sql"
        path.write_text("SELECT * FROM customers", encoding="utf-8")
        assert [r.table for r in parse_sql_file(str(path)).reads] == ["customers"]

    def test_missing_file(self, tmp_path):
        result = parse_sql_file(str(tmp_path / "missing.sql"))
        assert not result.is_valid
        assert result.errors[0].startswith("File not found")

    def test_unreadable_file(self, tmp_path):
        path = tmp_path / "bad.sql"
        path.write_bytes(b"\xff\xfe\x00not utf-8")
        result = parse_sql_file(str(path))
        assert not result.is_valid
        assert result.errors[0].startswith("Error reading file")


class TestTableReference:
    def test_qualified_name_full(self):
        ref = TableReference(database="analytics", schema="raw", table="customers")
        assert ref.qualified_name == "analytics.raw.customers"

    def test_qualified_name_schema_only(self):
        assert TableReference(schema="public", table="customers").qualified_name == (
            "public.customers"
        )

    def test_qualified_name_table_only(self):
        assert TableReference(schema=None, table="customers").qualified_name == "customers"
