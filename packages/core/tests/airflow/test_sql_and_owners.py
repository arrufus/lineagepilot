"""SQL operators (design section 6.2), and who a task belongs to."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from lineagepilot_core.airflow.dag_static import _tables_of, scan_dags

HEADER = """
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
"""


@pytest.fixture
def scan(tmp_path):
    def run(body):
        (tmp_path / "dags").mkdir(exist_ok=True)
        (tmp_path / "dags" / "t.py").write_text(HEADER + textwrap.dedent(body), encoding="utf-8")
        return scan_dags(str(tmp_path), ["dags"])

    return run


class TestSqlOperators:
    def test_tables_read_and_written(self, scan):
        result = scan(
            """
            with DAG("d") as d:
                SQLExecuteQueryOperator(
                    task_id="t",
                    sql="insert into marts.summary select * from marts.orders join raw.people on 1=1",
                )
            """
        )
        record = result["sql"][0]
        assert record["status"] == "resolved"
        assert record["reads"] == [[None, "marts", "orders"], [None, "raw", "people"]]
        assert record["writes"] == [[None, "marts", "summary"]]

    def test_a_list_of_statements_and_a_cte(self, scan):
        result = scan(
            """
            with DAG("d") as d:
                PostgresOperator(
                    task_id="t",
                    sql=["with x as (select * from a.b) select * from x", "delete from c.d"],
                )
            """
        )
        record = result["sql"][0]
        assert [None, "a", "b"] in record["reads"] and [None, "x"] not in record["reads"]
        assert record["writes"] == [[None, "c", "d"]]

    def test_bigquery_configuration(self, scan):
        result = scan(
            """
            with DAG("d") as d:
                BigQueryInsertJobOperator(
                    task_id="t",
                    configuration={"query": {"query": "select * from p.ds.orders"}},
                )
            """
        )
        assert result["sql"][0]["reads"] == [["p", "ds", "orders"]]

    def test_a_column_check_names_a_table(self, scan):
        result = scan(
            """
            with DAG("d") as d:
                SQLColumnCheckOperator(task_id="t", table="marts.orders", column_mapping={})
            """
        )
        assert result["sql"][0]["reads"] == [[None, "marts", "orders"]]

    def test_jinja_in_a_value_is_fine(self, scan):
        result = scan(
            """
            with DAG("d") as d:
                SQLExecuteQueryOperator(
                    task_id="t", sql="select * from marts.orders where d = '{{ ds }}'"
                )
            """
        )
        assert result["sql"][0]["reads"] == [[None, "marts", "orders"]]

    @pytest.mark.parametrize(
        ("sql", "why"),
        [
            ("select * from {{ params.table }}", "a table name is a template"),
            ("{% if x %}select 1{% endif %}", "a Jinja block"),
            ("select from where (((", "cannot be parsed"),
        ],
    )
    def test_what_cannot_be_read_is_counted_as_not_about_dbt(self, scan, sql, why):
        result = scan(
            f"""
            with DAG("d") as d:
                SQLExecuteQueryOperator(task_id="t", sql="{sql}")
            """
        )
        found = [u for u in result["unresolved"] if u["category"] == "sql"]
        assert why in found[0]["reason"] and found[0]["mentions_dbt"] is False

    def test_sql_in_a_variable_that_cannot_be_read(self, scan):
        result = scan(
            """
            with DAG("d") as d:
                SQLExecuteQueryOperator(task_id="t", sql=make_sql())
            """
        )
        assert [u for u in result["unresolved"] if u["category"] == "sql"]
        assert not result["sql"]

    def test_a_sql_file_next_to_the_dag(self, scan, tmp_path):
        (tmp_path / "dags").mkdir()
        (tmp_path / "dags" / "q.sql").write_text("select * from marts.orders")
        result = scan(
            """
            with DAG("d") as d:
                SQLExecuteQueryOperator(task_id="t", sql="q.sql")
                SQLExecuteQueryOperator(task_id="u", sql="missing.sql")
            """
        )
        assert result["sql"][0]["reads"] == [[None, "marts", "orders"]]
        assert any("missing.sql" in u["reason"] for u in result["unresolved"])

    def test_a_sql_task_in_a_loop_is_dynamic(self, scan):
        result = scan(
            """
            with DAG("d") as d:
                for q in names():
                    SQLExecuteQueryOperator(task_id=q, sql="select * from a.b")
            """
        )
        assert not result["sql"]
        assert any("a SQL task" in u["reason"] for u in result["unresolved"])

    def test_statements_with_no_tables(self):
        assert _tables_of("set search_path = x; commit", None) == ([], [])
        assert _tables_of("  ", None) == "the SQL is empty"


class TestOwners:
    def test_default_args_on_the_dag_and_owner_on_the_task(self, scan):
        result = scan(
            """
            with DAG("d", default_args={"owner": "team-a"}) as d:
                BashOperator(task_id="t", bash_command="echo")
                BashOperator(task_id="u", bash_command="echo", owner="team-b")
            """
        )
        assert result["dags"][0]["owner"] == "team-a"
        owners = {t["task_id"]: t["owner"] for t in result["tasks"]}
        assert owners == {"t": "team-a", "u": "team-b"}

    def test_default_args_from_a_variable(self, scan):
        result = scan(
            """
            ARGS = {"owner": "team-a", "retries": 1}
            with DAG("d", default_args=ARGS) as d:
                BashOperator(task_id="t", bash_command="echo")
            """
        )
        assert result["tasks"][0]["owner"] == "team-a"

    def test_task_with_dag_keyword(self, scan):
        result = scan(
            """
            d = DAG("d", default_args={"owner": "team-a"})
            BashOperator(task_id="t", bash_command="echo", dag=d)
            """
        )
        assert result["tasks"][0]["owner"] == "team-a"

    def test_decorated_dag_and_task(self, scan):
        result = scan(
            """
            from airflow.decorators import dag, task
            @dag(default_args={"owner": "team-a"})
            def my_dag():
                @task(owner="team-b")
                def one(): pass
                @task
                def two(): pass
                one()
                two()
            my_dag()
            """
        )
        assert result["dags"][0]["owner"] == "team-a"
        assert {t["task_id"]: t["owner"] for t in result["tasks"]} == {
            "one": "team-b",
            "two": "team-a",
        }

    def test_a_cosmos_group_takes_the_dag_s_owner(self, scan, tmp_path):
        (tmp_path / "p" / "models").mkdir(parents=True)
        (tmp_path / "p" / "dbt_project.yml").write_text("name: p\n")
        (tmp_path / "p" / "models" / "a.sql").write_text("select 1")
        path = Path(tmp_path / "p")
        result = scan(
            f"""
            from cosmos import DbtTaskGroup, DbtDag, ProjectConfig
            with DAG("d", default_args={{"owner": "team-a"}}) as d:
                DbtTaskGroup(group_id="g", project_config=ProjectConfig("{path.as_posix()}"))
            DbtDag(dag_id="e", project_config=ProjectConfig("{path.as_posix()}"),
                   default_args={{"owner": "team-b"}})
            """
        )
        assert {link["dag_id"]: link["owner"] for link in result["links"]} == {
            "d": "team-a",
            "e": "team-b",
        }
