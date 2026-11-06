"""dbt from a shell, and a SQL task that reads a mart: each is linked to what it touches."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG("operations", start_date=datetime(2026, 1, 1), schedule="@hourly") as dag:
    counts = BashOperator(
        task_id="refresh_counts",
        bash_command="dbt run --select order_counts",
        owner="operations-eng",
    )
    export = SQLExecuteQueryOperator(
        task_id="export_orders",
        conn_id="warehouse",
        sql="select order_id, customer_id from main.orders",
        owner="operations-eng",
    )

    counts >> export
