"""A task created in a loop: which models it runs is only known when the file runs, so the review
says it could not tell (task 5.4)."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG("backfill", start_date=datetime(2026, 1, 1), schedule=None) as dag:
    for name in Variable.get("backfill_models"):
        BashOperator(task_id=f"backfill_{name}", bash_command=f"dbt run --select {name}")
