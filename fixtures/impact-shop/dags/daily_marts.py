"""The acceptance scenario's DAG (design section 14): a Cosmos task group that consumes the mart.

Read by the scanner as syntax only. Nothing here is ever imported, so airflow and cosmos need not
be installed.
"""

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig

PROJECT = Path(__file__).parent.parent

with DAG(
    "daily_marts",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    default_args={"owner": "data-platform"},
) as dag:
    marts = DbtTaskGroup(
        group_id="marts",
        project_config=ProjectConfig(PROJECT),
        render_config=RenderConfig(select=["orders+"], exclude=["broken_report"]),
    )
    notify = BashOperator(task_id="notify_finance", bash_command="echo marts are ready")

    marts >> notify
