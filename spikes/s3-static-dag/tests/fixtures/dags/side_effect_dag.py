"""Fixture for spike S3: an Airflow DAG file with a visible side effect on import.

If anything ever imports or executes this file, it writes a marker file and exits.
The marker is written before any other import, so the side effect fires even where
Airflow is not installed. The scanner must still find the DAG below: it only parses the AST.
"""
import os
import sys

MARKER = os.environ.get("LP_SIDE_EFFECT_MARKER", "/tmp/lineagepilot_side_effect_marker")
with open(MARKER, "w") as fh:
    fh.write("this DAG file was imported or executed")
sys.exit("this DAG file must never be executed")

from airflow import DAG  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402

with DAG(dag_id="side_effect") as dag:
    BashOperator(task_id="t", bash_command="dbt run --select stg_orders --project-dir /usr/local/airflow/dbt/shop")
