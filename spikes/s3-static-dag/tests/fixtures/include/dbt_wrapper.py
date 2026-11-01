"""A workspace wrapper that builds dbt tasks at DAG-parse time (read as AST only)."""
from airflow.operators.bash import BashOperator


class DbtWrapper:
    def __init__(self, dbt_project):
        for model in ["a", "b"]:
            BashOperator(task_id=model, bash_command=f"dbt run --select {model} --project-dir {dbt_project}")
