"""Fixture for spike S3: dynamic Airflow DAG patterns that must come out as unknown, never guessed."""
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig

MODELS = ["stg_orders", "stg_customers"]
IMAGE_SHOP = "/usr/local/airflow/dbt/shop"

with DAG(dag_id="loop_dag") as dag:
    for m in MODELS:
        BashOperator(task_id=f"run_{m}", bash_command=f"dbt run --select {m} --project-dir {IMAGE_SHOP}")
    comp = [BashOperator(task_id=f"c_{m}", bash_command="dbt run") for m in MODELS]
    with open("/opt/dbt/target/manifest.json") as fh:
        manifest = json.load(fh)


def make_dag(name, select):
    with DAG(dag_id=name) as d:
        DbtTaskGroup(group_id="g", project_config=ProjectConfig(IMAGE_SHOP), render_config=RenderConfig(select=[select]))
    return d


for cfg in [{"name": "a", "select": "stg_orders"}]:
    globals()[cfg["name"]] = make_dag(cfg["name"], cfg["select"])


def never_called(path):
    return BashOperator(task_id="never", bash_command=f"dbt run --project-dir {path}")


def helper(tid, sel, project=IMAGE_SHOP):
    """Called with literals below: inlined at each call site."""
    return BashOperator(task_id=tid, bash_command=f"dbt run --select {sel} --project-dir {project}")


with DAG(dag_id="helper_dag") as hd:
    a = helper("h1", "stg_orders")
    b = helper("h2", sel="stg_payments")
    a >> b
    unknown_dep = some_unknown_thing()  # noqa: F821
    a >> unknown_dep

if __name__ == "__main__":
    hd.test()
