"""Fixture for spike S3: Cosmos single operators (Dbt<Command><ExecutionMode>Operator) in an Airflow DAG."""
from pathlib import Path

from airflow import DAG
from cosmos import DbtBuildOperator, DbtDocsOperator, DbtRunLocalOperator, DbtSeedOperator, DbtTestKubernetesOperator
from cosmos.operators.local import DbtSnapshotLocalOperator

SHOP = str(Path(__file__).parent.parent / "dbt" / "shop")

with DAG(dag_id="cosmos_ops") as dag:
    run = DbtRunLocalOperator(task_id="run", project_dir=SHOP, select="+fct_orders", exclude=["stg_payments"])
    test = DbtTestKubernetesOperator(task_id="test", project_dir=SHOP, models=["dim_customers", "fct_orders"])
    seed = DbtSeedOperator(task_id="seed", project_dir=SHOP, dbt_cmd_flags=["--select", "products"])
    build = DbtBuildOperator(task_id="build", project_dir=SHOP, selector="nightly")
    docs = DbtDocsOperator(task_id="docs", project_dir=SHOP)
    snap = DbtSnapshotLocalOperator(task_id="snap", project_dir=SHOP)
    mapped = DbtRunLocalOperator.partial(task_id="mapped", project_dir=SHOP).expand(select=["stg_orders", "stg_customers"])
    seed >> run >> [test, build]
    snap.set_downstream(docs)
    mapped.set_upstream(seed)
