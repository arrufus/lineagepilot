"""Fixture for spike S3: an Airflow DAG whose values come from other workspace modules."""
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, RenderConfig
from include import constants
from include.constants import EXEC, SELECT, SHOP_PATH, helper
from include.dbt_wrapper import DbtWrapper

from .local_consts import REL_SELECT

imported = DbtDag(
    dag_id="cross_module",
    project_config=ProjectConfig(SHOP_PATH),
    render_config=RenderConfig(select=SELECT),
    execution_config=EXEC,
)
via_module = DbtDag(dag_id="cross_module_attr", project_config=ProjectConfig(constants.SHOP_PATH),
                    render_config=RenderConfig(select=[REL_SELECT]))
with DAG(dag_id="wrapper_dag") as dag:
    DbtWrapper(dbt_project="/usr/local/airflow/dbt/shop")
    helper()
    some_lib_call(dbt_models=["x"])  # noqa: F821
