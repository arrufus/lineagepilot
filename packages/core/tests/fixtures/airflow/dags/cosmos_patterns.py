"""Fixture for spike S3: Astronomer Cosmos patterns. An Airflow DAG file that is parsed, never run."""
import os
from pathlib import Path

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from cosmos import DbtDag, DbtTaskGroup, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, SeedRenderingBehavior, TestBehavior

ROOT = Path(__file__).parent.parent / "dbt"
SHOP = ROOT / "shop"
SHOP_ENV = Path(os.getenv("SHOP_DIR", str(SHOP)))

# positional dag_id and positional ProjectConfig path
positional = DbtDag(
    "cosmos_positional",
    project_config=ProjectConfig(SHOP),
    render_config=RenderConfig(select=["tag:finance"], exclude=["fct_revenue"]),
)

# keyword dag_id, env-var default (heuristic), YAML selector, TestBehavior.BUILD
build_dag = DbtDag(
    dag_id="cosmos_build",
    project_config=ProjectConfig(dbt_project_path=SHOP_ENV),
    render_config=RenderConfig(selector="finance_and_children", test_behavior=TestBehavior.BUILD),
)

# manifest_path + project_name, AFTER_ALL tests, watcher execution
manifest_dag = DbtDag(
    dag_id="cosmos_manifest",
    project_config=ProjectConfig(manifest_path=SHOP / "target" / "manifest.json", project_name="shop"),
    render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST, select=["path:models/staging"],
                               test_behavior=TestBehavior.AFTER_ALL),
    execution_config=ExecutionConfig(execution_mode=ExecutionMode.WATCHER, dbt_project_path="/opt/shop"),
)

with DAG("cosmos_groups") as dag:
    with TaskGroup(group_id="outer"):
        inner = DbtTaskGroup(
            group_id="inner",
            project_config=ProjectConfig(SHOP),
            render_config=RenderConfig(select=["stg_customers", "stg_orders"]),
        )
    default_group = DbtTaskGroup(
        project_config=ProjectConfig(),
        render_config=RenderConfig(dbt_project_path=SHOP, select=["@stg_products"], group_nodes_by_folder=True,
                                   seed_rendering_behavior=SeedRenderingBehavior.NONE),
    )
    none_tests = DbtTaskGroup("none_tests", project_config=ProjectConfig(SHOP),
                              render_config=RenderConfig(select=["int_order_items"], test_behavior=TestBehavior.NONE))
    normalized = DbtTaskGroup(group_id="normalized", project_config=ProjectConfig(SHOP),
                              render_config=RenderConfig(normalize_task_id=lambda node: node.name))
    remote = DbtTaskGroup(group_id="remote",
                          project_config=ProjectConfig(manifest_path="s3://bucket/manifest.json", project_name="shop"),
                          render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST))
    unsupported = DbtTaskGroup(group_id="unsupported", project_config=ProjectConfig(SHOP),
                               render_config=RenderConfig(select=["state:modified+"]))
    templated = DbtTaskGroup(group_id="templated", project_config=ProjectConfig(SHOP),
                             render_config=RenderConfig(select=["{{ var('sel') }}"]))
    detached = DbtTaskGroup(group_id="detached", project_config=ProjectConfig(SHOP),
                            render_config=RenderConfig(select=["fct_orders", "dim_customers"],
                                                       should_detach_multiple_parents_tests=True))
    inner >> default_group
    [none_tests, normalized] >> remote

# Cosmos 0.x keyword interface
legacy = DbtDag(dag_id="cosmos_legacy", dbt_project_name="shop", dbt_root_path=str(ROOT), select={"tags": ["core"]})

# container path: does not exist on this machine, remapped by its trailing components
container = DbtDag(dag_id="cosmos_container", project_config=ProjectConfig("/usr/local/airflow/dbt/shop"))

# project with no manifest: approximate stand-in
mini = DbtDag(dag_id="cosmos_mini", project_config=ProjectConfig(ROOT / "mini"), render_config=RenderConfig(select=["tag:core"]))
