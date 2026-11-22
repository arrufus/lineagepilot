"""The acceptance scenario's Dagster definitions file (task 8.9): a dbt asset definition, the job
and schedule that run it, two assets that read dbt models, and one asset whose dbt cannot be read.

Read by LineagePilot as syntax only. Nothing here is ever imported, so dagster need not be
installed.
"""

import subprocess
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

shop = DbtProject(
    project_dir=Path(__file__).parent.parent.parent, profiles_dir=Path(__file__).parent
)


@dbt_assets(manifest=shop.manifest_path, select="order_counts")
def operations_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@asset(deps=[get_asset_key_for_model([operations_dbt], "order_counts")])
def order_counts_export() -> None:
    """Sends the hourly order counts to the operations dashboard."""


@asset(deps=["customer_revenue"])
def revenue_forecast() -> None:
    """Forecasts next month's revenue from the analytics team's customer_revenue asset."""


@asset
def snapshots() -> None:
    """Runs dbt from Python: which nodes it builds is only known when it runs."""
    subprocess.run(["dbt", "snapshot", "--project-dir", str(shop.project_dir)], check=True)


hourly_counts = define_asset_job(
    "hourly_counts", selection=AssetSelection.assets(operations_dbt).downstream()
)

defs = Definitions(
    assets=[operations_dbt, order_counts_export, revenue_forecast, snapshots],
    jobs=[hourly_counts],
    schedules=[ScheduleDefinition(job=hourly_counts, cron_schedule="0 * * * *")],
    resources={"dbt": DbtCliResource(project_dir=shop)},
)
