"""Shared constants, imported by fixture DAGs. Read as AST only."""
from pathlib import Path

from cosmos import ExecutionConfig

from include.more import SUB

SHOP_PATH = Path("/usr/local/airflow/dbt/shop")
SELECT = ["tag:core"] + [SUB]
EXEC = ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt")


def helper():
    return "not followed"
