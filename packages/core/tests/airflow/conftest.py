"""Shared scans of the DAG fixtures, which every scanner test reads."""

from __future__ import annotations

import json

import pytest
from airflow_support import FIXTURES, SHOP, scan

from lineagepilot_core.airflow.manifest import load_manifest


@pytest.fixture(scope="session")
def shop_manifest():
    return load_manifest(str(SHOP / "target" / "manifest.json"))


@pytest.fixture(scope="session")
def dbt_ls_answers():
    return json.loads((FIXTURES / "dbt" / "shop_dbt_ls.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def fixture_scan():
    return scan([str(FIXTURES / "dags")], workspace=str(FIXTURES))
