from __future__ import annotations

import json
from pathlib import Path

import pytest

from lineagepilot_spike_dag.manifest import load_manifest
from lineagepilot_spike_dag.scanner import scan

FIXTURES = Path(__file__).parent / "fixtures"
SHOP = FIXTURES / "dbt" / "shop"
MINI = FIXTURES / "dbt" / "mini"


@pytest.fixture(scope="session")
def shop_manifest():
    return load_manifest(str(SHOP / "target" / "manifest.json"))


@pytest.fixture(scope="session")
def dbt_ls_answers():
    return json.loads((FIXTURES / "dbt" / "shop_dbt_ls.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def fixture_scan():
    return scan([str(FIXTURES / "dags")], workspace=str(FIXTURES))


def find_link(result, dag_id, name=None, command_index=None, file=None):
    """The link for a DAG and a task id or (prefixed) group id; None group means the DbtDag itself."""
    out = []
    for link in result["links"]:
        if link["dag_id"] != dag_id:
            continue
        if file and not link["location"]["file"].endswith(file):
            continue
        ident = link.get("group_id") or link.get("task_id")
        if ident != name:
            continue
        if command_index is not None and link.get("command_index", 0) != command_index:
            continue
        out.append(link)
    assert len(out) == 1, f"expected one link for {dag_id}/{name}, got {len(out)}"
    return out[0]


def short(ids):
    return sorted(i.split(".", 2)[2] for i in ids)
