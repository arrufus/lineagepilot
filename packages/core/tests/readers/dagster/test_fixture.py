"""The acceptance scenario's Dagster code locations, in fixtures/impact-shop/dagster (task 8.9): a
dg project with a dbt component, and a definitions file. The reader finds in them what dagster
itself loads, as scripts/dagster_key_conformance.py recorded it, and the one pattern that cannot
be read is an unknown at its line."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from support import IMPACT_SHOP, copy_impact_shop

from lineagepilot_core.graph.model import DAGSTER, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.dagster.reader import DagsterReader

CORE = Path(__file__).resolve().parents[3]
RECORD = CORE / "tests" / "fixtures" / "dagster" / "asset_keys.json"
DEFINITIONS = IMPACT_SHOP / "dagster" / "operations" / "definitions.py"


def script():
    path = CORE / "scripts" / "dagster_key_conformance.py"
    spec = importlib.util.spec_from_file_location("dagster_key_conformance", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def shop(tmp_path_factory) -> Path:
    return copy_impact_shop(tmp_path_factory.mktemp("fixture") / "impact_shop", dagster=True)


def test_the_reader_finds_what_dagster_loads(shop):
    module = script()
    recorded = json.loads(RECORD.read_text(encoding="utf-8"))["fixture"]
    assert module.compare_fixture(recorded, module.reader_fixture(shop)) == []


def test_the_record_has_both_code_locations_and_a_row_of_each_kind():
    recorded = json.loads(RECORD.read_text(encoding="utf-8"))["fixture"]
    assert {label.split(": ")[0] for label in recorded} == {"shop-analytics", "operations"}
    schedule = recorded["operations: schedule hourly_counts_schedule"]
    assert (schedule["job"], schedule["cron"]) == ("hourly_counts", "0 * * * *")
    # An asset in one code location reads a dbt asset in the other: keys are global
    assert recorded["operations: asset revenue_forecast"] == ["model.impact_shop.customer_revenue"]
    assert "model.impact_shop.customer_revenue" in recorded["shop-analytics: definition component"]


def test_the_pattern_that_cannot_be_read_is_an_unknown_at_its_line(shop):
    reader = DagsterReader()
    graph = index_project(shop).graph
    reader.link(graph, reader.read(shop, shop, ["."], None))
    found = [u for u in graph.unresolved if u.evidence.origin is Origin.DAGSTER_DEFS]
    [unknown] = found
    line = DEFINITIONS.read_text(encoding="utf-8").splitlines().index("def snapshots() -> None:")
    assert unknown.subject == "dagster:operations.snapshots"
    assert unknown.reason.startswith(f"dagster/operations/definitions.py:{line + 1}: snapshots()")
    summary = graph.readers[DAGSTER]
    assert (summary.analysed, summary.unknowns, summary.problems) == (True, 1, ())
