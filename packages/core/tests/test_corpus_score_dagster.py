"""Scoring the Dagster reader on its held-out corpus (task 8.9): corpus/dagster/tool.py.

The corpus needs third-party repositories, so it is scored by hand, never in CI. These tests
score the reader offline, on fixtures/impact-shop's two Dagster code locations, with labels
written here in the corpus's format (corpus/dagster/README.md).
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

import pytest
from support import IMPACT_SHOP

CORPUS = Path(__file__).resolve().parents[3] / "corpus"
sys.path.insert(0, str(CORPUS))
import score  # noqa: E402

REPO = "example/impact-shop"
OPERATIONS = "dagster/operations/definitions.py"
MARTS = "dagster/analytics/src/shop_analytics/defs/marts/defs.yaml"


@pytest.fixture(scope="module")
def dagster():
    return score.tool("dagster")


@pytest.fixture
def work(tmp_path):
    """impact-shop checked out as fetch would leave a repository: the dbt project at its root,
    and the Dagster code locations under dagster/."""
    target = tmp_path / "example_impact-shop"
    shutil.copytree(
        IMPACT_SHOP, target, ignore=shutil.ignore_patterns("dags", "variants", "changes")
    )
    return tmp_path


def link(id: str, **fields) -> dict:
    unit, name = id.split(":", 1)
    return {
        "id": id,
        "unit": unit,
        "name": name,
        "package": "impact_shop",
        "project_path": ".",
        "expect": "resolved",
        "models": [],
        "other_nodes": [],
        "reason": "written for the test",
        **fields,
    }


def label(*links: dict, paths: tuple[str, ...] = ("dagster",)) -> dict:
    return {
        "repo": REPO,
        "url": "https://example.invalid/impact-shop",
        "sha": "0" * 40,
        "scan": {"paths": list(paths), "workspace": "."},
        "dbt_projects": ["."],
        "out_of_scope": [],
        "links": list(links),
    }


# The component's folder is its name, at its type: line
COMPONENT = link(
    "dbt_assets:marts",
    api="DbtProjectComponent",
    file=MARTS,
    line=4,
    models=["customer_revenue", "orders", "revenue_tiers"],
)
# At the decorator's line, where the reader gives the def's
DEFINITION = link(
    "dbt_assets:operations_dbt",
    api="@dbt_assets",
    file=OPERATIONS,
    line=26,
    models=["order_counts"],
)
# The job's model set is labelled wrong on purpose
JOB = link(
    "job:hourly_counts",
    api="define_asset_job",
    file=OPERATIONS,
    line=47,
    models=["order_counts", "orders"],
)
SCHEDULE = link(
    "schedule:hourly_counts_schedule",
    api="ScheduleDefinition",
    file=OPERATIONS,
    line=54,
    models=["order_counts"],
)
EXPORT = link(
    "asset:order_counts_export",
    api="@asset deps",
    file=OPERATIONS,
    line=31,
    models=["order_counts"],
)
# The reader does not read what Python that runs dbt runs
DBT_CLI = link("dbt_cli:snapshots", api="@asset", file=OPERATIONS, line=41)
# Nothing is at these
GHOST = link("job:ghost", api="define_asset_job", file=OPERATIONS, line=1, models=["orders"])
NOWHERE = link(
    "asset:nowhere",
    api="@asset deps",
    file=OPERATIONS,
    line=2,
    expect="unknown",
    unknown_reason="for the test",
)


def rows_of(found: dict, scan: str = "labelled_paths") -> dict[str, dict]:
    (repo,) = found["repos"]
    return {row["id"]: row for row in repo[scan]["rows"]}


def test_a_repository_is_scored_unit_by_unit_and_pooled(dagster, work):
    links = (COMPONENT, DEFINITION, JOB, SCHEDULE, EXPORT, DBT_CLI, GHOST, NOWHERE)
    found = score.evaluate(dagster, "dagster", [label(*links)], work)
    assert found["tool"] == "dagster" and found["reader_sha256"] == score.reader_hash(dagster)
    rows = rows_of(found)
    for id in (
        "dbt_assets:marts",
        "dbt_assets:operations_dbt",
        "schedule:hourly_counts_schedule",
        "asset:order_counts_export",
    ):
        assert rows[id]["outcome"] == "correct", rows[id]
        assert rows[id]["other_nodes_match"] is True
    assert rows["dbt_assets:operations_dbt"]["confidence"] == "inferred"
    assert (rows["job:hourly_counts"]["reason"], rows["job:hourly_counts"]["missing"]) == (
        "wrong model set",
        ["model.impact_shop.orders"],
    )
    assert (rows["dbt_cli:snapshots"]["reason"], rows["dbt_cli:snapshots"]["category"]) == (
        "unknown",
        "python_dbt_call",
    )
    assert rows["job:ghost"]["reason"] == "not found: no record and no unknown"
    assert rows["asset:nowhere"]["reason"] == "silently dropped: no record and no unknown"
    summary = found["summary"]["labelled_paths"]
    assert {k: summary[k] for k in ("expected_links", "correct", "misses", "legit_unknowns")} == {
        "expected_links": 8,
        "correct": 4,
        "misses": 4,
        "legit_unknowns": 0,
    }
    assert summary["by_unit"]["dbt_assets"] == {
        "expected_links": 2,
        "correct": 2,
        "misses": 0,
        "miss_rate": 0.0,
    }
    # revenue_forecast is labelled by nobody: the one false positive
    (positive,) = found["repos"][0]["labelled_paths"]["false_positives"]
    assert (positive["list"], positive["name"], positive["models"]) == (
        "assets",
        "revenue_forecast",
        1,
    )
    metrics = found["repos"][0]["labelled_paths"]["metrics"]
    assert metrics["dagster_files"] > 0 and metrics["unknown_records"] == 1
    # The default, the whole workspace, finds the same code here
    assert found["summary"]["default_paths"]["correct"] == 4
    assert list(found["repos"][0]["default_paths"]) == ["metrics"]


def test_an_unknown_answers_a_label_that_expects_one(dagster, work):
    unknown = {**DBT_CLI, "expect": "unknown", "unknown_reason": "what it runs is not in the files"}
    found = score.evaluate(dagster, "dagster", [label(unknown)], work)
    row = rows_of(found)["dbt_cli:snapshots"]
    assert (row["outcome"], row["category"]) == ("unknown_legit", "python_dbt_call")
    # An unknown a label cites is no unexpected one
    assert found["repos"][0]["labelled_paths"]["unexpected_unknown_links"] == []


def test_a_resolved_record_where_only_unknown_is_right_is_a_miss(dagster, work):
    guessed = {**DEFINITION, "expect": "unknown", "models": [], "unknown_reason": "for the test"}
    row = rows_of(score.evaluate(dagster, "dagster", [label(guessed)], work))[
        "dbt_assets:operations_dbt"
    ]
    assert row["reason"] == "resolved to a guess where only unknown is correct"
    assert row["got"] == ["model.impact_shop.order_counts"]


def test_a_unit_found_under_another_name_is_a_miss_and_no_false_positive(dagster, work):
    renamed = {**EXPORT, "id": "asset:export", "name": "export"}
    found = score.evaluate(dagster, "dagster", [label(renamed)], work)
    row = rows_of(found)["asset:export"]
    assert (row["reason"], row["got"]["name"]) == (
        "found with the wrong identity",
        "order_counts_export",
    )
    names = {p["name"] for p in found["repos"][0]["labelled_paths"]["false_positives"]}
    assert "order_counts_export" not in names


def test_an_unknown_at_a_place_answers_every_label_there(dagster, work):
    """A loop makes several units at one line; one unknown there is each one's outcome."""
    one = {**DBT_CLI, "id": "dbt_cli:a", "name": "a"}
    two = {**DBT_CLI, "id": "dbt_cli:b", "name": "b"}
    rows = rows_of(score.evaluate(dagster, "dagster", [label(one, two)], work))
    for id in ("dbt_cli:a", "dbt_cli:b"):
        assert (rows[id]["reason"], rows[id]["category"]) == (
            "only an unknown at its place",
            "python_dbt_call",
        )


CUSTOM = """import subprocess

import dagster


def multi(**kwargs):
    return lambda fn: fn


@multi(
    select="orders",
)
def custom_assets():
    subprocess.run(["dbt", "build", "--select", "orders"], check=True)
"""


def test_an_unknown_on_a_code_location_answers_a_label_at_its_place(dagster, work):
    """A decorator the reader does not know makes no asset, so Python that runs dbt in it is an
    unknown on the code location. It is still at the label's place. One that answers no label
    is counted apart, not as an unexpected unknown."""
    (work / "example_impact-shop" / "dagster" / "operations" / "custom.py").write_text(CUSTOM)
    custom = link(
        "dbt_assets:custom_assets",
        api="custom multi_asset",
        file="dagster/operations/custom.py",
        line=10,
        models=["orders"],
    )
    found = score.evaluate(dagster, "dagster", [label(custom)], work)
    row = rows_of(found)["dbt_assets:custom_assets"]
    assert (row["reason"], row["category"]) == ("only an unknown at its place", "python_dbt_call")
    scanned = found["repos"][0]["labelled_paths"]
    # The fixture's own snapshots asset is labelled by nobody here
    assert {u["name"] for u in scanned["unexpected_unknown_links"]} == {"snapshots"}
    assert scanned["metrics"]["location_unknowns_answering_no_label"] == 0
    # With no label there, it is counted, and listed nowhere
    found = score.evaluate(dagster, "dagster", [label(DEFINITION)], work)
    scanned = found["repos"][0]["labelled_paths"]
    assert scanned["metrics"]["location_unknowns_answering_no_label"] == 1
    assert {u["name"] for u in scanned["unexpected_unknown_links"]} == {"snapshots"}


def test_the_wrong_dbt_project_is_a_miss(dagster, work):
    elsewhere = {**DEFINITION, "project_path": "other"}
    row = rows_of(score.evaluate(dagster, "dagster", [label(elsewhere)], work))[
        "dbt_assets:operations_dbt"
    ]
    assert (row["reason"], row["got"], row["expected"]) == ("wrong project", ".", "other")


def test_seeds_and_snapshots_are_scored_apart(dagster, work):
    seeded = {**COMPONENT, "other_nodes": ["seed.raw_orders"]}
    row = rows_of(score.evaluate(dagster, "dagster", [label(seeded)], work))["dbt_assets:marts"]
    assert row["outcome"] == "correct" and row["other_nodes_match"] is False
    assert row["other_nodes_diff"] == {"missing": ["seed.impact_shop.raw_orders"], "extra": []}


def test_labelled_paths_are_the_label_s_and_the_default_is_the_workspace(dagster, work):
    found = score.evaluate(dagster, "dagster", [label(DEFINITION, paths=("dags",))], work)
    assert rows_of(found)["dbt_assets:operations_dbt"]["outcome"] == "miss"
    assert found["summary"]["default_paths"]["correct"] == 1


def test_the_dagster_reader_s_hash_covers_it_and_what_it_leans_on(dagster):
    files = [p.relative_to(score.CORE).as_posix() for p in score.hash_files(dagster)]
    assert {
        "readers/dagster/scan.py",
        "readers/dagster/python.py",
        "readers/common/evaluator.py",
        "readers/common/selector.py",
        "parsers/safe_yaml.py",
    } <= set(files)
    assert not any(f.startswith("readers/airflow/") for f in files)
    assert re.fullmatch(r"[0-9a-f]{64}", score.reader_hash(dagster))


def test_the_frozen_hash_is_the_one_tool_py_records(dagster):
    """The reader scored is the reader frozen: a change to it after the freeze shows here, and
    needs a new freeze and a new run, not an edit of the old one's result."""
    recorded = re.search(r"reader_sha256 ([0-9a-f]{64})", dagster.__doc__ or "")
    assert recorded is not None
    first = CORPUS / "dagster" / "results" / "first.json"
    if first.is_file():
        import json

        assert json.loads(first.read_text(encoding="utf-8"))["reader_sha256"] == recorded.group(1)
