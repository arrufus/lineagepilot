"""Tool names where people read them (task 7.6)."""

from __future__ import annotations

from lineagepilot_core.parsers.data_flow_annotations import PIPELINE_TOOLS
from lineagepilot_core.readers.registry import READERS
from lineagepilot_core.readers.tools import (
    TOOL_LABELS,
    counted,
    group_word,
    pipeline_word,
    tool_label,
    tool_of_id,
)


def test_every_tool_read_or_declared_has_a_name():
    assert set(READERS) | PIPELINE_TOOLS | {"dbt_semantic"} <= set(TOOL_LABELS)


def test_names():
    assert tool_label("airflow") == "Airflow"
    assert tool_label("powerbi") == "Power BI"
    assert tool_label("acme_bi") == "acme_bi"  # one this version does not know
    assert tool_label(None) == "other"


def test_pipeline_words():
    assert (pipeline_word("airflow", 1), pipeline_word("airflow", 3)) == ("DAG", "DAGs")
    assert (pipeline_word("dagster", 1), pipeline_word(None, 0)) == ("pipeline", "pipelines")


def test_the_review_groups_dagster_by_code_location():
    """Its jobs and schedules are pipelines, so CodeLens still counts them as such (task 8.9)."""
    assert (group_word("dagster", 1), group_word("dagster", 2)) == (
        "code location",
        "code locations",
    )
    assert (group_word("airflow", 2), group_word(None, 1)) == ("DAGs", "pipeline")


def test_counted_by_kind_and_tool():
    """CodeLens and the review's headline say counts the same way (tasks 7.6 and 7.7)."""
    counts = {
        ("consumer", "looker"): 3,
        ("model", None): 14,
        ("task", "airflow"): 2,
        ("task", "dagster"): 1,
        ("pipeline", "airflow"): 1,
        ("consumer", "powerbi"): 1,
    }
    assert counted(counts) == [
        "14 models",
        "3 tasks (Airflow 2, Dagster 1)",
        "1 Airflow DAG",
        "4 consumers (Looker 3, Power BI 1)",
    ]
    assert counted({("relation", None): 2}) == []


def test_the_tool_an_id_names():
    assert tool_of_id("airflow:daily.marts") == "airflow"
    assert tool_of_id("dbt_semantic:revenue") == "dbt_semantic"
    assert tool_of_id("model.shop.orders") is None
    assert tool_of_id("metric.shop.revenue") is None  # the semantic layer keeps dbt's IDs
    assert tool_of_id("macros/m.sql") is None
