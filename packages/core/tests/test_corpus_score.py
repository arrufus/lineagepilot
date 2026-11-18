"""The corpus scorer (task 7.9): any reader, a folder per tool, a reader hash per tool.

The real corpora need third-party repositories, so they are scored by hand, never in CI. These
tests score the Airflow reader offline, on fixtures/impact-shop's DAG files, with labels written
here, so the shared scoring and Airflow's rules are both run.
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


@pytest.fixture(scope="module")
def airflow():
    return score.tool("airflow")


@pytest.fixture
def work(tmp_path):
    """A work folder with impact-shop checked out in it, as fetch would leave a repository."""
    shutil.copytree(IMPACT_SHOP / "dags", tmp_path / "example_impact-shop" / "dags")
    for name in ("dbt_project.yml", "target"):
        source = IMPACT_SHOP / name
        target = tmp_path / "example_impact-shop" / name
        shutil.copytree(source, target) if source.is_dir() else shutil.copy(source, target)
    shutil.copytree(IMPACT_SHOP / "models", tmp_path / "example_impact-shop" / "models")
    return tmp_path


def link(id: str, **fields) -> dict:
    return {
        "id": id,
        "operator": "BashOperator",
        "package": "impact_shop",
        "project_path": ".",
        **fields,
    }


def label(*links: dict) -> dict:
    return {
        "repo": REPO,
        "url": "https://example.invalid/impact-shop",
        "sha": "0" * 40,
        "scan": {"paths": ["dags"]},
        "links": list(links),
    }


CORRECT = link(
    "refresh",
    file="dags/operations.py",
    line=10,
    unit="task",
    dag_id="operations",
    task_id="refresh_counts",
    dbt_command="run",
    models=["order_counts"],
    expect="resolved",
)
WRONG_MODELS = link(
    "marts",
    file="dags/daily_marts.py",
    line=22,
    unit="cosmos_task_group",
    dag_id="daily_marts",
    group_id="marts",
    operator="DbtTaskGroup",
    models=["orders"],
    expect="resolved",
    reason="UNSURE: what orders+ selects",
)
LOOP = link(
    "loop",
    file="dags/backfill.py",
    line=12,
    unit="task",
    dag_id="backfill",
    expect="unknown",
    legit=True,
)
MISSING = link(
    "ghost",
    file="dags/operations.py",
    line=40,
    unit="task",
    dag_id="operations",
    task_id="ghost",
    dbt_command="run",
    models=["orders"],
    expect="resolved",
)


def test_a_repository_is_scored_link_by_link_and_pooled(airflow, work):
    found = score.evaluate(airflow, "airflow", [label(CORRECT, WRONG_MODELS, LOOP, MISSING)], work)
    assert found["tool"] == "airflow" and found["reader_sha256"] == score.reader_hash(airflow)
    (repo,) = found["repos"]
    rows = {row["id"]: row for row in repo["labelled_folders"]["rows"]}
    assert rows["refresh"]["outcome"] == "correct" and rows["refresh"]["confidence"] == "heuristic"
    assert (rows["marts"]["outcome"], rows["marts"]["reason"]) == ("miss", "wrong model set")
    assert (
        rows["marts"]["missing"] == []
        and "model.impact_shop.revenue_tiers" in rows["marts"]["extra"]
    )
    assert (rows["loop"]["outcome"], rows["loop"]["category"]) == (
        "unknown_legit",
        "loop_or_factory",
    )
    assert rows["ghost"]["reason"] == "not found: no link and no unresolved record"
    # A repository's metrics carry the scan's own counts; the second scan, only its metrics
    metrics = repo["labelled_folders"]["metrics"]
    assert (metrics["dag_files"], metrics["unresolved_records"]) == (3, 1)
    assert list(repo["default_folders"]) == ["metrics"]
    summary = found["summary"]["labelled_folders"]
    assert {k: summary[k] for k in ("expected_links", "correct", "misses", "legit_unknowns")} == {
        "expected_links": 4,
        "correct": 1,
        "misses": 2,
        "legit_unknowns": 1,
    }
    assert summary["miss_rate"] == 0.5 and summary["false_positives"] == 0
    assert summary["confidence_of_correct"] == {"heuristic": 1}
    # The UNSURE: label is left out of the second figure
    assert summary["without_unsure_labels"]["miss_rate"] == round(1 / 3, 4)
    assert summary["by_operator"] == {
        "BashOperator": {"expected_links": 3, "correct": 1, "misses": 1, "miss_rate": 0.3333},
        "DbtTaskGroup": {"expected_links": 1, "correct": 0, "misses": 1, "miss_rate": 1.0},
    }
    assert summary["miss_reasons"] == {
        "wrong model set": 1,
        "not found: no link and no unresolved record": 1,
    }
    # The default **/dags finds the same files here
    assert found["summary"]["default_folders"]["correct"] == 1


def test_a_link_no_label_expects_is_a_false_positive(airflow, work):
    found = score.evaluate(airflow, "airflow", [label(WRONG_MODELS)], work)
    (positive,) = found["repos"][0]["labelled_folders"]["false_positives"]
    assert (positive["dag_id"], positive["task_or_group"], positive["models"]) == (
        "operations",
        "refresh_counts",
        1,
    )
    assert found["summary"]["labelled_folders"]["false_positives"] == 1


def test_a_repository_not_fetched_is_said_to_be(airflow, tmp_path):
    with pytest.raises(SystemExit, match="is not in .*: run fetch first"):
        score.evaluate(airflow, "airflow", [label(CORRECT)], tmp_path)


# A folder per tool, a hash per reader


def test_the_airflow_reader_s_hash_covers_it_and_what_it_leans_on(airflow):
    files = [p.relative_to(score.CORE).as_posix() for p in score.hash_files(airflow)]
    assert files[-2:] == ["parsers/sql_parser.py", "rules/safe_regex.py"]
    assert {
        "readers/airflow/dag_static.py",
        "readers/airflow/scan.py",
        "readers/common/selector.py",
    } <= set(files)
    assert files == sorted(f for f in files if f.startswith("readers/")) + files[-2:]
    assert re.fullmatch(r"[0-9a-f]{64}", score.reader_hash(airflow))


def test_every_tool_folder_has_what_the_scorer_needs():
    for path in sorted(CORPUS.glob("*/tool.py")):
        found = score.tool(path.parent.name)
        assert found.HASH_FILES and found.SCANS and found.GROUP and callable(found.score)
        assert (path.parent / "repos.json").is_file() and list(
            (path.parent / "labels").glob("*.json")
        )
        assert (path.parent / "README.md").is_file()


def test_a_glob_that_matches_nothing_is_refused(airflow, monkeypatch):
    monkeypatch.setattr(airflow, "HASH_FILES", ("readers/nothing/*.py",))
    with pytest.raises(SystemExit, match="matches nothing"):
        score.hash_files(airflow)


def test_another_corpus_of_a_tool_is_a_folder_inside_it():
    assert score.folder("airflow") == CORPUS / "airflow"
    assert score.folder("airflow", "fresh") == CORPUS / "airflow" / "fresh"
    assert len(score.labels("airflow", "fresh")) == 12
    for name in ("nope", "../airflow", "labels"):
        with pytest.raises(SystemExit, match="no corpus"):
            score.folder("airflow", name)


@pytest.mark.parametrize("name", ["nope", "../packages", "Airflow"])
def test_a_tool_with_no_corpus_is_refused(name):
    with pytest.raises(SystemExit, match="there is one for: airflow"):
        score.tool(name)


def test_the_command_line(airflow, tmp_path, capsys, monkeypatch):
    assert score.main(["hash", "airflow"]) == 0
    assert capsys.readouterr().out.strip() == score.reader_hash(airflow)
    with pytest.raises(SystemExit):
        score.main(["run", "airflow", "../escape"])
    # Fetching skips what is already there, and so runs nothing here
    for found in score.labels("airflow"):
        score.checkout(tmp_path, found).mkdir(parents=True)

    def no_git(*args, **kwargs):
        raise AssertionError("git ran")

    monkeypatch.setattr(score.subprocess, "run", no_git)
    assert score.main(["--work", str(tmp_path), "fetch", "airflow"]) == 0
