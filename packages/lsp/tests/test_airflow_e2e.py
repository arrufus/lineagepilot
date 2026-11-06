"""Tasks 5.1-5.4 end to end: DAG files read in a child process, over stdio, on impact-shop."""

from __future__ import annotations

import asyncio
import io
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import pytest
from conftest import REPO, age_sources
from lineagepilot_core.cli import main as cli_main
from lineagepilot_core.gitdiff import git_changes
from lineagepilot_core.project import read_project
from lsprotocol import types

from lineagepilot_lsp import protocol
from lineagepilot_lsp.airflow_scan import AirflowScanner, AirflowStage

SHOP = REPO / "fixtures" / "impact-shop"
RENAME = SHOP / "changes" / "rename-order-total"
ORDERS = "model.impact_shop.orders"
COUNTS = "model.impact_shop.order_counts"
GROUP = "airflow:daily_marts.marts"
BASH = "airflow:operations.refresh_counts"


@pytest.fixture
def shop(tmp_path) -> Path:
    root = tmp_path / "impact_shop"
    ignore = shutil.ignore_patterns("changes", "make_fixture.py", "PROVENANCE.json")
    shutil.copytree(SHOP, root, ignore=ignore)
    (root / ".gitignore").write_text("target/\n")
    age_sources(root)
    return root


def scanned(update: dict) -> bool:
    return update["airflow"]["stage"] in ("done", "failed") and not update["indexing"]


async def ready(start, root, options=None):
    client, _ = await start(root=root, options=options)
    status = await client.wait_for_update(lambda u: scanned(u) and u["state"] == "ready")
    return client, status


async def test_the_status_says_what_was_read(start, shop):
    _, status = await ready(start, shop)
    protocol.Status.from_json(status)
    summary = status["airflow"]["summary"]
    assert summary["analysed"] and summary["dagFiles"] == 3 and summary["dags"] == 3
    assert summary["tasks"] >= 4 and summary["unknowns"] == 1


async def test_the_tasks_that_run_a_model_and_where_to_open_them(start, shop):
    client, _ = await ready(start, shop)
    result = await client.request("lineage/airflow", {"nodes": [COUNTS]})
    protocol.AirflowResult.from_json(result)
    assert result["stage"] == "done"
    by_id = {t["id"]: t for d in result["dags"] for t in d["tasks"]}
    assert set(by_id) == {GROUP, BASH}
    assert by_id[GROUP]["derivedTasks"] == ["marts.order_counts_run"]
    location = by_id[BASH]["location"]
    assert Path(urlparse(location["uri"]).path).name == "operations.py"
    assert location["range"]["start"]["line"] == 9
    assert [u["subject"] for u in result["unknowns"]] == ["airflow:backfill"]


async def test_every_dag_and_task(start, shop):
    client, _ = await ready(start, shop)
    result = await client.request("lineage/airflow", {"nodes": None})
    assert {d["name"] for d in result["dags"]} == {"backfill", "daily_marts", "operations"}


async def test_codelens_counts_the_tasks(start, shop):
    client, _ = await ready(start, shop)
    lenses = await client.text_document_code_lens_async(
        types.CodeLensParams(
            text_document=types.TextDocumentIdentifier(
                uri=(shop / "models" / "marts" / "orders.sql").as_uri()
            )
        )
    )
    assert "Airflow tasks" in lenses[0].command.title
    # A scan that arrives after the first lens asks the editor to refresh them
    assert client.lens_refreshes >= 1


async def test_the_graph_and_impact_reach_tasks(start, shop):
    client, _ = await ready(start, shop)
    graph = await client.request(
        "lineage/graph",
        {"root": ORDERS, "direction": "downstream", "depth": 2, "granularity": "model"},
    )
    kinds = {n["id"]: n["kind"] for n in graph["nodes"]}
    assert kinds[GROUP] == "airflow_task"
    edge = next(e for e in graph["edges"] if e["upstream"] == ORDERS and e["downstream"] == GROUP)
    assert edge["kind"] == "orchestrates" and edge["origins"] == ["dag_static"]
    impact = await client.request("lineage/impact", {"targets": [ORDERS]})
    assert impact["counts"]["airflow_task"] >= 3
    assert any(u["subject"] == "airflow:backfill" for u in impact["unknowns"])


async def test_a_changed_dag_file_is_read_again(start, shop):
    client, status = await ready(start, shop)
    generation = status["airflow"]["generation"]
    dag = shop / "dags" / "operations.py"
    dag.write_text(dag.read_text().replace("order_counts", "customer_revenue"))
    client.file_changed(dag)
    await client.wait_for_update(
        lambda u: u["airflow"]["generation"] > generation and u["airflow"]["stage"] == "done"
    )
    result = await client.request(
        "lineage/airflow", {"nodes": ["model.impact_shop.customer_revenue"]}
    )
    assert BASH in {t["id"] for d in result["dags"] for t in d["tasks"]}
    result = await client.request("lineage/airflow", {"nodes": [COUNTS]})
    assert BASH not in {t["id"] for d in result["dags"] for t in d["tasks"]}


async def test_the_annotation_file_is_read_again(start, shop):
    client, status = await ready(start, shop)
    generation = status["airflow"]["generation"]
    annotation = shop / ".lineagepilot" / "airflow.yml"
    annotation.parent.mkdir()
    annotation.write_text(
        "pipelines:\n"
        "  - pipeline_id: loader\n"
        "    owner: ingest\n"
        "    tasks:\n"
        "      - task_id: load\n"
        f"        consumes: [{ORDERS}]\n"
    )
    client.file_changed(annotation, types.FileChangeType.Created)
    await client.wait_for_update(
        lambda u: u["airflow"]["generation"] > generation and u["airflow"]["stage"] == "done"
    )
    result = await client.request("lineage/airflow", {"nodes": [ORDERS]})
    task = next(t for d in result["dags"] for t in d["tasks"] if t["id"] == "airflow:loader.load")
    assert task["confidence"] == "declared" and task["owner"] == "ingest"
    assert task["origins"] == ["annotation"]


async def test_a_new_manifest_is_scanned_with_its_own_selectors(start, shop):
    client, status = await ready(start, shop)
    generation = status["airflow"]["generation"]
    shutil.copy(RENAME / "target" / "manifest.json", shop / "target" / "manifest.json")
    client.file_changed(shop / "target" / "manifest.json")
    await client.wait_for_update(
        lambda u: u["airflow"]["generation"] > generation and scanned(u) and u["generation"] > 1
    )
    result = await client.request("lineage/airflow", {"nodes": [ORDERS]})
    assert result["stage"] == "done" and result["dags"]


async def test_refresh_reads_them_again_and_the_model_level_stage_does_not_scan_twice(start, shop):
    client, status = await ready(start, shop)
    first = status["airflow"]["generation"]
    assert first == 1
    await client.request("lineage/refresh", {})
    status = await client.wait_for_update(lambda u: u["generation"] == 2 and scanned(u))
    assert status["airflow"]["generation"] == 2


async def test_with_no_dag_files_the_review_can_say_so(start, shop):
    shutil.rmtree(shop / "dags")
    _, status = await ready(start, shop)
    summary = status["airflow"]["summary"]
    assert not summary["analysed"] and "no DAG files were found in **/dags" in summary["reason"]


async def test_dag_folders_from_the_settings(start, shop):
    (shop / "orchestration").mkdir()
    (shop / "dags" / "operations.py").rename(shop / "orchestration" / "operations.py")
    _, status = await ready(start, shop, {"dagFolders": ["orchestration", "../elsewhere", "/abs"]})
    assert status["airflow"]["summary"]["dagFiles"] == 1
    assert status["airflow"]["summary"]["folders"] == ["orchestration"]


async def test_a_dag_file_outside_the_project_can_be_opened(start, tmp_path):
    repo = tmp_path / "repo"
    shutil.copytree(
        SHOP,
        repo / "dbt",
        ignore=shutil.ignore_patterns("changes", "make_fixture.py", "PROVENANCE.json", "dags"),
    )
    (repo / "airflow" / "dags").mkdir(parents=True)
    (repo / "airflow" / "dags" / "d.py").write_text(
        "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
        'with DAG("d") as d:\n'
        '    BashOperator(task_id="t", bash_command="dbt run --select orders --project-dir dbt")\n'
    )
    client, _ = await _start_in(start, repo, repo / "dbt")
    result = await client.request("lineage/airflow", {"nodes": [ORDERS]})
    location = result["dags"][0]["tasks"][0]["location"]
    assert location["uri"] == (repo / "airflow" / "dags" / "d.py").as_uri()
    assert ".." not in location["uri"]


async def _start_in(start, workspace, project):
    """A server whose workspace folder is not the project's own."""
    client, _ = await start(root=workspace, options={"projectRoot": str(project)})
    status = await client.wait_for_update(lambda u: scanned(u) and u["state"] == "ready")
    return client, status


async def test_restricted_mode_reads_dags_and_runs_nothing(start, shop):
    marker = shop / "marker"
    (shop / "dags" / "hostile.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('ran')\n"
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        'with DAG("hostile") as d:\n'
        '    BashOperator(task_id="t", bash_command="dbt run --select orders")\n'
    )
    client, status = await ready(start, shop, {"trusted": False})
    assert status["trusted"] is False and status["airflow"]["summary"]["analysed"]
    result = await client.request("lineage/airflow", {"nodes": [ORDERS]})
    assert "airflow:hostile.t" in {t["id"] for d in result["dags"] for t in d["tasks"]}
    assert not marker.exists()


async def test_the_panel_and_the_command_agree_with_the_tasks_in(start, shop, tmp_path):
    for args in (
        ("init", "-q", "-b", "main"),
        ("add", "-A"),
        ("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "base"),
    ):
        subprocess.run(["git", *args], cwd=shop, check=True, capture_output=True)
    for path in RENAME.rglob("*"):
        if path.is_file():
            shutil.copy(path, shop / path.relative_to(RENAME))
    age_sources(shop)
    client, _ = await ready(start, shop)
    changes = git_changes(read_project(shop), "main")
    review = await client.request(
        "lineage/reviewChanges",
        {
            "changedFiles": [
                {
                    "uri": (shop / f.path).as_uri(),
                    "status": f.status.value,
                    "previousUri": None,
                }
                for f in changes.files
            ],
            "baseline": {
                "ref": changes.base.ref,
                "commit": changes.base.commit,
                "manifestPath": None,
            },
        },
    )
    protocol.ReviewResult.from_json(review)
    out = io.StringIO()
    code = cli_main(
        [
            "review",
            "--project-dir",
            str(shop),
            "--base",
            "main",
            "--no-cache",
            "--workspace",
            str(shop),
        ],
        out,
    )
    assert code == 0 and review["markdown"] == out.getvalue()
    assert review["airflow"]["analysed"] and "`data-platform`" in review["markdown"]
    tasks = [n for n in review["impacted"] if n["kind"] == "airflow_task"]
    assert GROUP in {n["id"] for n in tasks}
    group = next(n for n in tasks if n["id"] == GROUP)
    assert group["owner"] == "data-platform"
    assert group["airflow"]["tasks"][:2] == ["marts.customer_revenue_run", "marts.orders.run"]
    assert review["locations"][GROUP]["uri"].endswith("dags/daily_marts.py")


class TestTheScanner:
    def project(self, tmp_path):
        return read_project(tmp_path)

    async def test_an_interpreter_that_cannot_start_is_a_failure_with_words(self, tmp_path):
        changes = []
        scanner = AirflowScanner(
            self.project(tmp_path),
            tmp_path,
            ["dags"],
            tmp_path / "out",
            lambda: changes.append(1),
            python=str(tmp_path / "no-such-python"),
        )
        scanner.start("k")
        await scanner.wait()
        assert scanner.stage is AirflowStage.FAILED and "could not start" in scanner.message
        assert changes == [1] and not scanner.current_for("k")

    async def test_a_reader_that_dies_is_a_failure_with_words(self, tmp_path):
        scanner = AirflowScanner(
            self.project(tmp_path), tmp_path, ["dags"], tmp_path / "out", lambda: None
        )
        scanner.request_body = lambda output: {"task": "airflow"}  # no paths: the reader fails
        scanner.start("k")
        await scanner.wait()
        assert scanner.stage is AirflowStage.FAILED and "KeyError" in scanner.message

    async def test_a_later_scan_overtakes_an_earlier_one(self, tmp_path):
        calls = []
        scanner = AirflowScanner(
            self.project(tmp_path), tmp_path, ["dags"], tmp_path / "out", lambda: calls.append(1)
        )
        scanner.start("a")
        scanner.start("b")
        await scanner.wait()
        await asyncio.sleep(0)
        assert calls == [1] and scanner.current_for("b") and not scanner.current_for("a")
        assert not list((tmp_path / "out").glob("*.json"))
        scanner.invalidate()
        assert not scanner.current_for("b")

    async def test_stop_kills_it(self, tmp_path):
        scanner = AirflowScanner(
            self.project(tmp_path), tmp_path, ["dags"], tmp_path / "out", lambda: None
        )
        scanner.start("a")
        await scanner.stop()
        assert scanner.scanning_for("a")  # stopped, not finished
