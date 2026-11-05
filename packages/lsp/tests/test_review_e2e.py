"""Tasks 4.1-4.5 end to end: lineage/reviewChanges over stdio, on real dbt artifacts from
before and after a column rename, against lineagepilot review on the same repository."""

from __future__ import annotations

import asyncio
import io
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import REPO, age_sources
from lineagepilot_core.analysis.baseline import ManifestBaseline
from lineagepilot_core.cli import main as cli_main
from lineagepilot_core.gitdiff import git_changes
from lineagepilot_core.project import read_project
from pygls.exceptions import JsonRpcException

from lineagepilot_lsp import protocol, worker
from lineagepilot_lsp.server import LineagePilotServer, Options

S = "model.impact_shop"
SHOP = REPO / "fixtures" / "impact-shop"
RENAME = SHOP / "changes" / "rename-order-total"
NO_BASELINE = {"ref": None, "commit": None, "manifestPath": None}


@pytest.fixture
def shop(tmp_path) -> Path:
    """impact-shop, deployed and committed on main, then renamed and recompiled."""
    root = tmp_path / "impact_shop"
    ignore = shutil.ignore_patterns("changes", "make_fixture.py", "PROVENANCE.json")
    shutil.copytree(SHOP, root, ignore=ignore)
    (root / ".gitignore").write_text("target/\n")
    for args in (
        ("init", "-q", "-b", "main"),
        ("add", "-A"),
        ("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "base"),
    ):
        subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
    for path in RENAME.rglob("*"):
        if path.is_file():
            shutil.copy(path, root / path.relative_to(RENAME))
    age_sources(root)
    return root


def as_params(root: Path, base: str = "main", manifest: str | None = None) -> dict:
    """The params the extension sends: what git says changed, as URIs."""
    changes = git_changes(read_project(root), base)
    return {
        "changedFiles": [
            {
                "uri": (root / f.path).as_uri(),
                "status": f.status.value,
                "previousUri": (root / f.previous_path).as_uri() if f.previous_path else None,
            }
            for f in changes.files
        ],
        "baseline": {
            "ref": changes.base.ref,
            "commit": changes.base.commit,
            "manifestPath": manifest,
        },
    }


async def ready(start, root):
    client, _ = await start(root=root)
    status = await client.ready()
    assert status["state"] == "ready", status
    return client


async def reviewed(client, params) -> dict:
    result = await client.request("lineage/reviewChanges", params)
    protocol.validate("ReviewResult", result)
    return result


async def test_the_acceptance_scenario_end_to_end(start, shop):
    client = await ready(start, shop)
    result = await reviewed(client, as_params(shop))
    node = result["changedNodes"][0]
    assert node["id"] == f"{S}.orders"
    assert node["columns"]["removed"] == ["order_total"]
    hits = {n["id"]: n for n in result["impacted"]}
    assert hits[f"{S}.customer_revenue"]["reach"] == "column"
    assert hits["exposure.impact_shop.revenue_dashboard"]["owner"] == "Revenue Analytics"
    assert "could not be parsed" in hits[f"{S}.broken_report"]["reason"]
    assert any(u["subject"] == f"{S}.broken_report" for u in result["unknowns"])
    assert result["columnLineage"] == "done"
    assert (
        result["locations"][f"{S}.orders"]["uri"]
        == (shop / "models" / "marts" / "orders.sql").as_uri()
    )


async def test_the_panel_and_the_cli_produce_identical_output(start, shop):
    # Exit evidence for task 4.5: the same repository through the server and the command line
    client = await ready(start, shop)
    panel = await reviewed(client, as_params(shop))
    out = io.StringIO()
    assert (
        cli_main(["review", "--project-dir", str(shop), "--base", "main", "--no-cache"], out) == 0
    )
    assert panel["markdown"] == out.getvalue()
    out = io.StringIO()
    args = ["review", "--project-dir", str(shop), "--base", "main", "--no-cache", "--json"]
    assert cli_main(args, out) == 0
    headless = json.loads(out.getvalue())
    assert {k: v for k, v in panel.items() if k != "locations"} == headless


async def test_the_result_is_the_same_on_every_run_and_in_any_order(start, shop):
    client = await ready(start, shop)
    params = as_params(shop)
    first = await reviewed(client, params)
    params["changedFiles"].reverse()
    assert await reviewed(client, params) == first


async def test_a_baseline_manifest_is_read_in_a_child_process_and_kept(
    start, shop, tmp_path, monkeypatch
):
    state = tmp_path / "state" / "manifest.json"
    state.parent.mkdir()
    shutil.copy(SHOP / "target" / "manifest.json", state)
    client = await ready(start, shop)
    result = await reviewed(client, as_params(shop, manifest=str(state)))
    columns = result["changedNodes"][0]["columns"]
    assert columns["baseline"] == "baseline_manifest"
    assert columns["removed"] == ["order_total"] and columns["added"] == ["total_usd"]
    assert result["artifacts"]["baselineManifest"]["name"] == "manifest.json"
    # Unreadable from now on: the same request is answered from what was read, until it changes
    again = await reviewed(client, as_params(shop, manifest=str(state)))
    assert again == result
    state.write_text("{ not json")
    broken = await reviewed(client, as_params(shop, manifest=str(state)))
    assert broken["changedNodes"][0]["columns"]["baseline"] == "catalog"
    assert any(u["reference"] == "baseline manifest" for u in broken["unknowns"])


async def test_a_baseline_path_is_relative_to_the_project(start, shop):
    (shop / "state").mkdir()
    shutil.copy(SHOP / "target" / "manifest.json", shop / "state" / "prod.json")
    client = await ready(start, shop)
    result = await reviewed(client, as_params(shop, manifest="state/prod.json"))
    assert result["changedNodes"][0]["columns"]["baseline"] == "baseline_manifest"


async def test_a_missing_baseline_falls_back_and_is_reported(start, shop):
    client = await ready(start, shop)
    result = await reviewed(client, as_params(shop, manifest="no/such/manifest.json"))
    assert result["changedNodes"][0]["columns"]["baseline"] == "catalog"
    said = [u for u in result["unknowns"] if u["reference"] == "baseline manifest"]
    assert "no file at" in said[0]["reason"]


async def test_files_outside_the_project_are_counted(start, shop):
    client = await ready(start, shop)
    result = await reviewed(
        client,
        {
            "changedFiles": [
                {"uri": "file:///somewhere/else.sql", "status": "modified", "previousUri": None},
                {"uri": "untitled:Untitled-1", "status": "modified", "previousUri": None},
            ],
            "baseline": NO_BASELINE,
        },
    )
    assert result["outsideProject"] == 2 and result["changedNodes"] == []


async def test_a_stale_model_says_to_recompile(start, shop):
    # The manifest from before the rename, with the file from after it
    shutil.copy(SHOP / "target" / "manifest.json", shop / "target" / "manifest.json")
    client = await ready(start, shop)
    result = await reviewed(client, as_params(shop))
    columns = result["changedNodes"][0]["columns"]
    assert columns["state"] == "unknown" and columns["stale"] is True
    assert result["summary"]["staleNodes"] == 1


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"changedFiles": []},
        {
            "changedFiles": [{"uri": "", "status": "modified", "previousUri": None}],
            "baseline": NO_BASELINE,
        },
        {"changedFiles": [], "baseline": {"ref": None, "commit": None}},
    ],
)
async def test_malformed_params_are_refused(start, shop, params):
    client = await ready(start, shop)
    with pytest.raises(JsonRpcException) as caught:
        await client.request("lineage/reviewChanges", params)
    assert caught.value.code == -32602


async def test_no_graph_means_not_ready(start, shop):
    (shop / "target" / "manifest.json").unlink()
    client = await ready_unknown(start, shop)
    with pytest.raises(JsonRpcException) as caught:
        await client.request("lineage/reviewChanges", {"changedFiles": [], "baseline": NO_BASELINE})
    assert caught.value.code == protocol.ErrorCode.NOT_READY


async def ready_unknown(start, root):
    client, _ = await start(root=root)
    status = await client.ready()
    assert status["state"] == "unknown"
    return client


class TestReadBaseline:
    """What goes wrong in the child process, without a language client."""

    def server(self, root: Path) -> LineagePilotServer:
        server = LineagePilotServer()
        server.project = read_project(root)
        server.options = Options()
        return server

    @pytest.mark.parametrize(
        ("failure", "expected"),
        [
            (asyncio.TimeoutError(), "took longer than"),
            (OSError("no such interpreter"), "could not be read"),
        ],
    )
    async def test_a_reader_that_fails_is_reported(self, tmp_path, monkeypatch, failure, expected):
        async def raises(*_args, **_kwargs):
            raise failure

        monkeypatch.setattr("lineagepilot_lsp.server.run_worker", raises)
        found = await self.server(tmp_path).read_baseline("m.json", [], [])
        assert expected in found.error and found.name == "m.json"

    async def test_a_reader_that_reports_failure_says_why(self, tmp_path, monkeypatch):
        async def fails(_cmd, on_event):
            await on_event({"event": "failed", "message": "ValueError: boom"})
            return worker_exit(1)

        monkeypatch.setattr("lineagepilot_lsp.server.run_worker", fails)
        found = await self.server(tmp_path).read_baseline("m.json", [], [])
        assert found.error == "ValueError: boom"

    async def test_a_reader_that_stops_silently_says_so(self, tmp_path, monkeypatch):
        async def silent(_cmd, _on_event):
            return worker_exit(3)

        monkeypatch.setattr("lineagepilot_lsp.server.run_worker", silent)
        found = await self.server(tmp_path).read_baseline("m.json", [], [])
        assert "stopped (3)" in found.error

    async def test_failures_are_not_kept(self, tmp_path, monkeypatch):
        calls = []

        async def silent(_cmd, _on_event):
            calls.append(1)
            return worker_exit(1)

        monkeypatch.setattr("lineagepilot_lsp.server.run_worker", silent)
        server = self.server(tmp_path)
        await server.read_baseline("m.json", [], [])
        await server.read_baseline("m.json", [], [])
        assert len(calls) == 2


def worker_exit(code: int):
    from lineagepilot_lsp.indexing import WorkerExit

    return WorkerExit(code)


class TestWorker:
    def test_it_reports_the_baseline_as_one_line_of_json(self):
        out = io.StringIO()
        code = worker.main(
            {
                "task": "baseline",
                "manifest": str(SHOP / "target" / "manifest.json"),
                "nodes": [f"{S}.orders"],
                "files": ["models/marts/orders.sql"],
            },
            out,
        )
        assert code == 0
        event = json.loads(out.getvalue())
        assert event["event"] == "baseline"
        baseline = ManifestBaseline.from_json(event["baseline"])
        assert baseline.nodes[f"{S}.orders"].columns[2] == "order_total"
        assert baseline.files == {"models/marts/orders.sql": [f"{S}.orders"]}

    def test_an_unexpected_error_is_reported(self, capsys):
        out = io.StringIO()
        assert worker.main({"task": "baseline"}, out) == 1
        assert json.loads(out.getvalue())["event"] == "failed"
        capsys.readouterr()
