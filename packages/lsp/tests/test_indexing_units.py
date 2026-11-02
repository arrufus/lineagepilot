"""Task 2.1 units: the project file, the indexer process and the index state machine."""

from __future__ import annotations

import asyncio
import gc
import io
import json
import os
import sys
import time
from pathlib import Path

import pytest
from conftest import FIXTURE
from pygls.protocol.json_rpc import JsonRPCNotification, JsonRPCRequestMessage

from lineagepilot_lsp import indexing, worker
from lineagepilot_lsp.indexing import (
    ColumnStage,
    IndexState,
    ProjectIndex,
    freeze_heap,
    run_worker,
    stale_files,
)
from lineagepilot_lsp.project import read_project
from lineagepilot_lsp.server import Options, converter
from lineagepilot_lsp.watch import Change, Debouncer, classify

# Project file


def write_project(root: Path, text: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "dbt_project.yml").write_text(text)


def test_project_defaults(tmp_path):
    write_project(tmp_path, "name: shop\n")
    project = read_project(tmp_path)
    assert project.name == "shop" and project.target_path == "target"
    assert project.source_paths == ("models", "seeds", "snapshots", "macros", "analyses", "tests")
    assert project.problems == ()
    assert project.manifest_path == tmp_path / "target" / "manifest.json"


def test_project_paths_are_read(tmp_path):
    write_project(
        tmp_path, 'name: x\ntarget-path: "out/dbt"\nmodel-paths: ["src/models", "more"]\n'
    )
    project = read_project(tmp_path)
    assert project.target_path == "out/dbt"
    assert project.source_paths[:2] == ("src/models", "more")
    assert project.is_source_file("src/models/a.sql") and not project.is_source_file("src/x.sql")
    assert project.is_source_file("dbt_project.yml")


@pytest.mark.parametrize(
    "text, problem",
    [
        ('target-path: "/tmp/out"', "target-path '/tmp/out' is absolute"),
        ('target-path: "C:/out"', "target-path 'C:/out' is absolute"),
        ('target-path: "../../out"', "target-path '../../out' leaves the project"),
        ("target-path: \"{{ env_var('T') }}\"", "is not a plain relative path"),
        ("target-path: 3", "target-path 3 is not a plain relative path"),
        ("model-paths: models", "model-paths is not a list"),
        ('model-paths: ["../shared"]', "model-paths '../shared' leaves the project"),
    ],
)
def test_hostile_project_paths_are_ignored(tmp_path, text, problem):
    write_project(tmp_path / "p", f"name: x\n{text}\n")
    project = read_project(tmp_path / "p")
    assert any(problem in p for p in project.problems), project.problems
    assert project.target_path == "target"
    assert all(not s.startswith("..") and not s.startswith("/") for s in project.source_paths)


@pytest.mark.parametrize(
    "content, problem",
    [
        (None, "no dbt_project.yml"),
        ("- a list\n", "is not a mapping"),
        ("name: [unclosed\n", "cannot be read"),
        ("x" * 1_000_001, "larger than 1 MB"),
    ],
)
def test_unusable_project_files_fall_back_to_defaults(tmp_path, content, problem):
    if content is not None:
        write_project(tmp_path, content)
    project = read_project(tmp_path)
    assert project.name is None and project.target_path == "target"
    assert any(problem in p for p in project.problems)


def test_a_project_rooted_source_path_covers_every_file(tmp_path):
    write_project(tmp_path, 'model-paths: ["."]\n')
    project = read_project(tmp_path)
    assert project.is_source_file("anything/at/all.sql")
    assert project.relative(Path("/elsewhere/x.sql")) is None


# Watching


def test_classify(tmp_path):
    write_project(tmp_path / "p", "name: x\n")
    project = read_project(tmp_path / "p")
    root, workspace = project.root, tmp_path
    dags = ["dags", "../escape"]

    def kind(path: Path):
        found = classify(project, workspace, dags, path)
        return found and found[0]

    assert kind(root / "target" / "manifest.json") is Change.ARTIFACTS
    assert kind(root / "target" / "catalog.json") is Change.ARTIFACTS
    assert kind(root / "target" / "run_results.json") is None
    assert kind(root / ".lineagepilot" / "rules.yml") is Change.RULES
    assert kind(root / "dbt_project.yml") is Change.PROJECT
    assert kind(root / "models" / "a.sql") is Change.SOURCES
    assert kind(workspace / "dags" / "sub" / "d.py") is Change.DAGS
    assert kind(workspace / "dags" / "d.txt") is None
    assert kind(workspace / "other" / "d.py") is None
    assert kind(tmp_path.parent / "escape" / "d.py") is None
    assert classify(project, None, dags, workspace / "dags" / "d.py") is None


async def test_debouncer_coalesces():
    fired: list[set[str]] = []
    debouncer = Debouncer(0.05, fired.append)
    for name in "abc":
        debouncer.push([name])
    await asyncio.sleep(0.15)
    assert fired == [{"a", "b", "c"}]
    debouncer.push(["d"])
    debouncer.cancel()
    await asyncio.sleep(0.1)
    assert fired == [{"a", "b", "c"}]


# The indexer process


def request(tmp_path: Path, project: Path = FIXTURE, **extra) -> dict:
    return {
        "projectDir": str(project),
        "manifest": str(project / "target" / "manifest.json"),
        "catalog": str(project / "target" / "catalog.json"),
        "cacheDir": str(tmp_path / "cache"),
        **extra,
    }


def events(out: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in out.getvalue().splitlines()]


def test_worker_reports_the_model_graph_then_the_full_graph(tmp_path):
    out = io.StringIO()
    assert worker.main(request(tmp_path), out=out) == 0
    model, done = events(out)
    assert (model["event"], done["event"], done["status"]) == ("model", "done", "ok")
    assert Path(model["snapshot"]).is_file() and Path(done["snapshot"]).is_file()
    assert done["projectName"] == "jaffle_shop" and done["schemaVersion"] == 12


def test_worker_reports_unknown(tmp_path):
    out = io.StringIO()
    assert worker.main(request(tmp_path, project=tmp_path / "nothing"), out=out) == 0
    [done] = events(out)
    assert done["status"] == "unknown" and done["snapshot"] is None
    assert done["errors"][0]["message"].startswith("No manifest at")


def test_worker_reports_failures(tmp_path, monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(worker, "index_project", boom)
    out = io.StringIO()
    assert worker.main(request(tmp_path), out=out) == 1
    assert events(out) == [{"event": "failed", "message": "RuntimeError: disk on fire"}]
    assert "disk on fire" in capsys.readouterr().err


def test_worker_command_restores_the_server_path():
    cmd = worker.command("/py", {"a": 1})
    assert cmd[:3] == ["/py", "-I", "-c"]
    assert json.loads(cmd[4]) == sys.path and json.loads(cmd[5]) == {"a": 1}


def python_cmd(code: str) -> list[str]:
    return [sys.executable, "-I", "-c", code]


async def test_run_worker_passes_events_and_skips_noise():
    seen: list[dict] = []
    stderr: list[str] = []

    async def on_event(event):
        seen.append(event)

    code = (
        "import sys; print('not json'); print('[1]'); print('{\"event\": \"done\"}');"
        "sys.stderr.write('warn\\n')"
    )
    exit_ = await run_worker(python_cmd(code), on_event, stderr.append)
    assert exit_.returncode == 0 and seen == [{"event": "done"}]
    assert stderr == ["warn"] and exit_.stderr_tail == ["warn"]


async def test_cancelling_run_worker_kills_the_process(tmp_path):
    marker = tmp_path / "alive"
    code = f"import time; open({str(marker)!r}, 'w').close(); time.sleep(60)"

    async def ignore(event):
        pass

    task = asyncio.create_task(run_worker(python_cmd(code), ignore))
    deadline = time.monotonic() + 20
    while not marker.exists():
        assert time.monotonic() < deadline
        await asyncio.sleep(0.05)
    started = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.monotonic() - started < 10


# The index state machine


def fake_worker(monkeypatch, code: str) -> None:
    monkeypatch.setattr(worker, "command", lambda python, body: python_cmd(code))


def make_index(tmp_path, **kwargs) -> tuple[ProjectIndex, list[str]]:
    calls: list[str] = []
    index = ProjectIndex(
        read_project(FIXTURE),
        tmp_path / "cache",
        on_status=lambda: calls.append("status"),
        on_graph=lambda: calls.append("graph"),
        **kwargs,
    )
    return index, calls


async def test_a_worker_that_dies_quietly_fails_the_index(tmp_path, monkeypatch):
    fake_worker(monkeypatch, "import sys; sys.stderr.write('segfault, probably\\n'); sys.exit(3)")
    index, calls = make_index(tmp_path)
    index.start()
    await index.wait()
    assert index.state is IndexState.FAILED and index.graph is None and not index.indexing
    assert index.message == "the indexer stopped without a result (exit code 3)"
    assert index.errors == [
        {"severity": "error", "message": "segfault, probably", "location": None}
    ]
    assert "graph" not in calls


async def test_a_worker_failure_event_fails_the_index(tmp_path, monkeypatch):
    fake_worker(monkeypatch, 'print(\'{"event": "failed", "message": "MemoryError"}\')')
    index, _ = make_index(tmp_path)
    index.start()
    await index.wait()
    assert index.state is IndexState.FAILED
    assert index.message == "the indexer failed: MemoryError"


@pytest.mark.parametrize("snapshot", [None, "/no/such/snapshot.json"])
async def test_an_unloadable_snapshot_fails_the_index(tmp_path, monkeypatch, snapshot):
    event = json.dumps({"event": "done", "status": "ok", "snapshot": snapshot})
    fake_worker(monkeypatch, f"print({event!r})")
    index, _ = make_index(tmp_path)
    index.start()
    await index.wait()
    assert index.state is IndexState.FAILED
    assert index.message.startswith("the index could not be loaded")


async def test_an_interpreter_that_cannot_start_fails_the_index(tmp_path):
    index, _ = make_index(tmp_path, python=str(tmp_path / "no-python"))
    index.start()
    await index.wait()
    assert index.state is IndexState.FAILED
    assert index.message.startswith("the indexer could not run")


async def test_unknown_events_are_ignored(tmp_path, monkeypatch):
    fake_worker(monkeypatch, 'print(\'{"event": "surprise"}\')')
    index, _ = make_index(tmp_path)
    index.start()
    await index.wait()
    assert index.message == "the indexer stopped without a result"


async def test_stop_cancels_the_run_and_the_timer(tmp_path, monkeypatch):
    fake_worker(monkeypatch, "import time; time.sleep(60)")
    index, _ = make_index(tmp_path, debounce_s=60)
    index.start()
    index.request()
    await index.stop()
    assert index._task.cancelled() and index._timer.cancelled()


async def test_without_column_lineage_the_stage_is_off(tmp_path):
    index, calls = make_index(tmp_path, column_lineage=False)
    assert index.columns is ColumnStage.OFF
    index.start()
    await index.wait()
    assert index.state is IndexState.READY and index.columns is ColumnStage.OFF
    assert calls.count("graph") == 1


async def test_staleness_updates(tmp_path):
    index, calls = make_index(tmp_path)
    await index.refresh_staleness()  # no manifest time yet: nothing is stale
    assert index.stale == set()
    # Older than every fixture file, whenever the repository was checked out
    index.manifest_mtime = 0
    index.update_staleness(["models/staging/stg_orders.sql"])
    assert index.stale == {"models/staging/stg_orders.sql"}
    index.update_staleness(["models/missing.sql"])
    assert index.stale == {"models/staging/stg_orders.sql"}
    assert calls.count("status") == 1


def test_stale_files_scan(tmp_path):
    write_project(tmp_path, "name: x\n")
    (tmp_path / "models" / ".hidden").mkdir(parents=True)
    for name in ("a.sql", "b.sql", ".hidden/c.sql"):
        (tmp_path / "models" / name).write_text("")
    old = time.time() - 3600
    os.utime(tmp_path / "models" / "a.sql", (old, old))
    project = read_project(tmp_path)
    assert stale_files(project, old + 60) == ["dbt_project.yml", "models/b.sql"]
    assert stale_files(project, old + 60, limit=1) == []


def test_freeze_heap():
    try:
        freeze_heap()
        assert gc.get_freeze_count() > 0
    finally:
        gc.unfreeze()


def test_iso_time():
    assert indexing.iso_time(0) == "1970-01-01T00:00:00+00:00"


# Server pieces


def test_requests_without_params_are_decoded():
    """vscode-languageclient omits params; pygls 2.1 alone never answers (S1 finding 4)."""
    c = converter()
    message = c.structure(
        {"jsonrpc": "2.0", "id": 1, "method": "lineage/status"}, JsonRPCRequestMessage
    )
    assert message.params is None
    message = c.structure(
        {"jsonrpc": "2.0", "id": 2, "method": "lineage/graph", "params": {"not-an-identifier": 1}},
        JsonRPCRequestMessage,
    )
    assert message.params == {"not-an-identifier": 1}
    note = c.structure({"jsonrpc": "2.0", "method": "x/y"}, JsonRPCNotification)
    assert note.params is None


@pytest.mark.parametrize(
    "data",
    [
        None,
        [],
        {"trusted": "yes", "maxArtifactMB": -1, "debounceMs": True, "dagFolders": "dags"},
        {"protocolVersions": "1", "projectRoot": "", "columnLineage": "no"},
    ],
)
def test_malformed_options_keep_safe_defaults(data):
    options = Options.from_json(data)
    assert options == Options()
    assert options.trusted is False


def test_relative_to_follows_the_platform_case_rules(tmp_path, monkeypatch):
    from lineagepilot_lsp import project as project_module

    root = tmp_path / "Proj"
    assert project_module.relative_to(root / "models" / "A.sql", root) == "models/A.sql"
    assert project_module.relative_to(root, root) == "."
    assert project_module.relative_to(tmp_path / "Proj2" / "a.sql", root) is None
    # As on Windows: a differently cased spelling is the same place, and keeps its case
    monkeypatch.setattr(project_module.os.path, "normcase", str.lower)
    spelled = Path(str(root).replace("Proj", "PROJ")) / "models" / "A.sql"
    assert project_module.relative_to(spelled, root) == "models/A.sql"


def test_the_project_keeps_the_client_s_spelling_of_its_path(tmp_path):
    """Diagnostics and navigation must use the editor's path, not the resolved one."""
    real = tmp_path / "real"
    write_project(real, 'name: x\nmodel-paths: ["models"]\n')
    (real / "models").mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    project = read_project(link)
    assert project.root == link and project.manifest_path == link / "target" / "manifest.json"
    assert project.relative(real / "models" / "a.sql") == "models/a.sql"
    assert project.relative(link / "models" / "a.sql") == "models/a.sql"
    assert project.source_paths[0] == "models" and project.problems == ()


async def test_a_current_snapshot_is_loaded_without_the_indexer(tmp_path, monkeypatch):
    index, calls = make_index(tmp_path)
    index.start()
    await index.wait()
    assert index.state is IndexState.READY and not index.result["fromCache"]

    def no_indexer(*args, **kwargs):
        raise AssertionError("the indexer ran although the cache had the graph")

    monkeypatch.setattr(worker, "command", no_indexer)
    released = []
    index.on_release = lambda: released.append(index.graph)
    index.start()
    await index.wait()
    assert index.state is IndexState.READY and index.result["fromCache"]
    assert index.columns is ColumnStage.DONE and index.result["projectName"] == "jaffle_shop"
    # The previous graph was let go before the cached one loaded
    assert released == [None]


async def test_a_corrupt_cached_snapshot_is_dropped_and_rebuilt(tmp_path):
    index, _ = make_index(tmp_path)
    index.start()
    await index.wait()
    snapshot = Path(index.result["snapshot"])
    snapshot.write_text("{")
    index.start()
    await index.wait()
    assert index.state is IndexState.READY and not index.result["fromCache"]
    assert snapshot.is_file()  # rebuilt by the indexer


def test_no_cached_snapshot_without_a_manifest_or_over_the_limit(tmp_path):
    index, _ = make_index(tmp_path, max_artifact_bytes=10)
    assert index._cached_snapshot() is None
    empty = ProjectIndex(read_project(tmp_path), tmp_path / "cache")
    assert empty._cached_snapshot() is None


def test_the_indexer_skips_a_windows_venv_launcher(monkeypatch, tmp_path):
    base = tmp_path / "python.exe"
    base.write_text("")
    monkeypatch.setattr(sys, "_base_executable", str(base), raising=False)
    monkeypatch.setattr(sys, "platform", "win32")
    assert worker.interpreter() == str(base)
    monkeypatch.setattr(sys, "platform", "linux")
    assert worker.interpreter() == sys.executable
