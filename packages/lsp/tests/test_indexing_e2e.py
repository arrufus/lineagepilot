"""Task 2.1 end to end: indexing on start, the snapshot cache, watching and re-indexing."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from pathlib import Path

import pytest
from conftest import CORE_FIXTURES, FIXTURE
from lineagepilot_core.indexer import index_project
from lsprotocol import types
from pygls.uris import to_fs_path

EXPECTED = index_project(FIXTURE, column_lineage=False).graph.stats()


async def test_indexes_on_start_model_graph_first(start):
    client, result = await start()
    lineage = result.capabilities.experimental["lineagePilot"]
    assert lineage["protocolVersion"] == 1 and lineage["protocolVersions"] == [1]

    status = await client.ready()
    assert status["state"] == "ready" and status["columnLineage"] == "done"
    assert status["project"]["name"] == "pilot_shop"
    assert status["counts"]["nodes"] == EXPECTED["nodes"]
    assert status["columnStates"] == {"resolved": 11, "partial": 0, "unknown": 0}
    assert status["artifacts"]["manifest"]["dbtVersion"] == "1.12.6"
    assert status["artifacts"]["manifest"]["schemaVersion"] == 12
    assert status["artifacts"]["catalog"]["exists"] is True
    assert status["stale"] == {"count": 0, "examples": []}
    assert not status["fromCache"] and status["generation"] == 1

    # The model-level graph was served while column lineage was computed
    stages = [(u["state"], u["columnLineage"]) for u in client.updates]
    assert stages.index(("ready", "computing")) < stages.index(("ready", "done"))


async def test_warm_start_comes_from_the_snapshot_cache(start):
    first, _ = await start()
    await first.ready()
    await first.shutdown_session()

    second, _ = await start()
    status = await second.ready()
    assert status["fromCache"] and status["columnLineage"] == "done"
    assert ("ready", "computing") not in [(u["state"], u["columnLineage"]) for u in second.updates]


async def test_lineage_status_and_refresh(start):
    client, _ = await start()
    await client.ready()
    status = await client.request("lineage/status")
    assert status["state"] == "ready" and status["trusted"] is True
    assert status["server"]["pid"] != os.getpid()

    refreshed = await client.request("lineage/refresh")
    assert refreshed["indexing"] and refreshed["generation"] == 2
    again = await client.ready(2)
    assert again["generation"] == 2 and again["fromCache"]


async def test_a_missing_manifest_is_unknown(start, workspace):
    (workspace / "target" / "manifest.json").unlink()
    client, _ = await start()
    status = await client.ready()
    assert status["state"] == "unknown" and status["counts"] is None
    assert status["message"].startswith("No manifest at")
    assert status["artifacts"]["manifest"]["exists"] is False


async def test_an_untrusted_manifest_is_unknown(start, workspace):
    (workspace / "target" / "manifest.json").write_text('{"nodes": []}')
    client, _ = await start()
    status = await client.ready()
    assert status["state"] == "unknown"
    assert any("structure check failed" in e["message"] for e in status["errors"])


async def test_an_oversized_manifest_is_unknown(start):
    client, _ = await start(options={"maxArtifactMB": 0.01})
    status = await client.ready()
    assert status["state"] == "unknown" and "MB limit" in status["message"]


async def test_watchers_are_registered(start, workspace):
    client, _ = await start(options={"dagFolders": ["dags", "../elsewhere", "/abs"]})
    await client.ready()
    [registration] = client.registrations
    assert registration.method == types.WORKSPACE_DID_CHANGE_WATCHED_FILES
    # Compared as paths: on Windows the client's URI may spell the drive letter c: or C:
    patterns = {
        (Path(to_fs_path(w["globPattern"]["baseUri"])), w["globPattern"]["pattern"])
        for w in registration.register_options["watchers"]
    }
    root = workspace
    assert patterns >= {
        (root, "target/{manifest,catalog}.json"),
        (root, "dbt_project.yml"),
        (root, ".lineagepilot/rules.yml"),
        (root, "models/**"),
        (root, "macros/**"),
        (root, "dags/**/*.py"),
    }
    assert not any("elsewhere" in p or p.startswith("/abs") for _, p in patterns)
    assert all(base == workspace for base, _ in patterns)


async def test_without_dynamic_registration_the_server_says_so(start):
    client, _ = await start(caps=types.ClientCapabilities())
    await client.ready()
    assert client.registrations == []
    assert any("cannot watch files" in m.message for m in client.log_messages)


async def test_artifact_changes_are_debounced_into_one_reindex(start, workspace):
    client, _ = await start()
    await client.ready()
    target = workspace / "target"
    for name in ("manifest.json", "catalog.json"):
        shutil.copy(CORE_FIXTURES / "synth-24" / name, target / name)
    for _ in range(5):
        client.file_changed(target / "manifest.json")
        client.file_changed(target / "catalog.json")
    status = await client.ready(2)
    assert status["state"] == "ready" and status["project"]["name"] == "synthetic"
    assert max(u["generation"] for u in client.updates) == 2


async def test_a_reindex_cancels_the_run_in_progress(start, workspace):
    client, _ = await start(options={"debounceMs": 0})
    await client.wait_for_update(lambda u: u["indexing"])
    client.file_changed(workspace / "target" / "manifest.json")
    status = await client.ready(2)
    assert status["state"] == "ready"
    # Run 1 never reported its result after run 2 started
    started = next(i for i, u in enumerate(client.updates) if u["generation"] == 2)
    assert all(u["generation"] == 2 for u in client.updates[started:])


async def test_a_broken_manifest_drops_the_graph(start, workspace):
    client, _ = await start()
    await client.ready()
    manifest = workspace / "target" / "manifest.json"
    manifest.write_text("{")
    client.file_changed(manifest)
    status = await client.ready(2)
    assert status["state"] == "unknown" and status["counts"] is None
    assert "Invalid JSON" in status["message"]


async def test_source_files_newer_than_the_manifest_are_stale(start, workspace):
    client, _ = await start()
    await client.ready()
    model = workspace / "models" / "staging" / "stg_orders.sql"
    future = time.time() + 60
    os.utime(model, (future, future))
    client.file_changed(model)
    status = await client.wait_for_update(lambda u: u["stale"]["count"] == 1)
    assert status["stale"]["examples"] == ["models/staging/stg_orders.sql"]

    past = time.time() - 3600
    os.utime(model, (past, past))
    client.file_changed(model)
    await client.wait_for_update(lambda u: u["stale"]["count"] == 0)

    # A file outside every source path is not watched for staleness
    client.file_changed(workspace / "README.md")
    model.unlink()
    client.file_changed(model, types.FileChangeType.Deleted)
    await client.wait_for_update(lambda u: u["stale"]["count"] == 0)


async def test_staleness_is_found_at_start(start, workspace):
    future = time.time() + 60
    os.utime(workspace / "models" / "marts" / "orders.yml", (future, future))
    client, _ = await start()
    status = await client.wait_for_update(lambda u: u["stale"]["count"] == 1)
    assert status["stale"]["examples"] == ["models/marts/orders.yml"]


async def test_a_moved_target_path_is_followed(start, workspace):
    client, _ = await start()
    await client.ready()
    shutil.copytree(workspace / "target", workspace / "build")
    shutil.rmtree(workspace / "target")
    project_file = workspace / "dbt_project.yml"
    project_file.write_text(
        project_file.read_text().replace('target-path: "target"', 'target-path: "build"')
    )
    client.file_changed(project_file)
    status = await client.ready(2)
    assert status["state"] == "ready"
    assert status["artifacts"]["manifest"]["path"] == str(workspace / "build" / "manifest.json")
    assert len(client.registrations) == 2


async def test_dag_changes_are_read_again(start, workspace):
    client, _ = await start()
    status = await client.wait_for_update(lambda u: u["airflow"]["stage"] == "done")
    generation = status["airflow"]["generation"]
    dag = workspace / "dags" / "daily.py"
    dag.parent.mkdir()
    dag.write_text("")
    client.file_changed(dag, types.FileChangeType.Created)
    await client.wait_for_update(lambda u: u["airflow"]["generation"] > generation)


async def test_without_storage_snapshots_go_to_a_temporary_directory(start, workspace):
    client, _ = await start(options={"storagePath": None, "columnLineage": False})
    status = await client.ready()
    assert status["state"] == "ready" and status["columnLineage"] == "off"
    assert status["columnStates"] == {"resolved": 0, "partial": 0, "unknown": 0}


@pytest.mark.parametrize("versions, chosen", [([1, 2], 1), ([99], None), (None, None), ("1", None)])
async def test_protocol_version_negotiation(start, versions, chosen):
    client, result = await start(options={"protocolVersions": versions})
    assert result.capabilities.experimental["lineagePilot"]["protocolVersion"] == chosen
    if chosen is None:
        with pytest.raises(Exception, match="no lineage protocol version in common"):
            await client.request("lineage/status")
    else:
        assert (await client.request("lineage/status"))["protocolVersion"] == chosen


async def test_the_project_file_problems_are_logged(start, workspace):
    project_file = workspace / "dbt_project.yml"
    project_file.write_text(
        project_file.read_text().replace('model-paths: ["models"]', 'model-paths: ["/etc"]')
    )
    client, _ = await start()
    status = await client.ready()
    assert any("model-paths '/etc' is absolute" in p for p in status["project"]["problems"])
    assert any("model-paths '/etc' is absolute" in m.message for m in client.log_messages)
    # The models still index: the manifest, not dbt_project.yml, says what exists
    assert status["state"] == "ready"


def test_fixture_artifacts_are_unchanged():
    provenance = json.loads((FIXTURE / "PROVENANCE.json").read_text())
    for name, digest in provenance["sha256"].items():
        assert hashlib.sha256((FIXTURE / name).read_bytes()).hexdigest() == digest
