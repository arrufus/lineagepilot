"""End-to-end tests of the bundled server over stdio, once per interpreter under test."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
from lsprotocol import types

from conftest import LIBS, SERVER, WORKSPACE, version_of


async def open_and_lens(client, path: Path) -> list[types.CodeLens]:
    uri = path.as_uri()
    client.text_document_did_open(types.DidOpenTextDocumentParams(
        text_document=types.TextDocumentItem(uri=uri, language_id="sql", version=1,
                                             text=path.read_text(encoding="utf-8"))))
    return await client.text_document_code_lens_async(
        types.CodeLensParams(text_document=types.TextDocumentIdentifier(uri=uri)))


def plain(value):
    """The pygls client returns JSON objects as namedtuples and arrays as tuples."""
    if hasattr(value, "_asdict"):
        return {k: plain(v) for k, v in value._asdict().items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return value


async def status(client) -> dict:
    return plain(await client.protocol.send_request_async("lineage/status", None))


def inside(path: str, directory: Path) -> bool:
    return Path(path).resolve().is_relative_to(directory.resolve())


async def test_codelens_uses_all_bundled_libraries(start_client, server_python):
    client = await start_client()
    lenses = await open_and_lens(client, WORKSPACE / "models" / "orders.sql")

    assert len(lenses) == 1
    title = lenses[0].command.title
    assert "2 upstream refs" in title          # regex over the Jinja
    assert "2 tables" in title                 # sqlglot parsed the SQL
    assert "spike_shop" in title               # PyYAML read dbt_project.yml
    assert f"Python {version_of(server_python)}" in title
    assert lenses[0].command.command == "lineagePilot.showStatus"


async def test_source_counts_as_upstream(start_client):
    client = await start_client()
    lenses = await open_and_lens(client, WORKSPACE / "models" / "stg_orders.sql")
    assert "1 upstream ref," in lenses[0].command.title


async def test_unparseable_model_says_so(start_client):
    client = await start_client()
    lenses = await open_and_lens(client, WORKSPACE / "models" / "broken.sql")
    assert "unparsed" in lenses[0].command.title


async def test_non_sql_file_gets_no_lens(start_client):
    client = await start_client()
    assert plain(await open_and_lens(client, WORKSPACE / "dbt_project.yml")) == []


async def test_status_proves_libraries_came_from_the_bundle(start_client, server_python):
    client = await start_client()
    result = await status(client)

    assert result["protocolVersion"] == 0
    assert result["python"]["version"] == version_of(server_python)
    assert result["python"]["isolated"] is True
    for name in ("pygls", "sqlglot", "yaml"):
        assert inside(result["libs"][name]["file"], LIBS), name
    assert result["libs"]["yaml"]["withLibyaml"] is False


async def test_status_accepts_an_empty_params_object(start_client):
    # vscode-languageclient omits params when none are given; pygls 2.1 cannot decode that and
    # never answers. The extension therefore always sends {}; this pins that {} works.
    client = await start_client()
    result = plain(await client.protocol.send_request_async("lineage/status", {}))
    assert result["protocolVersion"] == 0


async def test_hostile_cwd_and_pythonpath_are_ignored(start_client, tmp_path, monkeypatch):
    planted = tmp_path / "pygls"
    planted.mkdir()
    (planted / "__init__.py").write_text("raise ImportError('planted pygls was imported')\n")
    monkeypatch.chdir(WORKSPACE)  # holds a yaml.py that raises on import

    client = await start_client(env={**os.environ, "PYTHONPATH": str(tmp_path)})
    result = await status(client)

    assert inside(result["libs"]["pygls"]["file"], LIBS)
    assert inside(result["libs"]["yaml"]["file"], LIBS)


def version_tuple(python: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version_of(python).split(".")[:2])


def test_old_python_is_refused_with_a_clear_message():
    old = next((p for p in ("/usr/bin/python3", shutil.which("python3.9")) if p and Path(p).exists()
                and version_tuple(p) < (3, 10)), None)
    if old is None:
        pytest.skip("no Python older than 3.10 available")
    result = subprocess.run([old, "-I", str(SERVER)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 3
    assert "needs Python 3.10 or later" in result.stderr
