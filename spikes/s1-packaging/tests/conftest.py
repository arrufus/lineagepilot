"""Fixtures that run the bundled server end to end over stdio.

Set LP_TEST_PYTHONS to a list of interpreters separated by os.pathsep to test several Pythons
in one run, for example the supported minimum and the newest. The server process uses only
bundled/libs and the standard library; the test process's own packages never reach it.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import pytest_asyncio
from lsprotocol import types
from pytest_lsp import ClientServerConfig

ROOT = Path(__file__).resolve().parent.parent
SERVER = ROOT / "bundled" / "tool" / "lsp_server.py"
LIBS = ROOT / "bundled" / "libs"
WORKSPACE = ROOT / "fixture-workspace"


def interpreters() -> list[str]:
    configured = os.environ.get("LP_TEST_PYTHONS")
    return [p for p in configured.split(os.pathsep) if p] if configured else [sys.executable]


def version_of(python: str) -> str:
    return subprocess.run([python, "-I", "-c", "import platform; print(platform.python_version())"],
                          check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture(scope="session", autouse=True)
def bundle_exists():
    if not (LIBS / "BUNDLE.json").is_file():
        pytest.exit("bundled/libs is missing; run: python scripts/bundle_libs.py", returncode=2)


@pytest.fixture(params=interpreters(), ids=lambda p: f"py{version_of(p)}")
def server_python(request) -> str:
    return request.param


@pytest_asyncio.fixture
async def start_client(server_python):
    """Factory: start the server, optionally with a hostile environment, and initialise it."""
    clients = []

    async def start(env: dict[str, str] | None = None):
        config = ClientServerConfig(server_command=[server_python, "-I", str(SERVER)],
                                    server_env=env)
        client = await config.start()
        clients.append(client)
        await client.initialize_session(types.InitializeParams(
            capabilities=types.ClientCapabilities(),
            workspace_folders=[types.WorkspaceFolder(uri=WORKSPACE.as_uri(), name="fixture")],
        ))
        return client

    yield start
    for client in clients:
        await client.shutdown_session()
        await client.stop()
