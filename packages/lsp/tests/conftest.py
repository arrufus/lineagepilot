"""Fixtures that run the language server end to end over stdio, as the extension does.

The server is started with ``python -I -m lineagepilot_lsp``. Every test gets its own
copy of the fixture workspace, with the source files older than the manifest, so tests
can change files freely and staleness is deterministic.

Set LP_TEST_PYTHONS to several interpreters separated by os.pathsep to run the
end-to-end tests on each; by default they run on this interpreter only.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from lsprotocol import types
from pygls.protocol.json_rpc import JsonRPCResponseMessage
from pytest_lsp import LanguageClient
from pytest_lsp.client import register_lsp_features

from lineagepilot_lsp.server import converter

REPO = Path(__file__).resolve().parents[3]
FIXTURE = REPO / "fixtures" / "jaffle-shop"
CORE_FIXTURES = REPO / "packages" / "core" / "tests" / "fixtures" / "dbt"
WAIT_S = 60.0


def interpreters() -> list[str]:
    configured = os.environ.get("LP_TEST_PYTHONS")
    return [p for p in configured.split(os.pathsep) if p] if configured else [sys.executable]


def age_sources(root: Path, seconds: float = 3600) -> None:
    """Make every file except the artifacts older than the manifest."""
    past = time.time() - seconds
    for path in root.rglob("*"):
        if path.is_file() and "target" not in path.relative_to(root).parts:
            os.utime(path, (past, past))


@pytest.fixture
def workspace(tmp_path) -> Path:
    root = tmp_path / "jaffle-shop"
    shutil.copytree(FIXTURE, root)
    age_sources(root)
    return root


def client_converter():
    """The server's converter, and custom responses kept as decoded JSON too."""
    c = converter()
    c.register_structure_hook(JsonRPCResponseMessage, lambda obj, cls: cls(**obj))
    return c


class Client(LanguageClient):
    """A pytest-lsp client that records index updates and watcher registrations."""

    def __init__(self) -> None:
        super().__init__(converter_factory=client_converter)
        register_lsp_features(self)
        self.updates: list[dict[str, Any]] = []
        self.registrations: list[types.Registration] = []
        self._update_event = asyncio.Event()

        @self.feature("lineage/indexUpdated")
        def on_update(client: Client, params: dict[str, Any]) -> None:
            client.updates.append(params)
            client._update_event.set()

        @self.feature(types.CLIENT_REGISTER_CAPABILITY)
        def on_register(client: Client, params: types.RegistrationParams) -> None:
            client.registrations.extend(params.registrations)

        @self.feature(types.WORKSPACE_CODE_LENS_REFRESH)
        def on_lens_refresh(client: Client, params: None) -> None:
            client.lens_refreshes += 1

        self.lens_refreshes = 0

    async def wait_for_update(
        self, predicate: Callable[[dict[str, Any]], bool], timeout: float = WAIT_S
    ) -> dict[str, Any]:
        """The first index update, received or still to come, that matches."""
        seen = 0
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            for update in self.updates[seen:]:
                if predicate(update):
                    return update
            seen = len(self.updates)
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                last = self.updates[-1] if self.updates else None
                raise TimeoutError(f"no matching index update; the last was {last}")
            self._update_event.clear()
            try:
                await asyncio.wait_for(self._update_event.wait(), remaining)
            except asyncio.TimeoutError:
                pass

    async def ready(self, generation: int = 1, timeout: float = WAIT_S) -> dict[str, Any]:
        """The status once index run ``generation`` is over."""
        return await self.wait_for_update(
            lambda u: u["generation"] == generation and not u["indexing"], timeout
        )

    async def request(self, method: str, params: Any = None) -> Any:
        return await self.protocol.send_request_async(method, {} if params is None else params)

    def file_changed(self, path: Path, kind: types.FileChangeType = types.FileChangeType.Changed):
        self.workspace_did_change_watched_files(
            types.DidChangeWatchedFilesParams(
                changes=[types.FileEvent(uri=path.as_uri(), type=kind)]
            )
        )


def capabilities() -> types.ClientCapabilities:
    return types.ClientCapabilities(
        workspace=types.WorkspaceClientCapabilities(
            did_change_watched_files=types.DidChangeWatchedFilesClientCapabilities(
                dynamic_registration=True, relative_pattern_support=True
            ),
            code_lens=types.CodeLensWorkspaceClientCapabilities(refresh_support=True),
        ),
    )


@pytest.fixture(params=interpreters(), ids=lambda p: Path(p).name)
def server_python(request) -> str:
    return request.param


@pytest_asyncio.fixture
async def start(server_python, workspace, tmp_path):
    """Factory: start a server on the workspace and initialise it."""
    clients: list[Client] = []

    async def start_server(
        root: Path | None = None,
        options: dict[str, Any] | None = None,
        caps: types.ClientCapabilities | None = None,
    ) -> tuple[Client, types.InitializeResult]:
        root = root or workspace
        client = Client()
        await client.start_io(server_python, "-I", "-m", "lineagepilot_lsp")
        clients.append(client)
        init = {
            "protocolVersions": [1],
            "projectRoot": str(root),
            "trusted": True,
            "storagePath": str(tmp_path / "storage"),
            "debounceMs": 100,
            **(options or {}),
        }
        result = await client.initialize_session(
            types.InitializeParams(
                capabilities=caps or capabilities(),
                root_uri=root.as_uri(),
                workspace_folders=[types.WorkspaceFolder(uri=root.as_uri(), name=root.name)],
                initialization_options=init,
            )
        )
        return client, result

    yield start_server
    for client in clients:
        if not client.stopped:
            await client.shutdown_session()
            await client.stop()
