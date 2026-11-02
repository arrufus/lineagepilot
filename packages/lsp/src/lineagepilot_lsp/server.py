"""The LineagePilot language server (design sections 4 and 9).

One server runs per dbt project, over stdio only. The client passes, in
``initializationOptions``:

- ``protocolVersions``: the ``lineage/*`` protocol versions it speaks. The server picks
  the newest it also speaks and returns it in
  ``capabilities.experimental.lineagePilot.protocolVersion``, or null if there is none,
  in which case ``lineage/*`` requests fail. Standard LSP features work regardless.
- ``projectRoot``: the folder with ``dbt_project.yml`` (default: the root URI).
- ``trusted``: VS Code Workspace Trust. Untrusted workspaces get built-in rules only.
- ``storagePath``: where to cache graph snapshots (default: a temporary directory).
- ``columnLineage``, ``maxArtifactMB``, ``debounceMs``, ``dagFolders``.
"""

from __future__ import annotations

import asyncio
import gc
import hashlib
import json
import logging
import os
import platform
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lineagepilot_core.rules import (
    Evaluation,
    RulePack,
    builtin_rules,
    evaluate,
    load_rule_pack,
)
from lsprotocol import types
from pygls.exceptions import JsonRpcInvalidParams, JsonRpcServerError
from pygls.lsp.server import LanguageServer
from pygls.protocol import LanguageServerProtocol, default_converter, lsp_method
from pygls.protocol.json_rpc import JsonRPCNotification, JsonRPCRequestMessage
from pygls.uris import to_fs_path

from lineagepilot_lsp import __version__, features, methods, protocol
from lineagepilot_lsp.context import GraphContext, clip
from lineagepilot_lsp.indexing import ColumnStage, ProjectIndex, freeze_heap, iso_time
from lineagepilot_lsp.project import DbtProject, read_project
from lineagepilot_lsp.watch import RULES_FILE, Change, Debouncer, classify, watchers

logger = logging.getLogger(__name__)

# The lineage/* protocol versions this server speaks (design section 9)
PROTOCOL_VERSIONS = (protocol.PROTOCOL_VERSION,)
MAX_STATUS_ERRORS = 50
DEFAULT_MAX_ARTIFACT_MB = 512
SOURCE_DEBOUNCE_S = 0.5
NOTIFY_INDEX_UPDATED = "lineage/indexUpdated"


def converter():
    """pygls's converter, except that custom requests keep their params as decoded JSON.

    pygls turns unknown params into namedtuples, renaming keys that are not identifiers,
    and fails without replying to a request that has no ``params`` field (spike S1,
    finding 4). The lineage/* handlers validate plain dicts against the shared schema.
    """
    c = default_converter()

    def keep_params(obj: dict[str, Any], cls: type) -> Any:
        return cls(**{"params": None, **obj})

    c.register_structure_hook(JsonRPCRequestMessage, keep_params)
    c.register_structure_hook(JsonRPCNotification, keep_params)
    return c


class LineagePilotProtocol(LanguageServerProtocol):
    @lsp_method(types.INITIALIZE)
    def lsp_initialize(self, params: types.InitializeParams):
        result = yield from super().lsp_initialize(params)
        result.capabilities.experimental = self._server.experimental()
        return result


@dataclass
class Options:
    protocol_versions: list[int] = field(default_factory=list)
    project_root: Path | None = None
    trusted: bool = False
    storage_path: Path | None = None
    column_lineage: bool = True
    max_artifact_mb: float = DEFAULT_MAX_ARTIFACT_MB
    debounce_s: float = 1.0
    dag_folders: list[str] = field(default_factory=lambda: ["dags"])

    @classmethod
    def from_json(cls, data: Any) -> Options:
        """Read initializationOptions, keeping defaults for anything missing or malformed."""
        options = cls()
        if not isinstance(data, dict):
            return options
        versions = data.get("protocolVersions")
        if isinstance(versions, list):
            options.protocol_versions = [v for v in versions if isinstance(v, int)]
        for key, attr in (("projectRoot", "project_root"), ("storagePath", "storage_path")):
            if isinstance(data.get(key), str) and data[key]:
                setattr(options, attr, Path(data[key]))
        # Trust is only ever granted explicitly
        options.trusted = data.get("trusted") is True
        if isinstance(data.get("columnLineage"), bool):
            options.column_lineage = data["columnLineage"]
        if _number(data.get("maxArtifactMB")) and data["maxArtifactMB"] > 0:
            options.max_artifact_mb = data["maxArtifactMB"]
        if _number(data.get("debounceMs")) and data["debounceMs"] >= 0:
            options.debounce_s = data["debounceMs"] / 1000
        if isinstance(data.get("dagFolders"), list):
            options.dag_folders = [f for f in data["dagFolders"] if isinstance(f, str)]
        return options


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


class LineagePilotServer(LanguageServer):
    def __init__(self) -> None:
        super().__init__(
            "lineagepilot",
            __version__,
            protocol_cls=LineagePilotProtocol,
            converter_factory=converter,
        )
        self.options = Options()
        self.protocol_version: int | None = None
        self.project: DbtProject | None = None
        self.workspace_root: Path | None = None
        self.index: ProjectIndex | None = None
        self.context: GraphContext | None = None
        self.rule_pack: RulePack | None = None
        self.evaluation: Evaluation | None = None
        self.published: dict[str, int] = {}  # file URI -> fingerprint of its diagnostics
        self._temp_storage: str | None = None
        self._debouncers: dict[Change, Debouncer] = {}
        self._background: set[asyncio.Task[Any]] = set()

    # Lifecycle

    def configure(self, params: types.InitializeParams) -> None:
        self.options = Options.from_json(params.initialization_options)
        common = set(self.options.protocol_versions) & set(PROTOCOL_VERSIONS)
        self.protocol_version = max(common) if common else None
        root_uri = params.root_uri or (
            params.workspace_folders[0].uri if params.workspace_folders else None
        )
        self.workspace_root = Path(to_fs_path(root_uri)) if root_uri else None
        root = self.options.project_root or self.workspace_root
        self.project = read_project(root) if root is not None else None

    def experimental(self) -> dict[str, Any]:
        return {
            "lineagePilot": {
                "protocolVersion": self.protocol_version,
                "protocolVersions": list(PROTOCOL_VERSIONS),
                "serverVersion": __version__,
            }
        }

    async def initialized(self) -> None:
        if self.project is None:
            self.log("No dbt project to index: the client sent no root", types.MessageType.Error)
            return
        for problem in self.project.problems:
            self.log(f"dbt_project.yml: {problem}", types.MessageType.Warning)
        await self.register_watchers()
        self.index = ProjectIndex(
            self.project,
            self.cache_dir(),
            column_lineage=self.options.column_lineage,
            max_artifact_bytes=int(self.options.max_artifact_mb * 2**20),
            debounce_s=self.options.debounce_s,
            on_status=self.notify_index_updated,
            on_graph=self.graph_changed,
            on_release=self.release_graph,
        )
        self._debouncers = {
            Change.SOURCES: Debouncer(SOURCE_DEBOUNCE_S, self.sources_changed),
            Change.PROJECT: Debouncer(self.options.debounce_s, lambda _: self.reload_project()),
            Change.RULES: Debouncer(self.options.debounce_s, lambda _: self.rules_changed()),
            Change.DAGS: Debouncer(self.options.debounce_s, self.dags_changed),
        }
        self.load_rules()
        self.index.start()

    async def stop_indexing(self) -> None:
        for debouncer in self._debouncers.values():
            debouncer.cancel()
        if self.index is not None:
            await self.index.stop()
        for task in list(self._background):
            task.cancel()
        if self._temp_storage:
            shutil.rmtree(self._temp_storage, ignore_errors=True)

    def cache_dir(self) -> Path:
        assert self.project is not None
        key = hashlib.sha256(str(self.project.root).encode()).hexdigest()[:16]
        if self.options.storage_path is not None:
            return self.options.storage_path / "snapshots" / key
        self._temp_storage = self._temp_storage or tempfile.mkdtemp(prefix="lineagepilot-")
        return Path(self._temp_storage) / key

    async def register_watchers(self) -> None:
        assert self.project is not None
        capability = self.client_capabilities.workspace
        watched = capability.did_change_watched_files if capability else None
        if not (watched and watched.dynamic_registration):
            self.log(
                "The client cannot watch files for the server; re-index with lineage/refresh",
                types.MessageType.Warning,
            )
            return
        options = types.DidChangeWatchedFilesRegistrationOptions(
            watchers=watchers(self.project, self.workspace_root, self.options.dag_folders)
        )
        await self.client_register_capability_async(
            types.RegistrationParams(
                registrations=[
                    types.Registration(
                        id="lineagepilot-watch",
                        method=types.WORKSPACE_DID_CHANGE_WATCHED_FILES,
                        register_options=options,
                    )
                ]
            )
        )

    # Changes

    def files_changed(self, params: types.DidChangeWatchedFilesParams) -> None:
        if self.project is None or self.index is None:
            return
        for event in params.changes:
            found = classify(
                self.project,
                self.workspace_root,
                self.options.dag_folders,
                Path(to_fs_path(event.uri)),
            )
            if found is None:
                continue
            kind, relative = found
            if kind is Change.ARTIFACTS:
                self.index.request()
            elif kind in self._debouncers:
                self._debouncers[kind].push([relative])
            if kind is Change.PROJECT:
                self._debouncers[Change.SOURCES].push([relative])

    def reload_project(self) -> None:
        """dbt_project.yml changed: follow its paths, and re-index if they moved."""
        assert self.project is not None and self.index is not None
        project = read_project(self.project.root)
        moved = (project.target_path, project.source_paths) != (
            self.project.target_path,
            self.project.source_paths,
        )
        self.project = self.index.project = project
        if moved:
            self.spawn(self.register_watchers())
            self.index.request()

    def sources_changed(self, files: set[str]) -> None:
        assert self.index is not None and self.project is not None
        if self.context is not None:
            for relative in files:
                self.context.locator.forget(self.project.root / relative)
        self.index.update_staleness(files)

    def rules_changed(self) -> None:
        self.load_rules()
        self.refresh_rules()
        self.notify_index_updated()

    def dags_changed(self, files: set[str]) -> None:
        # Phase 5 analyses DAG files statically; until then a change is only logged
        self.log(f"{len(files)} Airflow DAG file(s) changed; DAG analysis is not available yet")

    def release_graph(self) -> None:
        """The graph is being replaced: forget it, but keep the diagnostics until the next.

        Its memory is collected before the next graph loads, so the two never coexist.
        """
        self.context = None
        self.evaluation = None
        gc.unfreeze()
        gc.collect()

    def graph_changed(self) -> None:
        """A new graph is loaded, or none is."""
        assert self.index is not None and self.project is not None
        index = self.index
        graph = index.graph
        self.context = (
            None
            if graph is None
            else GraphContext(graph, self.project, lambda: index.columns is ColumnStage.COMPUTING)
        )
        freeze_heap()
        self.refresh_code_lenses()
        self.spawn(self.index.refresh_staleness())
        if self.context is None:
            self.refresh_rules()
        else:
            # Lineage is ready now; the rules (0.4 s at 5,000 models) follow, so the
            # status says ready and waiting requests are answered first
            self.spawn(self._rules_after(self.context))

    async def _rules_after(self, context: GraphContext) -> None:
        await asyncio.sleep(0)
        if self.context is not context:
            return  # another graph arrived meanwhile
        self.refresh_rules()
        gc.freeze()
        self.notify_index_updated()

    # Rules and diagnostics

    def rules_path(self) -> Path:
        assert self.project is not None
        return self.project.root / RULES_FILE

    def load_rules(self) -> None:
        """The workspace's rule pack, in a trusted workspace only (design section 10)."""
        path = self.rules_path()
        if not self.options.trusted or not path.is_file():
            self.rule_pack = None
            return
        self.rule_pack = load_rule_pack(path, {rule.id for rule in builtin_rules()})
        for problem in self.rule_pack.problems:
            self.log(f"Rule pack: {problem}", types.MessageType.Warning)

    def refresh_rules(self) -> None:
        """Evaluate the rules on the loaded graph and publish what changed."""
        if self.context is None:
            self.evaluation = None
            self.publish({})
            return
        self.evaluation = evaluate(self.context.graph, self.rule_pack)
        self.publish(
            features.diagnostics(
                self.context,
                self.evaluation,
                self.rule_pack,
                self.rules_path(),
                restricted=not self.options.trusted,
            )
        )

    def publish(self, found: dict[str, list[dict[str, Any]]]) -> None:
        """Send diagnostics for files whose diagnostics changed, and clear the rest."""
        # A fingerprint per file, not the diagnostics: there can be tens of thousands
        fingerprints = {uri: hash(json.dumps(d, sort_keys=True)) for uri, d in found.items()}
        for uri in sorted(set(self.published) - set(found)):
            self._send_diagnostics(uri, [])
        for uri, diagnostics in sorted(found.items()):
            if self.published.get(uri) != fingerprints[uri]:
                self._send_diagnostics(uri, diagnostics)
        self.published = fingerprints

    def _send_diagnostics(self, uri: str, diagnostics: list[dict[str, Any]]) -> None:
        # Plain JSON straight to the transport: at 5,000 models there can be tens of
        # thousands of diagnostics, and notify() would have cattrs convert typed objects.
        # pygls is pinned below 2.2 for this private call; the end-to-end tests cover it.
        self.protocol._send_data(
            {
                "jsonrpc": "2.0",
                "method": types.TEXT_DOCUMENT_PUBLISH_DIAGNOSTICS,
                "params": {"uri": uri, "diagnostics": diagnostics},
            }
        )

    def refresh_code_lenses(self) -> None:
        workspace = self.client_capabilities.workspace
        lens = workspace.code_lens if workspace else None
        if lens and lens.refresh_support:
            self.workspace_code_lens_refresh(None)

    def rules_status(self) -> dict[str, Any]:
        path = self.rules_path() if self.project else None
        score = self.evaluation.score() if self.evaluation else None
        pack = self.rule_pack
        return {
            "pack": str(path) if path is not None and path.is_file() else None,
            "packLoaded": pack is not None,
            "restricted": not self.options.trusted,
            "problems": [clip(p) for p in (pack.problems[:MAX_STATUS_ERRORS] if pack else [])],
            "findings": len(self.evaluation.findings) if self.evaluation else 0,
            "score": score.score if score else None,
            "coverage": score.coverage if score else None,
        }

    def notify_index_updated(self) -> None:
        self.protocol.notify(NOTIFY_INDEX_UPDATED, self.status())

    # Requests

    def status(self) -> dict[str, Any]:
        index = self.index
        project = self.project
        graph = index.graph if index else None
        stats = graph.stats() if graph is not None else None
        return {
            "protocolVersion": self.protocol_version,
            "server": {
                "version": __version__,
                "python": platform.python_version(),
                "executable": sys.executable,
                "pid": os.getpid(),
                "isolated": bool(sys.flags.isolated),
                "modules": _module_files(),
            },
            "project": {
                "root": str(project.root) if project else None,
                "name": (graph.project_name if graph else None)
                or (project.name if project else None),
                "problems": [clip(p) for p in project.problems[:100]] if project else [],
            },
            "trusted": self.options.trusted,
            "state": index.state.value if index else "starting",
            "indexing": bool(index and index.indexing),
            "generation": index.generation if index else 0,
            "columnLineage": index.columns.value if index else "pending",
            "message": clip(index.message) if index else None,
            "errors": [
                {**e, "message": clip(e.get("message")), "location": clip(e.get("location"))}
                for e in (index.errors[:MAX_STATUS_ERRORS] if index else [])
            ],
            "errorCount": len(index.errors) if index else 0,
            "artifacts": self._artifacts(),
            "counts": None
            if stats is None
            else {
                "nodes": stats["nodes"],
                "columns": stats["columns"],
                "edges": stats["edges"],
                "unresolved": stats["unresolved"],
                "nodeKinds": stats["node_kinds"],
            },
            "columnStates": index.column_states() if index else None,
            "stale": {"count": len(index.stale), "examples": sorted(index.stale)[:5]}
            if index
            else {"count": 0, "examples": []},
            "fromCache": bool(index and index.result.get("fromCache")),
            "indexedAt": index.indexed_at if index else None,
            "rules": self.rules_status(),
        }

    def _artifacts(self) -> dict[str, Any]:
        project, index = self.project, self.index
        graph = index.graph if index else None
        if project is None:
            return {"manifest": None, "catalog": None}
        manifest = graph.manifest if graph is not None else None
        return {
            "manifest": {
                "path": str(project.manifest_path),
                "exists": project.manifest_path.is_file(),
                "generatedAt": manifest.generated_at if manifest else None,
                "dbtVersion": manifest.dbt_version if manifest else None,
                "schemaVersion": graph.schema_version if graph is not None else None,
                "modifiedAt": _mtime_iso(project.manifest_path),
            },
            "catalog": {
                "path": str(project.catalog_path),
                "exists": project.catalog_path.is_file(),
                "modifiedAt": _mtime_iso(project.catalog_path),
            },
        }

    def refresh(self) -> dict[str, Any]:
        if self.index is not None:
            self.index.start()
        return self.status()

    def check_protocol(self) -> None:
        if self.protocol_version is None:
            raise JsonRpcServerError(
                f"no lineage protocol version in common: the client speaks "
                f"{self.options.protocol_versions or 'none'}, the server {list(PROTOCOL_VERSIONS)}",
                protocol.ErrorCode.PROTOCOL_MISMATCH,
            )

    def ready_context(self) -> GraphContext:
        """The loaded graph's context, or a NotReady error saying why there is none."""
        if self.context is None:
            index = self.index
            state = index.state.value if index else "starting"
            reason = (index.message if index else None) or (
                "the index is being updated"
                if index and index.state.value == "ready"
                else "the index is still being built"
            )
            raise JsonRpcServerError(
                f"lineage is not available ({state}): {reason}", protocol.ErrorCode.NOT_READY
            )
        return self.context

    def column_stage(self) -> str:
        return self.index.columns.value if self.index else ColumnStage.PENDING.value

    # Helpers

    def log(self, message: str, level: types.MessageType = types.MessageType.Info) -> None:
        """Log to the client's output (through ClientLogHandler, when main() set it up)."""
        if any(isinstance(h, ClientLogHandler) for h in logging.getLogger().handlers):
            logger.log(_LOG_LEVELS[level], "%s", message)
        else:
            self.window_log_message(types.LogMessageParams(type=level, message=message))

    def spawn(self, coroutine) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coroutine)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task


_LOG_LEVELS = {
    types.MessageType.Error: logging.ERROR,
    types.MessageType.Warning: logging.WARNING,
    types.MessageType.Info: logging.INFO,
    types.MessageType.Log: logging.DEBUG,
}


_REPORTED_MODULES = ("lineagepilot_lsp", "lineagepilot_core", "pygls", "sqlglot", "yaml")


def _module_files() -> dict[str, str]:
    """Where the key libraries were imported from (they all are, by the time this runs)."""
    return {
        name: str(getattr(sys.modules[name], "__file__", ""))
        for name in _REPORTED_MODULES
        if name in sys.modules
    }


def _mtime_iso(path: Path) -> str | None:
    try:
        return iso_time(path.stat().st_mtime)
    except OSError:
        return None


def create_server() -> LineagePilotServer:
    server = LineagePilotServer()

    @server.feature(types.INITIALIZE)
    def on_initialize(ls: LineagePilotServer, params: types.InitializeParams) -> None:
        ls.configure(params)

    @server.feature(types.INITIALIZED)
    async def on_initialized(ls: LineagePilotServer, params: types.InitializedParams) -> None:
        await ls.initialized()

    @server.feature(types.SHUTDOWN)
    async def on_shutdown(ls: LineagePilotServer, params: None) -> None:
        await ls.stop_indexing()

    @server.feature(types.WORKSPACE_DID_CHANGE_WATCHED_FILES)
    def on_files_changed(ls: LineagePilotServer, params: types.DidChangeWatchedFilesParams) -> None:
        ls.files_changed(params)

    @server.feature(types.TEXT_DOCUMENT_CODE_LENS)
    def on_code_lens(ls: LineagePilotServer, params: types.CodeLensParams) -> list[Any]:
        found = _document(ls, params.text_document.uri)
        if ls.context is None or found is None:
            return []
        return features.code_lenses(ls.context, *found)

    @server.feature(types.TEXT_DOCUMENT_HOVER)
    def on_hover(ls: LineagePilotServer, params: types.HoverParams) -> Any:
        found = _document(ls, params.text_document.uri)
        if ls.context is None or found is None:
            return None
        position = params.position
        return features.hover(ls.context, *found, position.line, position.character)

    @server.feature("lineage/status")
    def on_status(ls: LineagePilotServer, params: Any) -> dict[str, Any]:
        _params(ls, protocol.EmptyParams, params)
        return ls.status()

    @server.feature("lineage/refresh")
    def on_refresh(ls: LineagePilotServer, params: Any) -> dict[str, Any]:
        _params(ls, protocol.EmptyParams, params)
        return ls.refresh()

    @server.feature("lineage/graph")
    def on_graph(ls: LineagePilotServer, params: Any) -> dict[str, Any]:
        checked = _params(ls, protocol.GraphParams, params)
        return methods.graph(ls.ready_context(), checked)

    @server.feature("lineage/impact")
    def on_impact(ls: LineagePilotServer, params: Any) -> dict[str, Any]:
        checked = _params(ls, protocol.ImpactParams, params)
        return methods.impact(ls.ready_context(), checked, ls.column_stage())

    @server.feature("lineage/export")
    def on_export(ls: LineagePilotServer, params: Any) -> dict[str, Any]:
        checked = _params(ls, protocol.ExportParams, params)
        return methods.export(ls.ready_context(), checked)

    @server.feature("lineage/node")
    def on_node(ls: LineagePilotServer, params: Any) -> dict[str, Any]:
        checked = _params(ls, protocol.NodeParams, params)
        return methods.node(ls.ready_context(), checked)

    @server.feature("lineage/nodesForFile")
    def on_nodes_for_file(ls: LineagePilotServer, params: Any) -> dict[str, Any]:
        checked = _params(ls, protocol.NodesForFileParams, params)
        return methods.nodes_for_file(ls.ready_context(), checked)

    return server


def _document(ls: LineagePilotServer, uri: str) -> tuple[Path, str] | None:
    """A document's path and text: the editor's copy when open, else the file on disk."""
    document = ls.workspace.get_text_document(uri)
    if not document.path:
        return None
    try:
        return Path(document.path), document.source
    except (OSError, UnicodeDecodeError):  # deleted, unreadable or not text
        return None


def _params(ls: LineagePilotServer, record: type[protocol._Record], params: Any) -> Any:
    """Check the protocol version, then validate the params against the shared schema."""
    ls.check_protocol()
    try:
        return record.from_json(params)
    except protocol.ValidationError as e:
        raise JsonRpcInvalidParams(f"invalid params: {e}") from None


class ClientLogHandler(logging.Handler):
    """Send log records to the client as window/logMessage, with their levels.

    The client shows the server's stderr as errors whatever it says, so stderr carries only
    warnings and errors, for anything that happens before the connection is up.
    """

    def __init__(self, server: LineagePilotServer) -> None:
        super().__init__(logging.INFO)
        self.server = server

    def emit(self, record: logging.LogRecord) -> None:
        if self.server.protocol.writer is None or self.server.protocol._shutdown:
            return
        level = next(
            (t for t, n in _LOG_LEVELS.items() if n <= record.levelno), types.MessageType.Log
        )
        try:
            self.server.window_log_message(
                types.LogMessageParams(type=level, message=self.format(record))
            )
        except Exception:  # never let logging break the server
            self.handleError(record)


def main() -> None:
    server = create_server()
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setLevel(logging.WARNING)
    client = ClientLogHandler(server)
    for handler in (stderr, client):
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[stderr, client])
    # pygls logs every message at INFO
    logging.getLogger("pygls").setLevel(logging.WARNING)
    server.start_io()
