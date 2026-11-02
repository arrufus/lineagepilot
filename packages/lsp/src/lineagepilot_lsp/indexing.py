"""Indexing one dbt project: run the indexer process, load the graph it produces, and
re-index when the artifacts change (task 2.1, design section 11).

The indexer runs in a child process (``worker.py``) and hands the graph over as a
snapshot file. The model-level graph arrives first and the full graph, with column
lineage, after it. While a re-index runs, the previous graph keeps answering. When the
artifacts cannot be trusted the graph is dropped and the state is ``unknown``, with the
reasons: never an empty or partial graph.
"""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import sys
import time
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from lineagepilot_core.graph.model import MetadataGraph
from lineagepilot_core.indexer import DEFAULT_MAX_ARTIFACT_BYTES
from lineagepilot_core.snapshot import SnapshotError, load_snapshot

from lineagepilot_lsp import worker
from lineagepilot_lsp.project import DbtProject

logger = logging.getLogger(__name__)

# An indexer event is one line of JSON; a project with many warnings makes long ones
EVENT_LINE_LIMIT = 32 * 2**20
# Clock skew and coarse file-system timestamps: a source file must be newer than the
# manifest by more than this to count as stale
STALE_TOLERANCE_S = 2.0
MAX_STALE_SCAN = 100_000


class IndexState(str, Enum):
    STARTING = "starting"  # nothing has finished yet
    READY = "ready"  # a graph is loaded
    UNKNOWN = "unknown"  # the artifacts are missing or cannot be trusted; no graph
    FAILED = "failed"  # the indexer or the snapshot failed; no graph


class ColumnStage(str, Enum):
    PENDING = "pending"  # the model-level graph is not loaded yet
    COMPUTING = "computing"  # the model-level graph is loaded; column lineage is being added
    DONE = "done"
    OFF = "off"  # column lineage is turned off


@dataclass
class WorkerExit:
    returncode: int | None
    stderr_tail: list[str] = field(default_factory=list)


async def run_worker(
    cmd: list[str],
    on_event: Callable[[dict[str, Any]], Awaitable[None]],
    on_stderr: Callable[[str], None] | None = None,
) -> WorkerExit:
    """Run the indexer and pass each event to ``on_event`` as it arrives.

    Cancelling the caller kills the process.
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=EVENT_LINE_LIMIT,
    )
    tail: deque[str] = deque(maxlen=20)

    async def drain() -> None:
        assert proc.stderr is not None
        async for raw in proc.stderr:
            line = raw.decode("utf-8", errors="replace").rstrip()
            tail.append(line)
            if on_stderr:
                on_stderr(line)

    stderr_task = asyncio.create_task(drain())
    try:
        assert proc.stdout is not None
        async for raw in proc.stdout:
            try:
                event = json.loads(raw)
            except ValueError:
                logger.warning("Ignoring indexer output that is not JSON: %.200r", raw)
                continue
            if isinstance(event, dict):
                await on_event(event)
        await stderr_task
        return WorkerExit(await proc.wait(), list(tail))
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        stderr_task.cancel()


def freeze_heap() -> None:
    """Collect, then move every object to the permanent generation (design section 11).

    The graph holds most of the heap. Without this, Python 3.10's full collections over
    a 5,000-model graph paused queries for 200-285 ms. Unfreezing first lets cycles left
    by the previous graph be collected.
    """
    gc.unfreeze()
    gc.collect()
    gc.freeze()


def stale_files(project: DbtProject, since: float, limit: int = MAX_STALE_SCAN) -> list[str]:
    """Source files modified after ``since`` (an mtime), relative to the project root."""
    found: list[str] = []
    seen = 0
    for source in project.source_paths:
        top = project.root / source
        for directory, dirnames, filenames in os.walk(top):
            dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
            for name in sorted(filenames):
                seen += 1
                if seen > limit:
                    return found
                path = Path(directory) / name
                if _newer(path, since):
                    relative = project.relative(path)
                    if relative:
                        found.append(relative)
    project_file = project.root / "dbt_project.yml"
    if _newer(project_file, since):
        found.append("dbt_project.yml")
    return sorted(set(found))


def _newer(path: Path, since: float) -> bool:
    try:
        return path.stat().st_mtime > since + STALE_TOLERANCE_S
    except OSError:
        return False


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def iso_time(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(timespec="seconds")


class ProjectIndex:
    """The graph of one project and the index runs that produce it."""

    def __init__(
        self,
        project: DbtProject,
        cache_dir: Path,
        *,
        column_lineage: bool = True,
        max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
        debounce_s: float = 1.0,
        python: str = sys.executable,
        on_status: Callable[[], None] = lambda: None,
        on_graph: Callable[[], None] = lambda: None,
    ) -> None:
        self.project = project
        self.cache_dir = cache_dir
        self.column_lineage = column_lineage
        self.max_artifact_bytes = max_artifact_bytes
        self.debounce_s = debounce_s
        self.python = python
        self.on_status = on_status  # the state, the counts or the staleness changed
        self.on_graph = on_graph  # a different graph is loaded, or none is

        self.graph: MetadataGraph | None = None
        self.state = IndexState.STARTING
        self.columns = ColumnStage.OFF if not column_lineage else ColumnStage.PENDING
        self.indexing = False
        self.generation = 0  # one per index run started
        self.message: str | None = None
        self.errors: list[dict[str, Any]] = []
        self.result: dict[str, Any] = {}  # the latest indexer event's description
        self.indexed_at: str | None = None
        self.manifest_mtime: float | None = None
        self.stale: set[str] = set()

        self._task: asyncio.Task[None] | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._finished = 0  # the latest generation that reported its outcome

    # Running

    def request(self) -> None:
        """Re-index after ``debounce_s`` without further requests."""
        if self._timer is not None:
            self._timer.cancel()
        self._timer = asyncio.get_running_loop().call_later(self.debounce_s, self.start)

    def start(self) -> None:
        """Start an index run now, cancelling any run in progress."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self.generation += 1
        self.indexing = True
        self.columns = ColumnStage.PENDING if self.column_lineage else ColumnStage.OFF
        self._task = asyncio.create_task(self._run(self.generation))
        self.on_status()

    async def stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def wait(self) -> None:
        """Until the current run, if any, has finished (for tests)."""
        if self._task is not None:
            await asyncio.shield(self._task)

    def request_body(self) -> dict[str, Any]:
        return {
            "projectDir": str(self.project.root),
            "manifest": str(self.project.manifest_path),
            "catalog": str(self.project.catalog_path)
            if self.project.catalog_path.is_file()
            else None,
            "cacheDir": str(self.cache_dir),
            "columnLineage": self.column_lineage,
            "maxArtifactBytes": self.max_artifact_bytes,
        }

    async def _run(self, generation: int) -> None:
        started = time.perf_counter()
        cmd = worker.command(self.python, self.request_body())
        try:
            exit_ = await run_worker(
                cmd,
                lambda event: self._on_event(generation, event),
                lambda line: logger.info("indexer: %s", line),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # the interpreter could not start, or the output broke
            logger.exception("The indexer could not run")
            self._fail(generation, f"the indexer could not run: {type(e).__name__}: {e}", [])
            return
        if generation == self.generation and self._finished != generation:
            detail = f" (exit code {exit_.returncode})" if exit_.returncode else ""
            self._fail(
                generation, f"the indexer stopped without a result{detail}", exit_.stderr_tail
            )
        logger.info("Index run %d took %.2f s", generation, time.perf_counter() - started)

    async def _on_event(self, generation: int, event: dict[str, Any]) -> None:
        if generation != self.generation:
            return
        kind = event.get("event")
        if kind == "failed":
            self._finished = generation
            self._fail(generation, f"the indexer failed: {event.get('message')}", [])
        elif kind == "model":
            await self._load(generation, event, ColumnStage.COMPUTING)
        elif kind == "done":
            self._finished = generation
            if event.get("status") == "ok":
                stage = ColumnStage.DONE if self.column_lineage else ColumnStage.OFF
                await self._load(generation, event, stage, final=True)
            else:
                self.indexing = False
                self._set(None, IndexState.UNKNOWN, event, _unknown_message(event))
        else:
            logger.warning("Ignoring unknown indexer event %.80r", kind)

    async def _load(
        self, generation: int, event: dict[str, Any], stage: ColumnStage, final: bool = False
    ) -> None:
        path = event.get("snapshot")
        try:
            if not isinstance(path, str):
                raise SnapshotError("the indexer reported no snapshot")
            graph = await asyncio.to_thread(load_snapshot, path)
        except SnapshotError as e:
            self._fail(generation, f"the index could not be loaded: {e}", [])
            return
        if generation != self.generation:
            return
        if final:
            self.indexing = False
        self.columns = stage
        self._set(graph, IndexState.READY, event, None)

    def _fail(self, generation: int, message: str, details: list[str]) -> None:
        if generation != self.generation:
            return
        self._finished = generation
        self.indexing = False
        errors = [{"severity": "error", "message": line, "location": None} for line in details]
        self._set(None, IndexState.FAILED, {"errors": errors}, message)

    def _set(
        self,
        graph: MetadataGraph | None,
        state: IndexState,
        event: dict[str, Any],
        message: str | None,
    ) -> None:
        changed = graph is not self.graph
        self.graph = graph
        self.state = state
        self.message = message
        self.result = {k: v for k, v in event.items() if k not in ("event", "errors")}
        self.errors = list(event.get("errors") or [])
        self.indexed_at = iso_time(time.time())
        self.manifest_mtime = _mtime(self.project.manifest_path) if graph is not None else None
        if graph is None:
            self.columns = ColumnStage.OFF if not self.column_lineage else ColumnStage.PENDING
        if changed:
            self.on_graph()
        self.on_status()

    # Staleness

    async def refresh_staleness(self) -> None:
        """Rescan the source files against the manifest's modification time."""
        since = self.manifest_mtime
        if since is None:
            stale: set[str] = set()
        else:
            stale = set(await asyncio.to_thread(stale_files, self.project, since))
        if stale != self.stale:
            self.stale = stale
            self.on_status()

    def update_staleness(self, relatives: Iterable[str]) -> None:
        """Re-check the given source files, after file events."""
        stale = set(self.stale)
        for relative in relatives:
            if self.manifest_mtime is not None and _newer(
                self.project.root / relative, self.manifest_mtime
            ):
                stale.add(relative)
            else:
                stale.discard(relative)
        if stale != self.stale:
            self.stale = stale
            self.on_status()

    # Reporting

    def column_states(self) -> dict[str, int] | None:
        if self.graph is None:
            return None
        counts = Counter(s.state.value for s in self.graph.column_lineage.values())
        return {state: counts.get(state, 0) for state in ("resolved", "partial", "unknown")}


def _unknown_message(event: dict[str, Any]) -> str:
    errors = [
        e.get("message", "") for e in event.get("errors") or [] if e.get("severity") == "error"
    ]
    return errors[0] if errors else "the dbt artifacts cannot be trusted"
