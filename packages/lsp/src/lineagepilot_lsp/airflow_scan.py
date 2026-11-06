"""Reading the DAG files, in a child process like the index (task 5.3, design section 11).

A scan depends on the manifest, whose selectors it evaluates, and on the DAG files, so it has its
own lifecycle. It is applied on top of whichever graph is loaded: a DAG file changes far more often
than the manifest, and re-indexing column lineage for it would take seconds at best. The scan has
no budget of its own on the event loop: at 5,000 models reading the manifest takes a second or two,
which is why it is a child process, and the graph answers meanwhile, saying the DAG files are still
being read.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any

from lineagepilot_lsp import indexing, worker
from lineagepilot_lsp.project import DbtProject

logger = logging.getLogger(__name__)

# Beyond this a scan is stopped and the review says it could not be read
SCAN_TIMEOUT_S = 120.0


class AirflowStage(str, Enum):
    PENDING = "pending"  # no scan has started
    SCANNING = "scanning"
    DONE = "done"
    FAILED = "failed"


class AirflowScanner:
    def __init__(
        self,
        project: DbtProject,
        workspace: Path,
        dag_folders: list[str],
        directory: Path,
        on_change: Callable[[], None],
        python: str | None = None,
    ) -> None:
        self.project = project
        self.workspace = workspace
        self.dag_folders = dag_folders
        self.directory = directory
        self.on_change = on_change
        self.python = python or worker.interpreter()
        self.stage = AirflowStage.PENDING
        self.message: str | None = None
        self.generation = 0
        self.scan: dict[str, Any] | None = None
        self.manifest_key: str | None = None
        self._task: asyncio.Task[None] | None = None

    def current_for(self, key: str | None) -> bool:
        """Whether the last scan is finished and read this manifest."""
        return self.stage is AirflowStage.DONE and self.manifest_key == key

    def scanning_for(self, key: str | None) -> bool:
        return self.stage is AirflowStage.SCANNING and self.manifest_key == key

    def release(self) -> None:
        """Nothing more is coming that needs this scan: it can be 30 MB at 5,000 models, and the
        server has a memory budget. If the graph is replaced, a new scan takes a second or two."""
        self.scan = None
        self.manifest_key = None

    def invalidate(self) -> None:
        """The DAG files may have changed: the scan held no longer counts."""
        self.manifest_key = None

    def start(self, key: str | None) -> None:
        """Start a scan now, cancelling any in progress."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self.generation += 1
        self.stage = AirflowStage.SCANNING
        self.message = None
        self.manifest_key = key
        self._task = asyncio.create_task(self._run(self.generation))

    async def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def wait(self) -> None:
        """Until the current scan, if any, has finished (for tests)."""
        if self._task is not None:
            await asyncio.shield(self._task)

    def request_body(self, output: Path) -> dict[str, Any]:
        return {
            "task": "airflow",
            "projectDir": str(self.project.root),
            "workspace": str(self.workspace),
            "dagFolders": self.dag_folders,
            "manifest": str(self.project.manifest_path),
            "output": str(output),
        }

    async def _run(self, generation: int) -> None:
        output = self.directory / f"scan-{generation}.json"
        found: dict[str, Any] = {}

        async def on_event(event: dict[str, Any]) -> None:
            found.update(event)

        problem = None
        scan = None
        command = worker.command(self.python, self.request_body(output))
        try:
            exit_ = await asyncio.wait_for(indexing.run_worker(command, on_event), SCAN_TIMEOUT_S)
        except asyncio.TimeoutError:
            problem = f"reading them took longer than {SCAN_TIMEOUT_S:.0f} s"
        except OSError as e:
            problem = f"the reader could not start: {e}"
        else:
            if found.get("event") != "airflow":
                problem = str(found.get("message") or f"the reader stopped ({exit_.returncode})")
            else:
                try:
                    scan = await asyncio.to_thread(_read, output)
                except (OSError, ValueError) as e:
                    problem = f"the scan could not be read: {type(e).__name__}"
        finally:
            output.unlink(missing_ok=True)
        if generation != self.generation:
            return
        if problem is not None:
            logger.warning("The DAG files could not be read: %s", problem)
            self.stage, self.message = AirflowStage.FAILED, problem
        else:
            self.stage, self.scan = AirflowStage.DONE, scan
        self.on_change()


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_bytes())
