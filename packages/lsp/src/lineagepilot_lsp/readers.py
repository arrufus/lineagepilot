"""Running a reader, in a child process like the index (tasks 5.3 and 7.3, design section 11).

One runner per reader (``lineagepilot_core.readers.registry``), the same for every tool. A read
depends on the manifest, whose selectors it may evaluate, and on the tool's files, so it has its
own lifecycle. It is applied on top of whichever graph is loaded: a DAG file changes far more often
than the manifest, and re-indexing column lineage for it would take seconds at best. A read has
no budget of its own on the event loop: at 5,000 models reading the manifest takes a second or two,
which is why it is a child process, with a time limit and a memory limit, and the graph answers
meanwhile, saying the files are still being read.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any

from lineagepilot_core.readers.contract import Reader

from lineagepilot_lsp import indexing, worker
from lineagepilot_lsp.project import DbtProject

logger = logging.getLogger(__name__)

# Beyond this a read is stopped and the review says the files could not be read
SCAN_TIMEOUT_S = 120.0
# The scan is read into the server, which has a memory budget: about 30 MB at 5,000 models
MAX_SCAN_BYTES = 128 * 2**20


class ScanTooLarge(ValueError):
    pass


class ReaderStage(str, Enum):
    PENDING = "pending"  # no scan has started
    SCANNING = "scanning"
    DONE = "done"
    FAILED = "failed"


class ReaderRunner:
    """Reads one tool's files for one project, and holds the result until it is linked."""

    def __init__(
        self,
        reader: Reader,
        project: DbtProject,
        workspace: Path,
        paths: list[str],
        directory: Path,
        on_change: Callable[[], None],
        python: str | None = None,
    ) -> None:
        self.reader = reader
        self.project = project
        self.workspace = workspace
        self.paths = paths
        self.directory = directory
        self.on_change = on_change
        self.python = python or worker.interpreter()
        self.stage = ReaderStage.PENDING
        self.message: str | None = None
        self.generation = 0
        self.scan: dict[str, Any] | None = None
        self.manifest_key: str | None = None
        self._task: asyncio.Task[None] | None = None

    def current_for(self, key: str | None) -> bool:
        """Whether the last scan is finished and read this manifest."""
        return self.stage is ReaderStage.DONE and self.manifest_key == key

    def scanning_for(self, key: str | None) -> bool:
        return self.stage is ReaderStage.SCANNING and self.manifest_key == key

    def release(self) -> None:
        """Nothing more is coming that needs this scan: it can be 30 MB at 5,000 models, and the
        server has a memory budget. If the graph is replaced, a new scan takes a second or two."""
        self.scan = None
        self.manifest_key = None

    def invalidate(self) -> None:
        """The reader's files may have changed: the scan held no longer counts."""
        self.manifest_key = None

    def start(self, key: str | None) -> None:
        """Start a scan now, cancelling any in progress."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self.generation += 1
        self.stage = ReaderStage.SCANNING
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
            except Exception:  # the scan's own failure must not keep the server from stopping
                logger.exception("A reader failed as it was stopped")

    async def wait(self) -> None:
        """Until the current scan, if any, has finished (for tests)."""
        if self._task is not None:
            await asyncio.shield(self._task)

    def request_body(self, output: Path) -> dict[str, Any]:
        return {
            "task": "read",
            "tool": self.reader.tool,
            "projectDir": str(self.project.root),
            "workspace": str(self.workspace),
            "paths": self.paths,
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
            if exit_.over_memory:
                problem = exit_.stopped("the reader")
            elif found.get("event") != "read":
                problem = str(found.get("message") or f"the reader stopped ({exit_.returncode})")
            else:
                try:
                    scan = await asyncio.to_thread(_read, output)
                except ScanTooLarge as e:
                    problem = f"the scan could not be read: {e}"
                except (OSError, ValueError) as e:
                    problem = f"the scan could not be read: {type(e).__name__}"
        finally:
            # On Windows a file still open (a read on a thread, when this task is cancelled at
            # shutdown) cannot be deleted. An error here would escape stop() and stall shutdown.
            with contextlib.suppress(OSError):
                output.unlink(missing_ok=True)
        if generation != self.generation:
            return
        if problem is not None:
            logger.warning("%s could not be read: %s", self.reader.files.capitalize(), problem)
            self.stage, self.message = ReaderStage.FAILED, problem
        else:
            self.stage, self.scan = ReaderStage.DONE, scan
        self.on_change()


def _read(path: Path) -> dict[str, Any]:
    with path.open("rb") as f:
        size = os.fstat(f.fileno()).st_size
        if size > MAX_SCAN_BYTES:
            raise ScanTooLarge(f"it is over {MAX_SCAN_BYTES // 2**20} MB")
        # What fstat said, and one more to see it grew: a read of the cap would allocate it
        data = f.read(size + 1)
    if len(data) > size:
        raise ScanTooLarge("it changed while it was read")
    return json.loads(data)
