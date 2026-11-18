"""What the server watches, and what each change means (tasks 2.1 and 7.3).

The editor watches the files (``workspace/didChangeWatchedFiles``, registered by the
server), so the server holds no file-system watchers of its own. Changes are grouped
by kind and debounced, because dbt writes several artifacts in a row:

- **artifacts**: ``target/manifest.json`` and ``catalog.json``; re-index.
- **project**: ``dbt_project.yml``; re-read the project, and re-index if its paths moved.
- **sources**: files dbt reads to build the manifest; update the staleness warning.
- **rules**: ``.lineagepilot/rules.yml``; re-run the rules.
- **readers**: each reader's files, found by its globs (the Airflow DAG folders' Python files), and
  the annotation file, ``.lineagepilot/lineage.yml`` or ``.lineagepilot/airflow.yml``, which
  every reader reads; read them again. A module a DAG file imports from elsewhere is not watched:
  **Reindex** picks that up.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping, Sequence
from enum import Enum
from pathlib import Path

from lineagepilot_core.parsers.data_flow_annotations import ANNOTATION_FILES
from lineagepilot_core.readers.registry import reader
from lsprotocol import types

from lineagepilot_lsp.project import PROJECT_FILE, DbtProject, relative_to

RULES_FILE = ".lineagepilot/rules.yml"
# A change that every reader reads again for: the annotation file
ALL_READERS = "*"


class Change(str, Enum):
    ARTIFACTS = "artifacts"
    PROJECT = "project"
    SOURCES = "sources"
    RULES = "rules"
    READERS = "readers"


# The paths setting of each reader, by tool
ReaderPaths = Mapping[str, Sequence[str]]


def watchers(
    project: DbtProject, workspace: Path | None, readers: ReaderPaths
) -> list[types.FileSystemWatcher]:
    """File-system watchers to register with the client."""
    root = project.root.as_uri()
    patterns = [
        f"{project.target_path}/{{manifest,catalog}}.json",
        PROJECT_FILE,
        RULES_FILE,
        *ANNOTATION_FILES,
        *(f"{p}/**" if p != "." else "**/*.{sql,yml,yaml,py,csv,md}" for p in project.source_paths),
    ]
    found = [
        types.FileSystemWatcher(glob_pattern=types.RelativePattern(base_uri=root, pattern=p))
        for p in patterns
    ]
    if workspace is not None:
        found += [
            types.FileSystemWatcher(
                glob_pattern=types.RelativePattern(base_uri=workspace.as_uri(), pattern=pattern)
            )
            for tool, paths in readers.items()
            for pattern in reader(tool).watch_globs(paths)
        ]
    return found


def classify(
    project: DbtProject, workspace: Path | None, readers: ReaderPaths, path: Path
) -> tuple[Change, str] | None:
    """The kind of a changed file, and its path relative to the project. For a reader's file it
    is the reader's tool instead, or ``ALL_READERS``: what the debouncer collects is what to read
    again."""
    relative = project.relative(path)
    if relative is not None:
        if relative in (
            f"{project.target_path}/manifest.json",
            f"{project.target_path}/catalog.json",
        ):
            return Change.ARTIFACTS, relative
        if relative == RULES_FILE:
            return Change.RULES, relative
        if relative in ANNOTATION_FILES:
            return Change.READERS, ALL_READERS
        if relative == PROJECT_FILE:
            return Change.PROJECT, relative
        if project.is_source_file(relative) and not relative.startswith(f"{project.target_path}/"):
            return Change.SOURCES, relative
    if workspace is not None:
        in_workspace = relative_to(path, workspace)
        if in_workspace is None:
            return None
        owners = [tool for tool, paths in readers.items() if reader(tool).owns(in_workspace, paths)]
        if owners:
            return Change.READERS, owners[0] if len(owners) == 1 else ALL_READERS
    return None


class Debouncer:
    """Collects items and runs ``action`` once with all of them, ``delay_s`` after the last."""

    def __init__(self, delay_s: float, action: Callable[[set[str]], None]) -> None:
        self.delay_s = delay_s
        self.action = action
        self.pending: set[str] = set()
        self._handle: asyncio.TimerHandle | None = None

    def push(self, items: Iterable[str] = ()) -> None:
        self.pending.update(items)
        if self._handle is not None:
            self._handle.cancel()
        self._handle = asyncio.get_running_loop().call_later(self.delay_s, self._fire)

    def cancel(self) -> None:
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None
        self.pending.clear()

    def _fire(self) -> None:
        self._handle = None
        items, self.pending = self.pending, set()
        self.action(items)
