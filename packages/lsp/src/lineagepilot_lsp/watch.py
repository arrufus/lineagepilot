"""What the server watches, and what each change means (task 2.1).

The editor watches the files (``workspace/didChangeWatchedFiles``, registered by the
server), so the server holds no file-system watchers of its own. Changes are grouped
by kind and debounced, because dbt writes several artifacts in a row:

- **artifacts**: ``target/manifest.json`` and ``catalog.json``; re-index.
- **project**: ``dbt_project.yml``; re-read the project, and re-index if its paths moved.
- **sources**: files dbt reads to build the manifest; update the staleness warning.
- **rules**: ``.lineagepilot/rules.yml``; re-run the rules.
- **reader**: a reader's files, found by its paths setting, and its annotation files: run that
  reader again (task 7.3). For Airflow, the Python files in the DAG folders and
  ``.lineagepilot/airflow.yml``. A module they import from elsewhere is not watched: **Reindex**
  picks that up.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Sequence
from enum import Enum
from pathlib import Path
from typing import NamedTuple

from lineagepilot_core.readers.contract import Reader
from lsprotocol import types

from lineagepilot_lsp.project import PROJECT_FILE, DbtProject, relative_to

RULES_FILE = ".lineagepilot/rules.yml"


class Change(str, Enum):
    ARTIFACTS = "artifacts"
    PROJECT = "project"
    SOURCES = "sources"
    RULES = "rules"
    READER = "reader"


# Each reader with its paths setting, already cleaned (Reader.clean_paths)
Watched = Sequence[tuple[Reader, list[str]]]


class Changed(NamedTuple):
    kind: Change
    relative: str  # to the project, or for a reader's file to the workspace
    tool: str | None = None  # whose file, for Change.READER


def watchers(
    project: DbtProject, workspace: Path | None, readers: Watched
) -> list[types.FileSystemWatcher]:
    """File-system watchers to register with the client."""
    root = project.root.as_uri()
    annotations = sorted({f for reader, _ in readers for f in reader.annotation_files})
    patterns = [
        f"{project.target_path}/{{manifest,catalog}}.json",
        PROJECT_FILE,
        RULES_FILE,
        *annotations,
        *(f"{p}/**" if p != "." else "**/*.{sql,yml,yaml,py,csv,md}" for p in project.source_paths),
    ]
    found = [
        types.FileSystemWatcher(glob_pattern=types.RelativePattern(base_uri=root, pattern=p))
        for p in patterns
    ]
    if workspace is not None:
        found += [
            types.FileSystemWatcher(
                glob_pattern=types.RelativePattern(base_uri=workspace.as_uri(), pattern=glob)
            )
            for reader, paths in readers
            for glob in reader.watch_globs(paths)
        ]
    return found


def classify(
    project: DbtProject, workspace: Path | None, readers: Watched, path: Path
) -> Changed | None:
    """The kind of a changed file, and its path relative to the project (or workspace)."""
    relative = project.relative(path)
    if relative is not None:
        if relative in (
            f"{project.target_path}/manifest.json",
            f"{project.target_path}/catalog.json",
        ):
            return Changed(Change.ARTIFACTS, relative)
        if relative == RULES_FILE:
            return Changed(Change.RULES, relative)
        for reader, _ in readers:
            if relative in reader.annotation_files:
                return Changed(Change.READER, relative, reader.tool)
        if relative == PROJECT_FILE:
            return Changed(Change.PROJECT, relative)
        if project.is_source_file(relative) and not relative.startswith(f"{project.target_path}/"):
            return Changed(Change.SOURCES, relative)
    if workspace is not None:
        in_workspace = relative_to(path, workspace)
        if in_workspace is None:
            return None
        for reader, paths in readers:
            if reader.watches(in_workspace, paths):
                return Changed(Change.READER, in_workspace, reader.tool)
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
