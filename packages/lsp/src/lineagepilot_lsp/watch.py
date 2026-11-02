"""What the server watches, and what each change means (task 2.1).

The editor watches the files (``workspace/didChangeWatchedFiles``, registered by the
server), so the server holds no file-system watchers of its own. Changes are grouped
by kind and debounced, because dbt writes several artifacts in a row:

- **artifacts**: ``target/manifest.json`` and ``catalog.json``; re-index.
- **project**: ``dbt_project.yml``; re-read the project, and re-index if its paths moved.
- **sources**: files dbt reads to build the manifest; update the staleness warning.
- **rules**: ``.lineagepilot/rules.yml``; re-run the rules.
- **dags**: Python files in the Airflow DAG folders; analysed from Phase 5.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from enum import Enum
from pathlib import Path, PurePosixPath

from lsprotocol import types

from lineagepilot_lsp.project import PROJECT_FILE, DbtProject, relative_to

RULES_FILE = ".lineagepilot/rules.yml"


class Change(str, Enum):
    ARTIFACTS = "artifacts"
    PROJECT = "project"
    SOURCES = "sources"
    RULES = "rules"
    DAGS = "dags"


def watchers(
    project: DbtProject, workspace: Path | None, dag_folders: Iterable[str]
) -> list[types.FileSystemWatcher]:
    """File-system watchers to register with the client."""
    root = project.root.as_uri()
    patterns = [
        f"{project.target_path}/{{manifest,catalog}}.json",
        PROJECT_FILE,
        RULES_FILE,
        *(f"{p}/**" if p != "." else "**/*.{sql,yml,yaml,py,csv,md}" for p in project.source_paths),
    ]
    found = [
        types.FileSystemWatcher(glob_pattern=types.RelativePattern(base_uri=root, pattern=p))
        for p in patterns
    ]
    if workspace is not None:
        found += [
            types.FileSystemWatcher(
                glob_pattern=types.RelativePattern(
                    base_uri=workspace.as_uri(), pattern=f"{folder}/**/*.py"
                )
            )
            for folder in _clean_folders(dag_folders)
        ]
    return found


def classify(
    project: DbtProject, workspace: Path | None, dag_folders: Iterable[str], path: Path
) -> tuple[Change, str] | None:
    """The kind of a changed file, and its path relative to the project (or workspace)."""
    relative = project.relative(path)
    if relative is not None:
        if relative in (
            f"{project.target_path}/manifest.json",
            f"{project.target_path}/catalog.json",
        ):
            return Change.ARTIFACTS, relative
        if relative == RULES_FILE:
            return Change.RULES, relative
        if relative == PROJECT_FILE:
            return Change.PROJECT, relative
        if project.is_source_file(relative) and not relative.startswith(f"{project.target_path}/"):
            return Change.SOURCES, relative
    if workspace is not None and path.suffix == ".py":
        in_workspace = relative_to(path, workspace)
        if in_workspace is None:
            return None
        for folder in _clean_folders(dag_folders):
            if in_workspace.startswith(f"{folder}/"):
                return Change.DAGS, in_workspace
    return None


def _clean_folders(folders: Iterable[str]) -> list[str]:
    """Relative folder names only: a DAG folder setting cannot point outside the workspace."""
    clean = []
    for folder in folders:
        if not isinstance(folder, str):
            continue
        posix = PurePosixPath(folder.strip().replace("\\", "/"))
        if not posix.parts or posix.is_absolute() or ".." in posix.parts or ":" in posix.parts[0]:
            continue
        clean.append(posix.as_posix().rstrip("/"))
    return clean


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
