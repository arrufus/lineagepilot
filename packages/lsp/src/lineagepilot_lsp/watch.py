"""What the server watches, and what each change means (task 2.1).

The editor watches the files (``workspace/didChangeWatchedFiles``, registered by the
server), so the server holds no file-system watchers of its own. Changes are grouped
by kind and debounced, because dbt writes several artifacts in a row:

- **artifacts**: ``target/manifest.json`` and ``catalog.json``; re-index.
- **project**: ``dbt_project.yml``; re-read the project, and re-index if its paths moved.
- **sources**: files dbt reads to build the manifest; update the staleness warning.
- **rules**: ``.lineagepilot/rules.yml``; re-run the rules.
- **dags**: Python files in the Airflow DAG folders, and ``.lineagepilot/airflow.yml``; read them
  again. A module they import from elsewhere is not watched: **Reindex** picks that up.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from enum import Enum
from pathlib import Path

from lineagepilot_core.airflow.dag_static import clean_dag_folder, in_dag_folder
from lineagepilot_core.airflow.workspace import ANNOTATION_FILE
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
        ANNOTATION_FILE,
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
            for folder in clean_folders(dag_folders)
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
        if relative == ANNOTATION_FILE:
            return Change.DAGS, relative
        if relative == PROJECT_FILE:
            return Change.PROJECT, relative
        if project.is_source_file(relative) and not relative.startswith(f"{project.target_path}/"):
            return Change.SOURCES, relative
    if workspace is not None and path.suffix == ".py":
        in_workspace = relative_to(path, workspace)
        if in_workspace is None:
            return None
        if in_dag_folder(in_workspace, clean_folders(dag_folders)):
            return Change.DAGS, in_workspace
    return None


def clean_folders(folders: Iterable[str]) -> list[str]:
    """Relative folders or patterns only: a setting cannot point outside the workspace."""
    return [f for f in (clean_dag_folder(folder) for folder in folders) if f]


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
