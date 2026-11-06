"""One scan for one dbt project: its annotation file, then its DAG files (tasks 5.1 and 5.2).

The workspace is what the DAG files and modules they import must be inside; the project is what
links are wanted for. In a repository with ``dags/`` beside ``dbt/`` they differ.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from lineagepilot_core.airflow.dag_static import DEFAULT_DAG_FOLDERS, scan_dags
from lineagepilot_core.airflow.safe_io import is_inside, to_posix
from lineagepilot_core.parsers.data_flow_annotations import DataFlowAnnotationLoader

ANNOTATION_FILE = ".lineagepilot/airflow.yml"


def default_workspace(project_root: str | Path) -> Path:
    """The repository a project is in: DAG files are usually beside it, not in it. It is only
    looked for, not asked of git, so nothing runs for this."""
    root = Path(os.path.realpath(project_root))
    for folder in (root, *root.parents):
        if (folder / ".git").exists():
            return folder
    return root


def scan_workspace(
    workspace: str | Path, dag_folders: Iterable[str] = DEFAULT_DAG_FOLDERS
) -> dict[str, Any]:
    """Every dbt project's links, with no project in focus. Nothing can be linked from this: it is
    what ``lineagepilot airflow`` reports, and what the held-out corpus is scored on."""
    return {**scan_dags(os.path.realpath(workspace), dag_folders), "project": None}


def scan_airflow(
    project_root: str | Path,
    workspace: str | Path | None = None,
    dag_folders: Iterable[str] = DEFAULT_DAG_FOLDERS,
    manifest: str | Path | None = None,
) -> dict[str, Any]:
    """The DAG files' links for this project, as JSON: read them with ``link_airflow``."""
    root = os.path.realpath(project_root)
    space = os.path.realpath(workspace) if workspace else str(default_workspace(root))
    if not is_inside(root, space):
        space = root
    loader = DataFlowAnnotationLoader()
    annotation = os.path.join(root, ANNOTATION_FILE)
    file = None
    if os.path.isfile(annotation):
        loader.load_annotations(annotation)
        file = to_posix(os.path.relpath(annotation, space))
    path_map, problems = _path_map(loader.path_map, root, space)
    manifests = {root: str(manifest)} if manifest else None
    result = scan_dags(space, dag_folders, manifests, path_map, focus_project=root)
    relative = to_posix(os.path.relpath(root, space))
    result["project"] = relative
    result["annotations"] = {
        "file": file,
        "tasks": [a.to_json() for a in loader.get_all_annotations().values()],
        "acknowledged": loader.acknowledged,
        "problems": loader.problems + problems,
    }
    return result


def _path_map(entries: dict[str, str], root: str, space: str) -> tuple[dict[str, str], list[str]]:
    """Each target is relative to the project, and must stay in the workspace."""
    found: dict[str, str] = {}
    problems: list[str] = []
    for prefix, target in sorted(entries.items()):
        path = os.path.realpath(os.path.join(root, target))
        if is_inside(path, space):
            found[prefix] = path
        else:
            problems.append(f"path_map: {prefix!r} leaves the workspace; ignored")
    return found, problems
