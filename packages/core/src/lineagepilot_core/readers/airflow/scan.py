"""One scan for one dbt project: its annotation file, then its DAG files (tasks 5.1 and 5.2).

The workspace and the annotation file are shared by every reader (``readers.common.workspace``).
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from lineagepilot_core.parsers.data_flow_annotations import (
    DEFAULT_TOOL,
    DataFlowAnnotationLoader,
)
from lineagepilot_core.readers.airflow.dag_static import DEFAULT_DAG_FOLDERS, scan_dags
from lineagepilot_core.readers.common.safe_io import to_posix
from lineagepilot_core.readers.common.workspace import (
    load_annotations,
    project_and_workspace,
    resolve_path_map,
)


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
    root, space = project_and_workspace(project_root, workspace)
    loader = DataFlowAnnotationLoader()
    file = load_annotations(loader, root, space)
    path_map, problems = resolve_path_map(loader.path_map, root, space)
    manifests = {root: str(manifest)} if manifest else None
    result = scan_dags(space, dag_folders, manifests, path_map, focus_project=root)
    relative = to_posix(os.path.relpath(root, space))
    result["project"] = relative
    result["annotations"] = {
        "file": file,
        "tasks": [a.to_json() for a in loader.for_tool(DEFAULT_TOOL)],
        "consumers": [c.to_json() for c in loader.get_consumers().values()],
        "acknowledged": loader.acknowledged,
        "problems": loader.problems + problems + _other_tools(loader),
    }
    return result


def _other_tools(loader: DataFlowAnnotationLoader) -> list[str]:
    """Tasks declared for a tool with no reader yet are kept out of the graph, and said so. A
    tool that has a reader, such as Dagster (task 8.8), links its own."""
    from lineagepilot_core.readers.registry import READERS  # the registry imports this reader

    counts: dict[str, int] = {}
    for annotation in loader.get_all_annotations().values():
        if annotation.tool not in READERS:
            counts[annotation.tool] = counts.get(annotation.tool, 0) + 1
    return [
        f"{n} task{'s' if n > 1 else ''} with tool {tool}: no reader for {tool} yet; ignored"
        for tool, n in sorted(counts.items())
    ]
