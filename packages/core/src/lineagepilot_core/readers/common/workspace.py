"""The workspace a reader reads in, and the project's annotation file (tasks 5.1, 5.2 and 7.4).

The workspace is what a tool's files, and the modules they import, must be inside; the project is
what links are wanted for. In a repository with ``dags/`` beside ``dbt/`` they differ.
"""

from __future__ import annotations

import os
from pathlib import Path

from lineagepilot_core.parsers.data_flow_annotations import (
    ANNOTATION_FILES,
    DataFlowAnnotationLoader,
    choose_file,
)
from lineagepilot_core.readers.common.safe_io import (
    is_inside,
    regular_file_inside,
    resolve_inside,
    to_posix,
)


def default_workspace(project_root: str | Path) -> Path:
    """The repository a project is in: DAG files are usually beside it, not in it. It is only
    looked for, not asked of git, so nothing runs for this."""
    root = Path(os.path.realpath(project_root))
    for folder in (root, *root.parents):
        if (folder / ".git").exists():
            return folder
    return root


def project_and_workspace(
    project_root: str | Path, workspace: str | Path | None = None
) -> tuple[str, str]:
    """Both as real paths. A workspace that does not hold the project is replaced by the project."""
    root = os.path.realpath(project_root)
    space = os.path.realpath(workspace) if workspace else str(default_workspace(root))
    return root, space if is_inside(root, space) else root


def load_annotations(loader: DataFlowAnnotationLoader, root: str, space: str) -> str | None:
    """Read the project's annotation file into ``loader``, and return its path in the workspace.

    ``lineage.yml`` is read if it exists, even if it cannot be used, and ``airflow.yml`` only
    without it (D-14): falling back would make which file counts depend on whether it parses.
    """
    paths = {name: os.path.join(root, name) for name in ANNOTATION_FILES}
    name, ignored = choose_file(name for name, path in paths.items() if os.path.lexists(path))
    loader.problems.extend(ignored)
    if name is None:
        return None
    # A link to a file outside the workspace is not read: what it holds would reach the review
    real = regular_file_inside(paths[name], space)
    if real is None:
        loader.problems.append(f"{name} is not a regular file in the workspace")
        return None
    loader.load_annotations(real)
    return to_posix(os.path.relpath(paths[name], space))


def resolve_path_map(
    entries: dict[str, str], root: str, space: str
) -> tuple[dict[str, str], list[str]]:
    """Each target is relative to the project, and must stay in the workspace. It is checked as a
    string first: on Windows a target of ``//host/share`` is a UNC path."""
    found: dict[str, str] = {}
    problems: list[str] = []
    for prefix, target in sorted(entries.items()):
        path = resolve_inside(os.path.join(root, target), space)
        if path is not None:
            found[prefix] = path
        else:
            problems.append(f"path_map: {prefix!r} leaves the workspace; ignored")
    return found, problems
