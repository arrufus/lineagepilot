"""Where a workspace's Dagster definitions are (task 8.2).

The setting ``lineagePilot.dagster.paths`` names folders, relative to the workspace, as the DAG
scan's ``dagFolders`` does. Dagster has no folder name everyone uses, so by default it is the whole
workspace. In those folders the reader finds:

- ``defs.yaml`` files: a ``dg`` project's components, such as ``DbtProjectComponent`` (task 8.3);
- ``pyproject.toml`` files with ``[tool.dg]`` or ``[tool.dagster]``, which name code locations;
- ``workspace.yaml`` and ``dagster_cloud.yaml``, which name code locations too;
- Python files that import Dagster, checked as text before anything parses them (task 8.4).

Nothing outside the workspace is listed, links out of it are not followed, and the walk is bounded
by the shared folder rules and by a number of files.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from lineagepilot_core.readers.common.folders import (
    SKIP_SCAN_DIRS,
    WORKSPACE,
    clean_folder,
    resolve_folders,
)
from lineagepilot_core.readers.common.safe_io import (
    is_regular_file,
    read_capped,
    resolve_inside,
    to_posix,
    walk_inside,
)

DEFAULT_PATHS = (WORKSPACE,)
COMPONENT_FILE = "defs.yaml"
PROJECT_FILE = "pyproject.toml"
WORKSPACE_FILES = ("workspace.yaml", "dagster_cloud.yaml")
# A file over this is not read, as with DAG files
FILE_SIZE_CAP = 2 * 1024 * 1024
# Files of any of the kinds above, in one workspace: enough for any real repository
MAX_FILES = 5_000
# Folders in the setting, as the extension's manifest bounds it too
MAX_PATHS = 50
# A Python file is one of Dagster's when a line imports dagster, or a package such as dagster_dbt,
# by its whole name: dagsterlike is not one
_DAGSTER_MODULE = rb"dagster(?:_\w+)?\b"
_IMPORTS_DAGSTER = re.compile(
    rb"^[ \t]*(?:from[ \t]+"
    + _DAGSTER_MODULE
    + rb"|import[ \t][^\n#]*\b"
    + _DAGSTER_MODULE
    + rb")",
    re.M,
)
_NAMES_TOOL = re.compile(rb"^[ \t]*\[tool\.(?:dg|dagster)\b", re.M)


@dataclass
class Found:
    """What discovery found, each file relative to the workspace."""

    paths: list[str]  # the setting, cleaned: where it looked
    components: list[str] = field(default_factory=list)
    projects: list[str] = field(default_factory=list)
    workspaces: list[str] = field(default_factory=list)
    python: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    @property
    def files(self) -> int:
        return len(self.components) + len(self.projects) + len(self.workspaces) + len(self.python)

    def to_json(self) -> dict[str, Any]:
        return {
            "components": self.components,
            "projects": self.projects,
            "workspaces": self.workspaces,
            "python": self.python,
        }


def clean_paths(paths: Iterable[Any]) -> list[str]:
    """The setting's folders that are safe to use, each once, in order, and no more than
    ``MAX_PATHS``: a workspace's own settings choose them, and each is walked."""
    return list(dict.fromkeys(f for f in (clean_folder(p) for p in paths) if f))[:MAX_PATHS]


def discover(workspace: str, paths: Iterable[Any]) -> Found:
    """The Dagster files under ``paths`` in ``workspace``."""
    space = os.path.realpath(workspace)
    found = Found(paths=clean_paths(paths))
    seen: set[str] = set()
    over_cap = 0
    for folder in resolve_folders(space, found.paths):
        for full in _files_under(folder, space):
            if full in seen:
                continue
            seen.add(full)
            if found.files >= MAX_FILES:
                found.problems.append(
                    f"more than {MAX_FILES} Dagster files; the rest were not read"
                )
                return found
            kind = _kind_of(full, space)
            if kind == "over_cap":
                over_cap += 1
            elif kind is not None:
                getattr(found, kind).append(to_posix(os.path.relpath(full, space)))
    if over_cap:
        found.problems.append(
            f"{over_cap} file{'s' if over_cap > 1 else ''} over {FILE_SIZE_CAP // 2**20} MB were "
            "not checked for Dagster definitions"
        )
    return found


def _files_under(folder: str, space: str) -> Iterable[str]:
    """Every file below ``folder`` (or ``folder`` itself, if it is a file), in a stable order,
    leaving out tools' own state, environments and hidden folders."""
    if not os.path.isdir(folder):
        yield folder
        return
    for dirpath, dirnames, filenames in walk_inside(folder, space):
        dirnames[:] = sorted(
            d for d in dirnames if d not in SKIP_SCAN_DIRS and not d.startswith(".")
        )
        for name in sorted(filenames):
            yield os.path.join(dirpath, name)


def _kind_of(full: str, space: str) -> str | None:
    """Which list a file belongs in, ``over_cap`` when it is too large to check, or None. A file
    that is not a regular file inside the workspace, such as a link out of it, is none of them."""
    name = os.path.basename(full)
    wanted = name in (COMPONENT_FILE, PROJECT_FILE, *WORKSPACE_FILES) or name.endswith(".py")
    if not wanted:
        return None
    real = resolve_inside(full, space)
    if real is None or not is_regular_file(real):
        return None
    if name == COMPONENT_FILE:
        return "components"
    if name in WORKSPACE_FILES:
        return "workspaces"
    if os.path.getsize(real) > FILE_SIZE_CAP:
        return "over_cap"
    text = read_capped(real, FILE_SIZE_CAP, space)
    if text is None:  # replaced or grown since: not read
        return None
    if name == PROJECT_FILE:
        return "projects" if _NAMES_TOOL.search(text) else None
    return "python" if _IMPORTS_DAGSTER.search(text) else None


def is_dagster_file(relative: str) -> bool:
    """Whether a file, by its name alone, could be one of these: the editor's file events give
    names, and a changed file is read again before its text is checked."""
    name = relative.rsplit("/", 1)[-1]
    return name in (COMPONENT_FILE, PROJECT_FILE, *WORKSPACE_FILES) or name.endswith(".py")
