"""The folders a reader's setting names, for every reader (task 8.2).

A reader's paths are a setting a workspace can choose: relative folders, or patterns with ``*``,
which are bounded in length and wildcards and never leave the workspace. They began as the DAG
scan's ``dagFolders`` (tasks 5.1 and 7.3); from task 8.2 every reader uses them.
"""

from __future__ import annotations

import fnmatch
import os
from collections.abc import Iterable
from pathlib import PurePosixPath
from typing import Any

from lineagepilot_core.readers.common.safe_io import (
    is_regular_file,
    regular_file_inside,
    resolve_inside,
    to_posix,
    walk_inside,
)

# Folders never worth reading: tools' own state, environments and build output
SKIP_SCAN_DIRS = {
    ".git",
    "__pycache__",
    "node_modules",
    ".venv",
    "venv",
    "dbt_packages",
    "target",
    "logs",
    ".tox",
}
MAX_FOLDER_DEPTH = 6
# A folder pattern is a setting a workspace can choose, matched on every file event: enough for
# any real layout, and a bound on the work (audit LP-SEC-05)
MAX_FOLDER_PATTERN = 200
MAX_FOLDER_WILDCARDS = 6
# The whole workspace, as a cleaned setting has it
WORKSPACE = "."


def glob_match(relative: str, pattern: str) -> bool:
    """``PurePosixPath(relative).match(pattern)``, in linear time: one ``fnmatch`` per component,
    from the right. On Python 3.13 ``PurePath.match`` compiles a pattern such as ``*a*a*a*b`` to a
    regular expression that backtracks, taking seconds on one long name; ``fnmatch`` does not."""
    parts, wanted = PurePosixPath(relative).parts, PurePosixPath(pattern).parts
    if not wanted:
        return False
    if PurePosixPath(pattern).is_absolute():
        if len(wanted) != len(parts):
            return False
    elif len(wanted) > len(parts):
        return False
    pairs = zip(reversed(parts), reversed(wanted), strict=False)  # the path may be longer
    return all(fnmatch.fnmatchcase(p, w) for p, w in pairs)


def is_dbt_project(folder: str, workspace: str) -> bool:
    marker = os.path.join(folder, "dbt_project.yml")
    return os.path.lexists(marker) and regular_file_inside(marker, workspace) is not None


def clean_folder(folder: Any) -> str | None:
    """A relative folder or pattern, or None: a setting cannot point outside the workspace, and
    a pattern is bounded in length and wildcards."""
    if not isinstance(folder, str):
        return None
    if folder.strip() in (".", "./"):
        return WORKSPACE
    if len(folder) > MAX_FOLDER_PATTERN or (
        folder.count("*") + folder.count("?") > MAX_FOLDER_WILDCARDS
    ):
        return None
    posix = PurePosixPath(folder.strip().replace("\\", "/"))
    if not posix.parts or posix.is_absolute() or ".." in posix.parts or ":" in posix.parts[0]:
        return None
    return posix.as_posix().rstrip("/")


def in_folders(relative: str, folders: Iterable[str]) -> bool:
    """Whether a file (relative to the workspace) is below one of these folders or patterns. The
    workspace itself, ``.``, holds every file in it."""
    parents = PurePosixPath(relative).parents
    for folder in folders:
        if folder == WORKSPACE:
            return True
        if "*" not in folder:
            if relative.startswith(f"{folder}/"):
                return True
            continue
        target = folder[3:] if folder.startswith("**/") else folder
        if any(p.parts and glob_match(p.as_posix(), target) for p in parents):
            return True
    return False


def resolve_folders(workspace: str, patterns: Iterable[str]) -> list[str]:
    """The folders that exist for these patterns: a plain path (a single file will do), or a
    pattern with ``*``, which is looked for up to a few levels down. Each is a real path inside
    the workspace: a folder that is a link out of it is not followed."""
    workspace = os.path.realpath(workspace)
    found: list[str] = []
    wanted = [p for p in (clean_folder(p) for p in patterns) if p]
    for pattern in wanted:
        if "*" not in pattern:
            path = resolve_inside(os.path.join(workspace, pattern), workspace)
            if path is not None and (os.path.isdir(path) or is_regular_file(path)):
                found.append(path)
            continue
        target = pattern[3:] if pattern.startswith("**/") else pattern
        for dirpath, dirnames, _ in walk_inside(workspace, workspace):
            depth = (
                0
                if dirpath == workspace
                else to_posix(os.path.relpath(dirpath, workspace)).count("/") + 1
            )
            dirnames[:] = (
                sorted(
                    d
                    for d in dirnames
                    if d not in SKIP_SCAN_DIRS
                    and not d.startswith(".")
                    and not is_dbt_project(os.path.join(dirpath, d), workspace)
                )
                if depth < MAX_FOLDER_DEPTH
                else []
            )
            for d in list(dirnames):
                relative = to_posix(os.path.relpath(os.path.join(dirpath, d), workspace))
                if glob_match(relative, target):
                    # A link to a folder is resolved here, and left out if it leads elsewhere
                    real = resolve_inside(os.path.join(dirpath, d), workspace)
                    if real is not None:
                        found.append(real)
                    dirnames.remove(d)  # what is below it is scanned as part of it
    return sorted(set(found))
