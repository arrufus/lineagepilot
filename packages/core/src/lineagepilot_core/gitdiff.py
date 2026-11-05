"""What changed since the base, from git (task 4.5).

The extension asks the same questions through the ``vscode.git`` API (``extension/src/git.ts``).
Both try the same candidates for the base, take the merge-base with ``HEAD``, compare it with the
working tree using the same filter, and add the untracked files, so that the panel and
``lineagepilot review`` are given the same input.

A checkout can hold anything, including a ``git`` executable in the folder this runs from, so git
is found on ``PATH`` and refused if it comes from inside the project. Only read commands run,
with arguments rather than a shell, a ref is passed where it cannot be taken for an option, and
``core.fsmonitor``, the one setting that makes these commands run a program, is turned off.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from lineagepilot_core.analysis.review import BaseRef, ChangedFile, FileStatus
from lineagepilot_core.project import DbtProject, relative_to

DEFAULT_BASES = ("origin/HEAD", "origin/main", "origin/master", "main", "master")
_TIMEOUT_S = 120
_STATUSES = {"A": FileStatus.ADDED, "M": FileStatus.MODIFIED, "D": FileStatus.DELETED}


class GitError(RuntimeError):
    """git is missing, or cannot say what changed. The message says why, for a person."""


@dataclass
class GitChanges:
    base: BaseRef
    files: list[ChangedFile] = field(default_factory=list)
    outside_project: int = 0


def git_changes(project: DbtProject, base: str | None = None) -> GitChanges:
    """The files that differ from the merge-base of ``HEAD`` and ``base``, or of the default
    branch, including untracked files. Paths are relative to the project; files outside it
    are only counted."""
    git = _executable(project.root)
    top = Path(_run(git, project.root, "rev-parse", "--show-toplevel").strip())
    ref, commit = _merge_base(git, top, base)
    changes = GitChanges(BaseRef(ref, commit))
    for status, path, previous in _tracked(git, top, commit):
        _add(changes, project, top, status, path, previous)
    for path in _split(_run(git, top, "ls-files", "--others", "--exclude-standard", "-z")):
        _add(changes, project, top, FileStatus.UNTRACKED, path, None)
    return changes


def _executable(root: Path) -> str:
    found = shutil.which("git")
    if found is None:
        raise GitError("git is not on PATH")
    if relative_to(Path(found), root) is not None:
        raise GitError(f"refusing to run {found}, which is inside the project")
    return found


def _run(git: str, cwd: Path, *args: str, check: bool = True) -> str:
    try:
        done = subprocess.run(
            [git, "--no-pager", "-c", "core.fsmonitor=false", *args],
            cwd=cwd,
            capture_output=True,
            timeout=_TIMEOUT_S,
            # Never touch the index for a read, which a concurrent git would trip over
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise GitError(f"git {args[0]} did not finish: {e}") from None
    if done.returncode != 0:
        if not check:
            return ""
        message = done.stderr.decode("utf-8", errors="replace").strip()
        raise GitError(f"git {args[0]} failed: {message[:500]}")
    return done.stdout.decode("utf-8", errors="surrogateescape")


def _merge_base(git: str, top: Path, base: str | None) -> tuple[str, str]:
    if _run(git, top, "rev-parse", "--verify", "--quiet", "HEAD^{commit}", check=False) == "":
        raise GitError("this repository has no commits yet")
    candidates = (base,) if base else DEFAULT_BASES
    for ref in candidates:
        commit = _run(git, top, "merge-base", "HEAD", "--end-of-options", ref, check=False).strip()
        if commit:
            return ref, commit
    if base:
        raise GitError(f"{base} has no merge-base with HEAD; is it fetched, and not shallow?")
    raise GitError(
        f"none of {', '.join(DEFAULT_BASES)} has a merge-base with HEAD; name a ref with --base"
    )


def _tracked(git: str, top: Path, commit: str) -> list[tuple[FileStatus, str, str | None]]:
    # The same filter as vscode.git's diffWith: a type change or copy is not reported by either
    output = _run(
        git, top, "diff", "--no-ext-diff", "--name-status", "-z", "--diff-filter=ADMR", commit, "--"
    )
    fields = _split(output)
    found: list[tuple[FileStatus, str, str | None]] = []
    index = 0
    while index < len(fields):
        status = fields[index]
        if status.startswith("R"):
            previous, path = fields[index + 1], fields[index + 2]
            found.append((FileStatus.RENAMED, path, previous))
            index += 3
        else:
            found.append((_STATUSES[status[0]], fields[index + 1], None))
            index += 2
    return found


def _split(output: str) -> list[str]:
    return [part for part in output.split("\0") if part]


def _add(
    changes: GitChanges,
    project: DbtProject,
    top: Path,
    status: FileStatus,
    path: str,
    previous: str | None,
) -> None:
    relative = relative_to(top / path, project.root)
    if relative is None:
        changes.outside_project += 1
        return
    before = relative_to(top / previous, project.root) if previous else None
    changes.files.append(ChangedFile(relative, status, before))
