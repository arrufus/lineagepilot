"""What changed since the base, from git (task 4.5).

The extension asks the same questions through the ``vscode.git`` API (``extension/src/git.ts``).
Both try the same candidates for the base, take the merge-base with ``HEAD``, compare it with the
working tree using the same filter, and add the untracked files, so that the panel and
``lineagepilot review`` are given the same input.

A checkout can hold anything, including a ``git`` executable in the folder this runs from, so git
is found on ``PATH`` and refused if it comes from inside the project. Only read commands run,
with arguments rather than a shell, and a ref is passed where it cannot be taken for an option.

A ``.git`` folder can hold anything too when it arrives with the files, from an archive or a
shared folder (a clone never copies one). Its configuration and hooks name programs that git
runs, and running git in it is not safe in general. These commands turn off every one they can
reach (LP-SEC-07): ``core.fsmonitor``; every configured filter driver, whose ``clean`` or
``process`` command runs when ``git diff`` reads a changed file; hooks, since ``git diff`` writes
the index and that runs ``post-index-change``; external diff, textconv and the pager; the
configuration inside submodules; and fetching the objects a partial clone lacks, since a remote
can be a program.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from lineagepilot_core.analysis.review import BaseRef, ChangedFile, FileStatus
from lineagepilot_core.project import DbtProject, relative_to

DEFAULT_BASES = ("origin/HEAD", "origin/main", "origin/master", "main", "master")
_TIMEOUT_S = 120
_STATUSES = {"A": FileStatus.ADDED, "M": FileStatus.MODIFIED, "D": FileStatus.DELETED}

# git passes -c settings on to the git processes it starts, so these hold for those too
_SAFE_OPTIONS = (
    "--no-pager",
    "-c",
    "core.fsmonitor=false",
    "-c",
    f"core.hooksPath={os.devnull}",
)
_SAFE_ENV = {
    # Take no optional lock, which a concurrent git would trip over
    "GIT_OPTIONAL_LOCKS": "0",
    # No lazy fetch from a partial clone's promisor remote (git 2.44 and later), and, for any
    # git, no transport at all: whatever the configuration says, git may use only the
    # protocols listed here, and no protocol is called this
    "GIT_NO_LAZY_FETCH": "1",
    "GIT_ALLOW_PROTOCOL": "none",
}


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
    executable = _executable(project.root)
    git = _command(executable, project.root)
    top = Path(_run(git, project.root, "rev-parse", "--show-toplevel").strip())
    # core.worktree can make the top another repository, with a configuration of its own
    git = _command(executable, top)
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


def _command(executable: str, cwd: Path) -> list[str]:
    """git with the safe options, and every filter driver configured for ``cwd`` emptied.

    Drivers are named in configuration only, which git reads from every scope and included
    file here without running anything. An empty command is no filter (``convert.c``), and
    ``required=false`` stops git failing for want of one. Driver names must be listed one by
    one: ``-c`` has no wildcard."""
    git = [executable, *_SAFE_OPTIONS]
    names = set()
    for key in _split(_run(git, cwd, "config", "--list", "--null", "--name-only")):
        section, _, rest = key.partition(".")
        if section == "filter" and "." in rest:
            names.add(rest.rpartition(".")[0])
    for name in sorted(names):
        if "=" in name:
            # -c splits at the first "=", so this driver could not be turned off
            raise GitError(f"refusing to run git: a filter driver is named {name!r}")
        for key in ("clean", "smudge", "process"):
            git += ["-c", f"filter.{name}.{key}="]
        git += ["-c", f"filter.{name}.required=false"]
    return git


def _run(git: Sequence[str], cwd: Path, *args: str, check: bool = True) -> str:
    try:
        done = subprocess.run(
            [*git, *args],
            cwd=cwd,
            capture_output=True,
            timeout=_TIMEOUT_S,
            env={**os.environ, **_SAFE_ENV},
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise GitError(f"git {args[0]} did not finish: {e}") from None
    if done.returncode != 0:
        if not check:
            return ""
        message = done.stderr.decode("utf-8", errors="replace").strip()
        if "promisor remote" in message:
            message += " (this partial clone lacks objects, and the review never fetches: "
            message += "fetch them with git, then run it again)"
        raise GitError(f"git {args[0]} failed: {message[:500]}")
    return done.stdout.decode("utf-8", errors="surrogateescape")


def _merge_base(git: Sequence[str], top: Path, base: str | None) -> tuple[str, str]:
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


def _tracked(
    git: Sequence[str], top: Path, commit: str
) -> list[tuple[FileStatus, str, str | None]]:
    # The same filter as vscode.git's diffWith: a type change or copy is not reported by either.
    # A submodule is reported when its commit changes, but its work tree is not looked at: that
    # runs git status inside it, under the submodule's own configuration.
    output = _run(
        git,
        top,
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--ignore-submodules=dirty",
        "--name-status",
        "-z",
        "--diff-filter=ADMR",
        commit,
        "--",
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
