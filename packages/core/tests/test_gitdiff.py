"""lineagepilot review runs no program that a repository's ``.git`` names (LP-SEC-07).

A ``.git`` folder that arrives with the files, from an archive or a shared folder, is written by
whoever sent it. Each test sets a trap in one and checks that ``git_changes`` does not spring it,
then that plain git does: a trap that cannot spring on this machine is skipped, not passed.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
from support import copy_impact_shop

import lineagepilot_core.gitdiff as gitdiff
from lineagepilot_core.analysis.review import ChangedFile, FileStatus
from lineagepilot_core.gitdiff import GitError, git_changes
from lineagepilot_core.project import read_project

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")


def git(cwd, *args, check=True):
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    done = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True)
    assert done.returncode == 0 or not check, done.stderr
    return done.stdout.strip()


def commit_all(cwd):
    git(cwd, "add", "-A")
    git(cwd, "commit", "-q", "-m", "x")


@pytest.fixture
def repo(tmp_path):
    root = copy_impact_shop(tmp_path / "impact_shop")
    git(root, "init", "-q", "-b", "main")
    commit_all(root)
    return root


def leave(trap: Path) -> str:
    """A shell command that creates ``trap``, with no program but the shell."""
    return f': > "{trap.as_posix()}"'


def add_config(repo: Path, text: str) -> None:
    with open(repo / ".git" / "config", "a", encoding="utf-8") as f:
        f.write(text)


def plain_git_springs(cwd: Path, trap: Path, *args: str) -> None:
    git(cwd, *args, check=False)
    if not trap.exists():
        pytest.skip("the trap does not spring with plain git here, so the test proves nothing")


ORDERS = ChangedFile("models/marts/orders.sql", FileStatus.MODIFIED)

TRAPS = {
    "filter clean": ('[filter "evil"]\n\tclean = {cmd}\n\trequired = true\n', "* filter=evil\n"),
    "filter process": ('[filter "evil"]\n\tprocess = {cmd}\n', "* filter=evil\n"),
    "filter with dots": ('[filter "a.b"]\n\tclean = {cmd}\n', "* filter=a.b\n"),
    "textconv": ('[diff "evil"]\n\ttextconv = {cmd}\n', "* diff=evil\n"),
    "diff driver command": ('[diff "evil"]\n\tcommand = {cmd}\n', "* diff=evil\n"),
    "diff.external": ("[diff]\n\texternal = {cmd}\n", ""),
    "core.fsmonitor": ("[core]\n\tfsmonitor = {cmd}\n", ""),
}


@pytest.mark.parametrize("name", TRAPS)
def test_no_configured_program_runs(repo, tmp_path, name):
    config, attributes = TRAPS[name]
    trap = tmp_path / "trap"
    add_config(repo, config.format(cmd=leave(trap)))
    (repo / ".git" / "info").mkdir(exist_ok=True)
    (repo / ".git" / "info" / "attributes").write_text(attributes)
    (repo / "models" / "marts" / "orders.sql").write_text("select 1 as a\n")
    assert git_changes(read_project(repo), "main").files == [ORDERS]
    assert not trap.exists()
    # A patch, which plain git makes with the textconv and external diff as well
    plain_git_springs(repo, trap, "diff", "main", "--")


def test_a_filter_in_an_included_file_and_named_in_gitattributes(repo, tmp_path):
    (repo / ".gitattributes").write_text("* filter=evil diff=evil\n")
    commit_all(repo)
    trap = tmp_path / "trap"
    included = tmp_path / "included.cfg"
    included.write_text(f'[filter "evil"]\n\tclean = {leave(trap)}\n')
    add_config(repo, f'[include]\n\tpath = "{included.as_posix()}"\n')
    (repo / "models" / "marts" / "orders.sql").write_text("select 1 as a\n")
    assert git_changes(read_project(repo), "main").files == [ORDERS]
    assert not trap.exists()
    plain_git_springs(repo, trap, "diff", "--name-status", "main", "--")


def test_a_filter_that_cannot_be_turned_off_stops_the_review(repo, tmp_path):
    """``-c filter.a=b.clean=`` would set ``filter.a`` to ``b.clean=``, leaving the driver on."""
    trap = tmp_path / "trap"
    add_config(repo, f'[filter "a=b"]\n\tclean = {leave(trap)}\n')
    (repo / ".gitattributes").write_text("* filter=a=b\n")
    (repo / "models" / "marts" / "orders.sql").write_text("select 1 as a\n")
    with pytest.raises(GitError, match="filter driver is named 'a=b'"):
        git_changes(read_project(repo), "main")
    assert not trap.exists()
    plain_git_springs(repo, trap, "diff", "--name-status", "main", "--")


@pytest.mark.parametrize("where", ["hooks folder", "core.hooksPath"])
def test_hooks_do_not_run(repo, tmp_path, where):
    """git diff writes the index when a file's times changed but not its content, and writing
    the index runs the post-index-change hook."""
    trap = tmp_path / "trap"
    hooks = repo / ".git" / "hooks"
    if where == "core.hooksPath":
        hooks = tmp_path / "githooks"
        hooks.mkdir()
        add_config(repo, f'[core]\n\thooksPath = "{hooks.as_posix()}"\n')
    (hooks / "post-index-change").write_text(f"#!/bin/sh\n{leave(trap)}\n")
    (hooks / "post-index-change").chmod(0o755)
    touched = repo / "models" / "marts" / "orders.sql"
    os.utime(touched, (1_000_000_000, 1_000_000_000))
    assert git_changes(read_project(repo), "main").files == []
    assert not trap.exists()
    os.utime(touched, (1_100_000_000, 1_100_000_000))
    plain_git_springs(repo, trap, "diff", "--name-status", "main", "--")


def test_a_submodule_s_own_configuration_is_not_reached(repo, tmp_path):
    """git checks a submodule's work tree with git status inside it, under its own .git."""
    library = tmp_path / "library"
    library.mkdir()
    (library / "lib.txt").write_text("one\n")
    git(library, "init", "-q", "-b", "main")
    commit_all(library)
    git(repo, "-c", "protocol.file.allow=always", "submodule", "add", "-q", str(library), "vendor")
    commit_all(repo)
    trap = tmp_path / "trap"
    module = repo / ".git" / "modules" / "vendor"  # the submodule's .git
    with open(module / "config", "a", encoding="utf-8") as f:
        f.write(f'[filter "evil"]\n\tclean = {leave(trap)}\n')
    (module / "info").mkdir(exist_ok=True)
    (module / "info" / "attributes").write_text("* filter=evil\n")
    (repo / "vendor" / "lib.txt").write_text("two\n")
    assert git_changes(read_project(repo), "main").files == []
    assert not trap.exists()
    plain_git_springs(repo, trap, "diff", "--name-status", "main", "--")


@pytest.mark.parametrize("guard", ["GIT_NO_LAZY_FETCH", "GIT_ALLOW_PROTOCOL"])
def test_a_partial_clone_fetches_nothing(repo, tmp_path, monkeypatch, guard):
    """A partial clone fetches the objects it lacks from its promisor remote, and an ext::
    remote is a program. Each guard is enough alone; git before 2.44 has only the second."""
    for other in {"GIT_NO_LAZY_FETCH", "GIT_ALLOW_PROTOCOL"} - {guard}:
        monkeypatch.delitem(gitdiff._SAFE_ENV, other)
    git(repo, "checkout", "-q", "-b", "feature")
    (repo / "models" / "marts" / "orders.sql").write_text("select 1 as a\n")
    commit_all(repo)
    tree = git(repo, "rev-parse", "main:models/marts")
    lost = repo / ".git" / "objects" / tree[:2] / tree[2:]
    lost.chmod(0o644)  # git writes objects read-only, which Windows will not delete
    lost.unlink()
    trap = tmp_path / "trap"
    git(repo, "config", "core.repositoryformatversion", "1")
    git(repo, "config", "extensions.partialClone", "origin")
    git(repo, "config", "remote.origin.promisor", "true")
    # ext:: splits its command at spaces; "% " is a space inside an argument
    git(repo, "config", "remote.origin.url", "ext::sh -c :>" + trap.as_posix().replace(" ", "% "))
    git(repo, "config", "protocol.ext.allow", "always")
    with pytest.raises(GitError, match="git diff failed"):
        git_changes(read_project(repo), "main")
    assert not trap.exists()
    plain_git_springs(repo, trap, "diff", "--name-status", "main", "--")


def test_a_work_tree_in_another_repository_is_read_with_that_repository_s_settings(tmp_path):
    """core.worktree can send git to a folder that has a .git of its own."""
    project = copy_impact_shop(tmp_path / "impact_shop")
    other = tmp_path / "other"
    other.mkdir()
    (other / "a.txt").write_text("one\n")
    git(other, "init", "-q", "-b", "main")
    commit_all(other)
    trap = tmp_path / "trap"
    add_config(other, f'[filter "evil"]\n\tclean = {leave(trap)}\n')
    (other / ".git" / "info" / "attributes").write_text("* filter=evil\n")
    (other / "a.txt").write_text("two\n")
    git(project, "init", "-q", "-b", "main")
    commit_all(project)
    add_config(project, f'[core]\n\tworktree = "{other.as_posix()}"\n')
    found = git_changes(read_project(project), "main")
    assert found.files == [] and found.outside_project == 1
    assert not trap.exists()
    plain_git_springs(other, trap, "diff", "--name-status", "main", "--")
