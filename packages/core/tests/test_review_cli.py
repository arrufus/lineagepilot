"""``lineagepilot review`` and what it asks git, on real repositories (tasks 4.1 and 4.5)."""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from support import IMPACT_SHOP, apply_rename, copy_impact_shop

from lineagepilot_core.airflow.link import link_airflow
from lineagepilot_core.airflow.workspace import scan_airflow
from lineagepilot_core.analysis.review import (
    BaseRef,
    ChangedFile,
    FileStatus,
    review_changes,
)
from lineagepilot_core.cli import EXIT_OK, EXIT_UNKNOWN, EXIT_USAGE, main
from lineagepilot_core.gitdiff import GitError, git_changes
from lineagepilot_core.indexer import index_project
from lineagepilot_core.project import read_project

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")

GOLDEN = Path(__file__).parent / "fixtures" / "review" / "rename-order-total.md"


def git(cwd, *args):
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    done = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    return done.stdout.strip()


def commit_all(cwd, message="x"):
    git(cwd, "add", "-A")
    git(cwd, "commit", "-q", "-m", message)
    return git(cwd, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path):
    """The project, committed on main, in a repository of its own."""
    root = copy_impact_shop(tmp_path / "impact_shop")
    git(root, "init", "-q", "-b", "main")
    commit_all(root, "base")
    return root


def run(*args, out=None):
    out = out or io.StringIO()
    code = main(["review", *args], out)
    return code, out.getvalue()


class TestGitChanges:
    def test_changed_and_untracked_files_relative_to_the_project(self, repo):
        (repo / "models" / "marts" / "orders.sql").write_text("select 1 as a\n")
        (repo / "models" / "marts" / "new_model.sql").write_text("select 1 as a\n")
        (repo / "models" / "staging" / "stg_customers.sql").unlink()
        found = git_changes(read_project(repo), "main")
        assert sorted(found.files, key=lambda f: f.path) == [
            ChangedFile("models/marts/new_model.sql", FileStatus.UNTRACKED),
            ChangedFile("models/marts/orders.sql", FileStatus.MODIFIED),
            ChangedFile("models/staging/stg_customers.sql", FileStatus.DELETED),
        ]
        assert found.base == BaseRef("main", git(repo, "rev-parse", "HEAD"))

    def test_ignored_files_are_left_out(self, repo):
        (repo / "target" / "something.json").write_text("{}")
        assert git_changes(read_project(repo), "main").files == []

    def test_a_staged_new_file_and_a_committed_file_both_count(self, repo):
        git(repo, "checkout", "-q", "-b", "feature")
        (repo / "models" / "marts" / "committed.sql").write_text("select 1 as a\n")
        commit_all(repo)
        (repo / "models" / "marts" / "staged.sql").write_text("select 1 as a\n")
        git(repo, "add", "models/marts/staged.sql")
        found = git_changes(read_project(repo), "main")
        assert {f.path: f.status for f in found.files} == {
            "models/marts/committed.sql": FileStatus.ADDED,
            "models/marts/staged.sql": FileStatus.ADDED,
        }

    def test_a_rename_keeps_where_the_file_was(self, repo):
        git(repo, "mv", "models/marts/orders.sql", "models/marts/orders_mart.sql")
        found = git_changes(read_project(repo), "main")
        assert found.files == [
            ChangedFile(
                "models/marts/orders_mart.sql", FileStatus.RENAMED, "models/marts/orders.sql"
            )
        ]

    def test_the_merge_base_not_the_tip_of_the_other_branch(self, repo):
        base = git(repo, "rev-parse", "HEAD")
        git(repo, "checkout", "-q", "-b", "feature")
        (repo / "models" / "marts" / "mine.sql").write_text("select 1 as a\n")
        commit_all(repo)
        git(repo, "checkout", "-q", "main")
        (repo / "theirs.txt").write_text("from main\n")
        commit_all(repo)
        git(repo, "checkout", "-q", "feature")
        found = git_changes(read_project(repo), "main")
        assert found.base.commit == base
        assert [f.path for f in found.files] == ["models/marts/mine.sql"]

    def test_the_default_base_is_the_first_candidate_that_exists(self, repo):
        assert git_changes(read_project(repo)).base.ref == "main"
        git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
        assert git_changes(read_project(repo)).base.ref == "origin/main"
        git(repo, "update-ref", "refs/remotes/origin/HEAD", "HEAD")
        assert git_changes(read_project(repo)).base.ref == "origin/HEAD"

    def test_no_candidate_says_what_to_do(self, repo):
        git(repo, "branch", "-m", "main", "trunk")
        with pytest.raises(GitError, match="name a ref with --base"):
            git_changes(read_project(repo))

    def test_an_unknown_ref_says_so(self, repo):
        with pytest.raises(GitError, match="no merge-base"):
            git_changes(read_project(repo), "no-such-ref")

    def test_a_ref_cannot_be_taken_for_an_option(self, repo):
        trap = repo / "written-by-git"
        with pytest.raises(GitError):
            git_changes(read_project(repo), f"--output={trap}")
        assert not trap.exists()

    def test_a_repository_with_no_commits(self, tmp_path):
        root = copy_impact_shop(tmp_path / "fresh")
        git(root, "init", "-q")
        with pytest.raises(GitError, match="no commits"):
            git_changes(read_project(root))

    def test_a_folder_that_is_not_a_repository(self, tmp_path, monkeypatch):
        root = copy_impact_shop(tmp_path / "plain")
        monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
        with pytest.raises(GitError, match="rev-parse failed"):
            git_changes(read_project(root))

    def test_files_outside_a_project_in_a_subfolder_are_counted(self, tmp_path):
        top = tmp_path / "mono"
        top.mkdir()
        copy_impact_shop(top / "analytics")
        (top / "README.md").write_text("one\n")
        git(top, "init", "-q", "-b", "main")
        commit_all(top)
        (top / "README.md").write_text("two\n")
        (top / "analytics" / "models" / "marts" / "orders.sql").write_text("select 1 as a\n")
        found = git_changes(read_project(top / "analytics"), "main")
        assert [f.path for f in found.files] == ["models/marts/orders.sql"]
        assert found.outside_project == 1

    def test_a_git_inside_the_project_is_refused(self, repo, monkeypatch):
        planted = repo / "bin"
        planted.mkdir()
        fake = planted / ("git.cmd" if sys.platform == "win32" else "git")
        marker = repo / "planted-git-ran"
        fake.write_text(f'#!/bin/sh\ntouch "{marker}"\n')
        fake.chmod(0o755)
        monkeypatch.setenv("PATH", f"{planted}{os.pathsep}{os.environ['PATH']}")
        with pytest.raises(GitError, match="inside the project"):
            git_changes(read_project(repo), "main")
        assert not marker.exists()

    def test_no_git_at_all(self, repo, monkeypatch):
        monkeypatch.setattr("lineagepilot_core.gitdiff.shutil.which", lambda _: None)
        with pytest.raises(GitError, match="not on PATH"):
            git_changes(read_project(repo))


class TestReviewCommand:
    def test_the_acceptance_scenario_prints_the_panel_s_markdown(self, repo):
        apply_rename(repo)
        code, text = run("--project-dir", str(repo), "--base", "main")
        assert code == EXIT_OK
        assert "Removed: `order_total`" in text and "`revenue_dashboard`" in text
        # What the panel gets for the same inputs, through the same function
        changes = git_changes(read_project(repo), "main")
        graph = index_project(repo).graph
        link_airflow(graph, scan_airflow(repo, repo))
        direct = review_changes(graph, changes.files, read_project(repo), base=changes.base)
        assert text == direct["markdown"]
        assert "`daily_marts`" in text and "`data-platform`" in text

    def test_the_same_repository_gives_the_same_bytes_every_time(self, repo):
        apply_rename(repo)
        first = run("--project-dir", str(repo), "--base", "main", "--no-cache")[1]
        cached_once = run("--project-dir", str(repo), "--base", "main")[1]
        cached_twice = run("--project-dir", str(repo), "--base", "main")[1]
        assert first == cached_once == cached_twice

    def test_it_matches_the_golden_file_apart_from_the_commit_and_the_changes_it_adds(self, repo):
        apply_rename(repo)
        (repo / "macros" / "cents_to_dollars.sql").write_text("{# changed #}\n")
        (repo / "models" / "marts" / "brand_new.sql").write_text("select 1 as a\n")
        code, text = run("--project-dir", str(repo), "--base", "main", "--no-cache")
        assert code == EXIT_OK
        golden = GOLDEN.read_text(encoding="utf-8")
        commit = git(repo, "rev-parse", "HEAD")[:12]
        # The golden file adds a README and two files outside the project to the same changes
        expected = golden.replace("`origin/main` (`0123456789ab`)", f"`main` (`{commit}`)")
        expected = expected[: expected.index("<details>")]
        assert text.rstrip("\n") == expected.rstrip("\n")

    def test_json_is_the_whole_review(self, repo):
        apply_rename(repo)
        code, text = run("--project-dir", str(repo), "--base", "main", "--json")
        data = json.loads(text)
        assert code == EXIT_OK and data["project"] == "impact_shop"
        assert data["changedNodes"][0]["columns"]["removed"] == ["order_total"]
        assert data["markdown"].startswith("## Change impact review")

    def test_a_baseline_manifest_can_be_named(self, repo, tmp_path):
        apply_rename(repo)
        state = tmp_path / "state.json"
        shutil.copy(IMPACT_SHOP / "target" / "manifest.json", state)
        args = ("--project-dir", str(repo), "--base", "main", "--json")
        code, text = run(*args, "--baseline-manifest", str(state))
        data = json.loads(text)
        assert code == EXIT_OK
        assert data["changedNodes"][0]["columns"]["baseline"] == "baseline_manifest"
        assert data["artifacts"]["baselineManifest"]["name"] == "state.json"
        assert str(tmp_path) not in data["markdown"]

    def test_with_column_lineage_off_it_says_so(self, repo):
        apply_rename(repo)
        code, text = run("--project-dir", str(repo), "--base", "main", "--no-column-lineage")
        assert code == EXIT_OK and "Column lineage is turned off" in text

    def test_a_custom_target_path_is_followed(self, repo):
        shutil.move(repo / "target", repo / "build")
        project_file = repo / "dbt_project.yml"
        project_file.write_text(project_file.read_text().replace('"target"', '"build"'))
        code, text = run("--project-dir", str(repo), "--base", "main")
        assert code == EXIT_OK and "Other changed files" not in text

    def test_git_trouble_exits_1_with_the_reason_on_stderr(self, repo, capsys):
        code, text = run("--project-dir", str(repo), "--base", "no-such-ref")
        assert code == EXIT_UNKNOWN and text == ""
        assert "no merge-base" in capsys.readouterr().err

    def test_no_manifest_exits_1(self, repo):
        shutil.rmtree(repo / "target")
        code, text = run("--project-dir", str(repo), "--base", "main")
        assert code == EXIT_UNKNOWN and "No manifest" in text

    def test_a_bad_option_exits_2(self, capsys):
        assert main(["review", "--no-such-option"]) == EXIT_USAGE
        capsys.readouterr()


class TestAirflowInTheCommands:
    def test_no_airflow_says_so(self, repo):
        apply_rename(repo)
        code, text = run("--project-dir", str(repo), "--base", "main", "--no-airflow")
        assert code == EXIT_OK and "Airflow DAGs were not analysed: --no-airflow was given" in text
        assert "`daily_marts`" not in text

    def test_dag_folders_and_the_workspace_can_be_named(self, repo):
        apply_rename(repo)
        args = ("--project-dir", str(repo), "--base", "main", "--workspace", str(repo))
        code, text = run(*args, "--dag-folder", "dags/operations.py")
        assert code == EXIT_OK and "`refresh_counts`" in text and "`daily_marts`" not in text
        code, text = run(*args, "--dag-folder", "nowhere")
        assert code == EXIT_OK and "no DAG files were found in nowhere" in text

    def test_a_snapshot_never_holds_a_dag_scan(self, repo):
        apply_rename(repo)
        run("--project-dir", str(repo), "--base", "main")
        snapshots = list((repo / "target" / ".lineagepilot").glob("*.json"))
        assert snapshots and all(b"daily_marts" not in s.read_bytes() for s in snapshots)

    def test_impact_lists_tasks(self, repo):
        out = io.StringIO()
        code = main(["impact", "orders", "--project-dir", str(repo), "--json"], out)
        data = json.loads(out.getvalue())
        assert code == EXIT_OK and data["counts"]["airflow_task"] >= 3

    def test_the_airflow_command_as_text_and_json(self, repo):
        out = io.StringIO()
        assert main(["airflow", "--project-dir", str(repo), "--workspace", str(repo)], out) == 0
        text = out.getvalue()
        assert "3 DAG files in dags: 3 DAGs" in text and "daily_marts  marts" in text
        assert "dags/backfill.py:12  loop or factory" in text
        out = io.StringIO()
        assert main(["airflow", "--workspace", str(repo), "--json"], out) == 0
        data = json.loads(out.getvalue())
        assert data["project"] is None and data["stats"]["links_resolved"] == 2

    def test_the_airflow_command_reports_problems(self, repo):
        (repo / ".lineagepilot").mkdir()
        (repo / ".lineagepilot" / "airflow.yml").write_text("path_map: [a]\n")
        scan_args = ["airflow", "--project-dir", str(repo), "--workspace", str(repo), "--json"]
        out = io.StringIO()
        assert main(scan_args, out) == 0
        assert (
            "path_map is not a mapping" in json.loads(out.getvalue())["annotations"]["problems"][0]
        )
