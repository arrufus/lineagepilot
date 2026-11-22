"""Where a workspace's Dagster definitions are (task 8.2): lineagePilot.dagster.paths."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from lineagepilot_core.readers.common.folders import in_folders
from lineagepilot_core.readers.dagster import discovery
from lineagepilot_core.readers.dagster.discovery import discover

DEFINITIONS = "from dagster import Definitions\n\ndefs = Definitions()\n"
ASSETS = "from dagster_dbt import DbtProject, dbt_assets\n"


def write(root: Path, relative: str, text: str = "") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """A repository with a dg project beside a dbt project, and files that are not Dagster's."""
    root = tmp_path / "repo"
    write(root, "orchestration/pyproject.toml", '[project]\nname = "x"\n\n[tool.dg.project]\n')
    write(root, "orchestration/src/x/definitions.py", DEFINITIONS)
    write(root, "orchestration/src/x/defs/dbt/defs.yaml", "type: dagster_dbt.DbtProjectComponent\n")
    write(root, "orchestration/src/x/assets.py", ASSETS)
    write(root, "orchestration/src/x/util.py", "import os\n")
    write(root, "orchestration/src/x/notes.py", "# dagster is mentioned, not imported\n")
    write(root, "workspace.yaml", "load_from:\n  - python_module: x\n")
    write(root, "pyproject.toml", '[project]\nname = "repo"\n')
    write(root, "dbt/dbt_project.yml", "name: shop\n")
    write(root, "dbt/models/orders.sql", "select 1\n")
    return root


class TestWhatItFinds:
    def test_each_kind_of_file_by_default(self, workspace):
        found = discover(str(workspace), discovery.DEFAULT_PATHS)
        assert found.paths == ["."]
        assert found.components == ["orchestration/src/x/defs/dbt/defs.yaml"]
        assert found.projects == ["orchestration/pyproject.toml"]
        assert found.workspaces == ["workspace.yaml"]
        assert found.python == [
            "orchestration/src/x/assets.py",
            "orchestration/src/x/definitions.py",
        ]
        assert found.files == 5 and found.problems == []
        assert found.to_json()["python"] == found.python

    @pytest.mark.parametrize(
        ("text", "is_dagster"),
        [
            ("import dagster\n", True),
            ("import dagster as dg\n", True),
            ("from dagster import asset\n", True),
            ("from dagster_dbt import dbt_assets\n", True),
            ("    from dagster import asset\n", True),
            ("import os, dagster\n", True),
            ("import dagsterlike\n", False),
            ("# from dagster import asset\n", False),
            ('"""import dagster"""\n', False),
            ("x = 'dagster'\n", False),
        ],
    )
    def test_a_python_file_is_dagster_s_when_a_line_imports_it(self, tmp_path, text, is_dagster):
        write(tmp_path, "a.py", text)
        assert discover(str(tmp_path), ["."]).python == (["a.py"] if is_dagster else [])

    def test_a_pyproject_counts_only_with_a_dg_or_dagster_table(self, tmp_path):
        write(tmp_path, "a/pyproject.toml", "[tool.dagster]\nmodule_name = 'a'\n")
        write(tmp_path, "b/pyproject.toml", "[tool.dg]\n")
        write(tmp_path, "c/pyproject.toml", "[tool.ruff]\n# [tool.dg] in a comment\n")
        assert discover(str(tmp_path), ["."]).projects == ["a/pyproject.toml", "b/pyproject.toml"]

    def test_only_the_setting_s_folders(self, workspace):
        found = discover(str(workspace), ["orchestration/src"])
        assert found.paths == ["orchestration/src"]
        assert found.files == 3 and found.projects == [] and found.workspaces == []

    def test_a_pattern_finds_folders_anywhere(self, workspace):
        found = discover(str(workspace), ["**/defs"])
        assert found.components == ["orchestration/src/x/defs/dbt/defs.yaml"] and found.files == 1

    def test_a_file_named_in_the_setting_is_read_alone(self, workspace):
        found = discover(str(workspace), ["orchestration/src/x/assets.py"])
        assert found.python == ["orchestration/src/x/assets.py"] and found.files == 1

    def test_a_folder_named_twice_is_read_once(self, workspace):
        once = discover(str(workspace), ["."])
        assert discover(str(workspace), [".", "./", "orchestration"]).to_json() == once.to_json()

    def test_nothing_found_says_nothing_is_wrong(self, tmp_path):
        found = discover(str(tmp_path), ["."])
        assert found.files == 0 and found.problems == []


class TestWhatItLeavesOut:
    def test_tools_state_environments_and_hidden_folders(self, tmp_path):
        for folder in (".venv", "venv", "node_modules", "target", "dbt_packages", ".hidden"):
            write(tmp_path, f"{folder}/x.py", DEFINITIONS)
            write(tmp_path, f"{folder}/defs.yaml", "type: x\n")
        write(tmp_path, "src/x.py", DEFINITIONS)
        assert discover(str(tmp_path), ["."]).to_json() == {
            "components": [],
            "projects": [],
            "workspaces": [],
            "python": ["src/x.py"],
        }

    def test_dagster_code_inside_a_dbt_project_is_found(self, tmp_path):
        write(tmp_path, "shop/dbt_project.yml", "name: shop\n")
        write(tmp_path, "shop/orchestration/definitions.py", DEFINITIONS)
        assert discover(str(tmp_path), ["."]).python == ["shop/orchestration/definitions.py"]

    @pytest.mark.parametrize("setting", ["../outside", "/etc", "C:/x", "", 7, None, "a" * 201])
    def test_a_setting_that_leaves_the_workspace_or_is_unbounded_is_dropped(
        self, tmp_path, setting
    ):
        assert discovery.clean_paths([setting]) == []
        assert discover(str(tmp_path), [setting]).paths == []

    def test_the_number_of_folders_is_bounded(self):
        assert discovery.clean_paths([f"f{n}" for n in range(60)]) == [f"f{n}" for n in range(50)]

    @pytest.mark.skipif(sys.platform == "win32", reason="symbolic links need privileges on Windows")
    def test_a_link_out_of_the_workspace_is_not_listed(self, tmp_path):
        outside = tmp_path / "outside"
        write(outside, "definitions.py", DEFINITIONS)
        write(outside, "defs.yaml", "type: x\n")
        root = tmp_path / "repo"
        root.mkdir()
        os.symlink(outside / "definitions.py", root / "definitions.py")
        os.symlink(outside / "defs.yaml", root / "defs.yaml")
        os.symlink(outside, root / "linked")
        assert discover(str(root), ["."]).files == 0

    def test_a_file_over_the_size_limit_is_counted_not_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr(discovery, "FILE_SIZE_CAP", 100)
        write(tmp_path, "big.py", DEFINITIONS + "#" * 200)
        write(tmp_path, "huge.py", "#" * 200)
        write(tmp_path, "small.py", DEFINITIONS)
        found = discover(str(tmp_path), ["."])
        assert found.python == ["small.py"]
        assert found.problems == ["2 files over 0 MB were not checked for Dagster definitions"]

    def test_a_file_replaced_while_it_is_read_is_left_out(self, tmp_path, monkeypatch):
        write(tmp_path, "a.py", DEFINITIONS)
        monkeypatch.setattr(discovery, "read_capped", lambda *_: None)
        assert discover(str(tmp_path), ["."]).files == 0

    def test_the_number_of_files_is_bounded(self, tmp_path, monkeypatch):
        monkeypatch.setattr(discovery, "MAX_FILES", 3)
        for n in range(5):
            write(tmp_path, f"d{n}.py", DEFINITIONS)
        found = discover(str(tmp_path), ["."])
        assert found.python == ["d0.py", "d1.py", "d2.py"]
        assert found.problems == ["more than 3 Dagster files; the rest were not read"]


class TestTheWorkspaceAsAFolder:
    @pytest.mark.parametrize("relative", ["a.py", "deep/down/a.py"])
    def test_the_workspace_holds_every_file_in_it(self, relative):
        """``.`` in any reader's setting means the whole workspace (task 8.2): before, the DAG
        scan's file events never matched it."""
        assert in_folders(relative, ["."]) is True
        assert in_folders(relative, ["dags"]) is False

    def test_is_dagster_file_goes_by_name(self):
        assert all(
            discovery.is_dagster_file(name)
            for name in (
                "a/defs.yaml",
                "pyproject.toml",
                "workspace.yaml",
                "x/dagster_cloud.yaml",
                "a.py",
            )
        )
        assert not any(discovery.is_dagster_file(n) for n in ("a/defs.yml", "a.sql", "README.md"))
