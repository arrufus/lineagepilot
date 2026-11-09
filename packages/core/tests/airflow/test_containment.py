"""Nothing outside the workspace is read, or even looked at (audit LP-SEC-01 and LP-SEC-03).

A hostile repository can hold symlinks out of itself, and DAG files can name any path. Each test
records every path handed to the filesystem while a workspace is scanned, and fails if one is
outside the workspace (its own ancestors, which resolving it walks, are allowed). On Windows a
``stat`` of ``//host/share/x`` can open an SMB connection to that host, so a path written in a DAG
file must be refused before any filesystem call, not after.
"""

from __future__ import annotations

import builtins
import io
import json
import os
import sys
import textwrap
from pathlib import Path

import pytest

from lineagepilot_core.airflow.dag_static import discover_dag_files, glob_match, scan_dags
from lineagepilot_core.airflow.safe_io import (
    is_remote,
    read_capped,
    regular_file_inside,
    resolve_inside,
)
from lineagepilot_core.airflow.workspace import scan_airflow

CANARY = "CANARY_7f3a91"
needs_symlinks = pytest.mark.skipif(
    sys.platform == "win32", reason="creating symlinks needs Developer Mode on Windows"
)


def _from_coverage() -> bool:
    """Whether coverage.py is the caller: it looks at a module's file when one first runs."""
    frame = sys._getframe(2)
    while frame is not None:
        if f"{os.sep}coverage{os.sep}" in frame.f_code.co_filename:
            return True
        frame = frame.f_back
    return False


@pytest.fixture
def touched(monkeypatch):
    """Every path passed to the filesystem while the test runs, by anything but coverage."""
    seen: list[str] = []

    def trace(module, name):
        real = getattr(module, name)

        def traced(*args, **kwargs):
            if args and isinstance(args[0], (str, os.PathLike)) and not _from_coverage():
                seen.append(os.fspath(args[0]))
            return real(*args, **kwargs)

        monkeypatch.setattr(module, name, traced)

    for name in ("stat", "lstat", "open", "scandir", "listdir", "readlink"):
        trace(os, name)
    trace(os.path, "realpath")
    trace(builtins, "open")
    trace(io, "open")
    return seen


def outside(paths: list[str], workspace: Path) -> list[str]:
    ws = os.path.realpath(workspace)
    found = []
    for p in paths:
        full = os.path.normpath(os.path.abspath(p))
        inside = full == ws or full.startswith(ws + os.sep)
        ancestor = ws.startswith(full.rstrip(os.sep) + os.sep)
        if not (inside or ancestor):
            found.append(p)
    return sorted(set(found))


def dbt_project(root: Path, name: str = "shop") -> Path:
    (root / "models").mkdir(parents=True)
    (root / "dbt_project.yml").write_text(f"name: {name}\n")
    (root / "models" / "orders.sql").write_text("select 1 as id\n")
    return root


class TestSymlinksOut:
    """LP-SEC-01: links out of the workspace are not followed, and nothing they hold is shown."""

    @needs_symlinks
    def test_nothing_behind_a_link_out_is_read_or_shown(self, tmp_path, touched):
        secret = tmp_path / "outside"
        (secret / "dags").mkdir(parents=True)
        # An invalid regex and an over-long line: both used to be echoed into "problems"
        (secret / "ignore").write_text(f"{CANARY}_(\n{CANARY}{'x' * 300}\n")
        (secret / "dags" / "secret_dag.py").write_text(
            f"from airflow import DAG\nwith DAG('{CANARY}'): pass\n"
        )
        (secret / "airflow.yml").write_text(f"path_map:\n  /{CANARY}: .\n")
        (secret / "model.sql").write_text(f"select * from {{{{ ref('{CANARY}') }}}}\n")
        (secret / "manifest.json").write_text(json.dumps({"metadata": {"project_name": CANARY}}))

        ws = tmp_path / "ws"
        shop = dbt_project(ws / "shop")
        (shop / ".lineagepilot").mkdir()
        (shop / ".lineagepilot" / "airflow.yml").symlink_to(secret / "airflow.yml")
        (shop / "target").mkdir()
        (shop / "target" / "manifest.json").symlink_to(secret / "manifest.json")
        (shop / "models" / "linked.sql").symlink_to(secret / "model.sql")
        (ws / "dags").mkdir()
        (ws / "dags" / ".airflowignore").symlink_to(secret / "ignore")
        (ws / "dags" / "real_dag.py").write_text(
            "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
            "with DAG('d'):\n    BashOperator(task_id='t', bash_command='dbt run --project-dir shop')\n"
        )
        (ws / "dags" / "linked_dag.py").symlink_to(secret / "dags" / "secret_dag.py")
        (ws / "airflow").mkdir()
        (ws / "airflow" / "dags").symlink_to(secret / "dags", target_is_directory=True)
        (ws / "linked_dags").symlink_to(secret / "dags", target_is_directory=True)

        touched.clear()  # what the test itself wrote
        result = scan_airflow(shop, ws, ["**/dags", "linked_dags", "dags"])

        assert CANARY not in json.dumps(result)
        assert outside(touched, ws) == []
        assert result["folders"] == ["dags"]
        assert result["stats"]["dags"] == 1  # the real DAG is still read

    @needs_symlinks
    def test_an_airflowignore_line_is_reported_by_number_not_shown(self, tmp_path):
        (tmp_path / "dags").mkdir()
        (tmp_path / "dags" / ".airflowignore").write_text(f"ok_.*\n{CANARY}_(\n")
        _, _, problems = discover_dag_files(str(tmp_path / "dags"))
        assert problems == [
            ".airflowignore line 2 is not a safe regular expression; it is only read as a glob"
        ]

    @needs_symlinks
    def test_a_link_inside_the_workspace_is_followed(self, tmp_path):
        (tmp_path / "shared").mkdir()
        (tmp_path / "shared" / "ignore").write_text("skipped_.*\n")
        (tmp_path / "dags").mkdir()
        (tmp_path / "dags" / ".airflowignore").symlink_to(tmp_path / "shared" / "ignore")
        (tmp_path / "dags" / "skipped_dag.py").write_text("# airflow dag\n")
        (tmp_path / "dags" / "kept_dag.py").write_text("# airflow dag\n")
        files, skipped, _ = discover_dag_files(str(tmp_path / "dags"), workspace=str(tmp_path))
        assert [Path(f).name for f in files] == ["kept_dag.py"]
        assert skipped == [{"path": "skipped_dag.py", "why": ".airflowignore"}]
        # Without the workspace, the folder is the boundary
        files, _, _ = discover_dag_files(str(tmp_path / "dags"))
        assert len(files) == 2


DAG_NAMING_REMOTE_PATHS = r"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

with DAG("remote") as dag:
    BashOperator(task_id="a", bash_command="dbt run --project-dir //evil-host/share/shop")
    BashOperator(task_id="b", bash_command="cd //evil-host/share/shop && dbt run")
    BashOperator(task_id="c", bash_command=r"dbt run --project-dir \\evil-host\share\shop")
    BashOperator(task_id="d", bash_command="dbt run --project-dir C:shop")
    BashOperator(task_id="e", bash_command="dbt run --project-dir /usr/local/airflow/shop")
    SQLExecuteQueryOperator(task_id="s", sql="//evil-host/share/q.sql")
    SQLExecuteQueryOperator(task_id="t", sql=r"\\evil-host\share\q.sql")
    DbtTaskGroup(group_id="g1", project_config=ProjectConfig("//evil-host/share/shop"))
    DbtTaskGroup(
        group_id="g2",
        project_config=ProjectConfig(
            manifest_path="//evil-host/share/shop/target/manifest.json", project_name="shop"
        ),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    )
    DbtTaskGroup(
        group_id="g3",
        project_config=ProjectConfig(
            manifest_path=r"/x/\\evil-host\share/target/manifest.json", project_name="shop"
        ),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    )
    DbtTaskGroup(
        group_id="g4",
        project_config=ProjectConfig("//evil-host/share/shop"),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS_FILE, dbt_ls_path="//evil-host/share/ls.txt"
        ),
    )
"""


class TestPathsWrittenInDagFiles:
    """LP-SEC-03: a path a DAG file names is checked as a string before any filesystem call."""

    def test_no_remote_path_reaches_the_filesystem(self, tmp_path, touched):
        ws = tmp_path / "ws"
        shop = dbt_project(ws / "shop")
        (shop / ".lineagepilot").mkdir()
        (shop / ".lineagepilot" / "airflow.yml").write_text(
            "path_map:\n  /usr/local/airflow: //evil-host/share\n  /opt/a: '\\\\evil-host\\share'\n"
        )
        (ws / "dags").mkdir()
        (ws / "dags" / "remote.py").write_text(textwrap.dedent(DAG_NAMING_REMOTE_PATHS))

        touched.clear()
        result = scan_airflow(shop, ws, ["dags"])

        assert outside(touched, ws) == [], "a path outside the workspace reached the filesystem"
        # On POSIX a backslash is part of a file name, so \\evil-host\share under the project is
        # a local name inside the workspace; on Windows it is refused like //evil-host
        assert not any(is_remote(p) for p in touched)
        problems = result["annotations"]["problems"]
        assert "path_map: '/usr/local/airflow' leaves the workspace; ignored" in problems
        # The scan still answers: by the path's last component, as for a container path
        assert result["stats"]["dags"] == 1

    def test_is_remote(self):
        for path in ("//host/share", r"\\host\share", r"\\?\UNC\host\x", r"\\.\pipe\x", "C:x"):
            assert is_remote(path), path
        for path in ("/usr/local", "C:/dags", "C:\\dags", "dags/x", ""):
            assert not is_remote(path), path


class TestResolveInside:
    def test_lexical_checks_come_first(self, tmp_path, touched):
        root = str(tmp_path)
        for path in (
            "//evil-host/share/x",
            r"\\evil-host\share\x",
            str(tmp_path / ".." / "elsewhere"),
            "/etc/passwd",
            "relative/path",
            "C:x",
            "",
            "a\0b",
        ):
            assert resolve_inside(path, root) is None, path
        assert outside(touched, tmp_path) == []

    def test_a_path_inside_resolves_to_itself(self, tmp_path):
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "f.py").write_text("x")
        real = os.path.realpath(tmp_path)
        assert resolve_inside(str(tmp_path / "a" / "f.py"), str(tmp_path)) == os.path.join(
            real, "a", "f.py"
        )
        assert resolve_inside(str(tmp_path / "a" / "missing"), str(tmp_path)) == os.path.join(
            real, "a", "missing"
        )
        assert resolve_inside(str(tmp_path), str(tmp_path)) == real
        assert regular_file_inside(str(tmp_path / "a"), str(tmp_path)) is None

    @needs_symlinks
    def test_links_are_followed_only_while_they_stay_inside(self, tmp_path, touched):
        ws, secret = tmp_path / "ws", tmp_path / "secret"
        (ws / "a" / "b").mkdir(parents=True)
        secret.mkdir()
        (secret / "key").write_text(CANARY)
        (ws / "a" / "b" / "f.txt").write_text("inside")
        (ws / "rel").symlink_to("a/b/f.txt")
        (ws / "abs").symlink_to(ws / "a" / "b")
        (ws / "a" / "up").symlink_to("..")
        (ws / "chain").symlink_to("rel")
        (ws / "out_abs").symlink_to(secret / "key")
        (ws / "out_rel").symlink_to("../secret/key")
        (ws / "a" / "sneaky").symlink_to("b/../../../secret/key")
        (ws / "loop1").symlink_to("loop2")
        (ws / "loop2").symlink_to("loop1")
        (ws / "unc").symlink_to("//evil-host/share/x")
        f = os.path.join(os.path.realpath(ws), "a", "b", "f.txt")
        touched.clear()
        assert resolve_inside(str(ws / "rel"), str(ws)) == f
        assert resolve_inside(str(ws / "chain"), str(ws)) == f
        assert resolve_inside(str(ws / "abs" / "f.txt"), str(ws)) == f
        assert resolve_inside(str(ws / "a" / "up" / "a" / "b" / "f.txt"), str(ws)) == f
        for name in ("out_abs", "out_rel", "a/sneaky", "loop1", "unc"):
            assert resolve_inside(str(ws / name), str(ws)) is None, name
            assert read_capped(str(ws / name), 100, str(ws)) is None, name
        assert outside(touched, ws) == []

    @pytest.mark.skipif(sys.platform == "win32", reason="no FIFOs on Windows")
    def test_a_fifo_is_not_read_and_does_not_block(self, tmp_path):
        os.mkfifo(tmp_path / "fifo")
        assert read_capped(str(tmp_path / "fifo"), 100, str(tmp_path)) is None


class TestFolderPatterns:
    """LP-SEC-05: a folder pattern is matched without a backtracking regular expression."""

    @pytest.mark.parametrize(
        ("path", "pattern", "expected"),
        [
            ("dags", "dags", True),
            ("a/dags", "dags", True),
            ("a/dags", "*/dags", True),
            ("a/b/dags", "a/*/dags", True),
            ("a/dags", "a/*/dags", False),
            ("dags", "/dags", False),
            ("x/dags", "/dags", False),
            ("dags/x", "dags", False),
            ("Dags", "dags", False),
            ("a", "", False),
        ],
    )
    def test_glob_match_is_purepath_match(self, path, pattern, expected):
        assert glob_match(path, pattern) is expected
        if pattern:
            assert Path(path).match(pattern) is expected or sys.platform == "win32"

    def test_an_exploding_pattern_is_fast(self):
        import time

        name = "a" * 250
        started = time.perf_counter()
        for _ in range(100):
            assert not glob_match(f"x/{name}", "*a*a*a*a*a*a*a*a*a*b")
        assert time.perf_counter() - started < 1

    def test_a_pattern_folder_setting_is_bounded(self):
        from lineagepilot_core.airflow.dag_static import clean_dag_folder

        assert clean_dag_folder("**/dags") == "**/dags"
        assert clean_dag_folder("*a*a*a*a*a*a*a*b") is None
        assert clean_dag_folder("a/" * 150) is None

    def test_scanning_with_a_hostile_pattern_is_fast(self, tmp_path):
        import time

        (tmp_path / ("a" * 200)).mkdir()
        started = time.perf_counter()
        scan_dags(str(tmp_path), ["*a*a*a*a*b"])
        assert time.perf_counter() - started < 2
