"""The server runs any reader the same way (task 7.3): the watch, the options and the worker.

A second reader is registered for each test, beside Airflow, so nothing here depends on what
Airflow's reader does.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from lineagepilot_core.readers.registry import READERS

from lineagepilot_lsp import worker
from lineagepilot_lsp.project import read_project
from lineagepilot_lsp.server import Options
from lineagepilot_lsp.watch import ALL_READERS, Change, classify, watchers


class FakeReader:
    """Reads ``*.fake`` files below its folders, and says which it found."""

    tool = "fake"
    option = "fakePaths"
    default_paths = ("pipelines",)
    files = "the fake files"

    def clean_paths(self, paths):
        return [p for p in paths if isinstance(p, str) and ".." not in p]

    def watch_globs(self, paths):
        return [f"{p}/**/*.fake" for p in self.clean_paths(paths)]

    def owns(self, relative, paths):
        return relative.endswith(".fake") and any(
            relative.startswith(f"{p}/") for p in self.clean_paths(paths)
        )

    def read(self, project_root, workspace, paths, manifest):
        found = sorted(
            p.relative_to(workspace).as_posix()
            for folder in paths
            for p in (Path(workspace) / folder).rglob("*.fake")
        )
        return {"files": found, "manifest": manifest}

    def link(self, graph, scan):
        return None

    def unlink(self, graph):
        pass

    def mark_not_analysed(self, graph, reason):
        pass


@pytest.fixture
def fake(monkeypatch):
    monkeypatch.setitem(READERS, "fake", FakeReader())
    return READERS["fake"]


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "dbt"
    root.mkdir()
    (root / "dbt_project.yml").write_text("name: shop\n")
    return read_project(root)


PATHS = {"airflow": ["dags"], "fake": ["pipelines", "../escape"]}


class TestWatch:
    def test_each_reader_has_its_globs_watched_in_the_workspace(self, fake, project, tmp_path):
        found = {
            (w.glob_pattern.base_uri, w.glob_pattern.pattern)
            for w in watchers(project, tmp_path, PATHS)
        }
        assert (tmp_path.as_uri(), "dags/**/*.py") in found
        assert (tmp_path.as_uri(), "pipelines/**/*.fake") in found
        assert not any("escape" in pattern for _, pattern in found)

    def test_a_changed_file_is_read_again_by_the_reader_that_owns_it(self, fake, project, tmp_path):
        def change(path):
            return classify(project, tmp_path, PATHS, tmp_path / path)

        assert change("pipelines/a/x.fake") == (Change.READERS, "fake")
        assert change("dags/d.py") == (Change.READERS, "airflow")
        assert change("pipelines/x.py") is None
        assert change("dbt/.lineagepilot/lineage.yml") == (Change.READERS, ALL_READERS)

    def test_a_file_two_readers_own_is_read_again_by_all(self, fake, project, tmp_path):
        both = {"airflow": ["shared"], "fake": ["shared"]}
        fake.owns = lambda relative, paths: relative.startswith("shared/")
        assert classify(project, tmp_path, both, tmp_path / "shared" / "d.py") == (
            Change.READERS,
            ALL_READERS,
        )


class TestOptions:
    def test_every_reader_gets_its_defaults(self, fake):
        options = Options.from_json({})
        assert options.reader_paths == {"airflow": ["**/dags"], "fake": ["pipelines"]}

    def test_each_reader_has_its_own_option(self, fake):
        options = Options.from_json({"dagFolders": ["orchestration"], "fakePaths": ["x", 3]})
        assert options.reader_paths == {"airflow": ["orchestration"], "fake": ["x"]}

    def test_a_malformed_option_keeps_the_default(self, fake):
        assert Options.from_json({"fakePaths": "x"}).reader_paths["fake"] == ["pipelines"]


class TestWorker:
    def run(self, request):
        out = io.StringIO()
        code = worker.main(request, out)
        return code, [json.loads(line) for line in out.getvalue().splitlines()]

    def test_the_worker_runs_the_reader_named_by_its_tool(self, fake, tmp_path):
        (tmp_path / "pipelines" / "a").mkdir(parents=True)
        (tmp_path / "pipelines" / "a" / "x.fake").write_text("")
        output = tmp_path / "out" / "scan.json"
        request = {
            "task": "read",
            "tool": "fake",
            "projectDir": str(tmp_path),
            "workspace": str(tmp_path),
            "paths": ["pipelines"],
            "manifest": "target/manifest.json",
            "output": str(output),
        }
        code, events = self.run(request)
        assert code == 0
        assert events == [{"event": "read", "tool": "fake", "path": str(output)}]
        assert json.loads(output.read_text()) == {
            "files": ["pipelines/a/x.fake"],
            "manifest": "target/manifest.json",
        }

    def test_a_tool_with_no_reader_fails_with_words(self, tmp_path):
        code, events = self.run({"task": "read", "tool": "nope", "output": str(tmp_path / "x")})
        assert code == 1 and events[0]["event"] == "failed" and "KeyError" in events[0]["message"]
