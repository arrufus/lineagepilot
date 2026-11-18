"""The reader contract and the registry (task 7.3), with Airflow as the first reader."""

from __future__ import annotations

import pytest
from support import copy_impact_shop

from lineagepilot_core.graph.model import AIRFLOW, MetadataGraph, NodeKind
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.airflow.reader import AirflowReader
from lineagepilot_core.readers.contract import Reader
from lineagepilot_core.readers.registry import READERS, reader


class Minimal:
    """A reader for a tool with nothing in it: what any reader has to declare."""

    tool = "minimal"
    option = "minimalPaths"
    default_paths = ("minimal",)
    files = "the minimal files"
    kinds = frozenset({NodeKind.CONSUMER})
    origins = frozenset()

    def clean_paths(self, paths):
        return [p for p in paths if isinstance(p, str)]

    def watch_globs(self, paths):
        return [f"{p}/**" for p in paths]

    def owns(self, relative, paths):
        return any(relative.startswith(f"{p}/") for p in paths)

    def read(self, project_root, workspace, paths, manifest):
        return {}

    def link(self, graph, scan):
        return None

    def unlink(self, graph):
        pass

    def mark_not_analysed(self, graph, reason):
        pass

    def summary_json(self, graph):
        return {}


class TestRegistry:
    def test_every_reader_keeps_the_contract_under_its_own_tool(self):
        assert READERS
        for tool, found in READERS.items():
            assert isinstance(found, Reader) and found.tool == tool and reader(tool) is found
            assert found.option and found.files and found.default_paths

    def test_airflow_is_the_first_reader(self):
        assert isinstance(reader(AIRFLOW), AirflowReader)
        assert reader(AIRFLOW).option == "dagFolders"

    def test_a_tool_with_no_reader_is_an_error(self):
        with pytest.raises(KeyError):
            reader("no_such_tool")

    def test_a_reader_is_anything_that_keeps_the_contract(self):
        assert isinstance(Minimal(), Reader)
        no_link = type("NoLink", (), {k: v for k, v in vars(Minimal).items() if k != "link"})
        assert not isinstance(no_link(), Reader)


class TestAirflowDiscovery:
    AIRFLOW = AirflowReader()

    def test_the_setting_is_cleaned_before_use(self):
        found = self.AIRFLOW.clean_paths(["dags", "../escape", "/abs", 7, "C:/x", "a/**/dags"])
        assert found == ["dags", "a/**/dags"]

    def test_its_globs_are_python_files_under_each_folder(self):
        assert self.AIRFLOW.watch_globs(["dags", "../escape"]) == ["dags/**/*.py"]

    @pytest.mark.parametrize(
        ("relative", "owned"),
        [
            ("dags/d.py", True),
            ("dags/sub/d.py", True),
            ("team/dags/d.py", False),
            ("dags/d.txt", False),
            ("other/d.py", False),
        ],
    )
    def test_which_files_are_its_own(self, relative, owned):
        assert self.AIRFLOW.owns(relative, ["dags"]) is owned

    def test_a_pattern_finds_folders_anywhere(self):
        assert self.AIRFLOW.owns("team/dags/d.py", list(self.AIRFLOW.default_paths))


class TestAirflowLifecycle:
    def test_read_link_unlink(self, tmp_path):
        root = copy_impact_shop(tmp_path / "impact_shop")
        graph = index_project(root).graph
        before = graph.stats()
        airflow = reader(AIRFLOW)
        scan = airflow.read(root, root, airflow.default_paths, None)
        summary = airflow.link(graph, scan)
        assert summary is graph.readers[AIRFLOW] and summary.analysed and summary.dags
        airflow.unlink(graph)
        assert graph.stats() == before and AIRFLOW not in graph.readers

    def test_not_analysed_says_why(self):
        graph = MetadataGraph()
        reader(AIRFLOW).mark_not_analysed(graph, "the DAG files are still being read")
        summary = graph.readers[AIRFLOW]
        assert not summary.analysed and summary.reason == "the DAG files are still being read"
