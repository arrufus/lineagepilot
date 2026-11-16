"""The reader contract (task 7.3), and Airflow as its first reader."""

from __future__ import annotations

import json

import pytest
from support import IMPACT_SHOP

from lineagepilot_core.graph.model import NodeKind
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.airflow.dag_static import DEFAULT_DAG_FOLDERS
from lineagepilot_core.readers.airflow.reader import READER as AIRFLOW_READER
from lineagepilot_core.readers.airflow.workspace import ANNOTATION_FILE, scan_airflow
from lineagepilot_core.readers.contract import Reader
from lineagepilot_core.readers.registry import READERS, reader_for


@pytest.mark.parametrize("reader", READERS, ids=lambda r: r.tool)
def test_every_reader_keeps_the_contract(reader):
    assert isinstance(reader, Reader)
    assert reader.tool and reader.files and reader.option
    assert reader.clean_paths(reader.default_paths) == list(reader.default_paths)
    # A setting cannot point outside the workspace
    assert reader.clean_paths(["../elsewhere", "/abs", "..", ""]) == []
    for annotation in reader.annotation_files:
        assert not annotation.startswith(("/", ".."))


def test_each_tool_has_one_reader():
    tools = [r.tool for r in READERS]
    assert len(tools) == len(set(tools))
    assert reader_for("airflow") is AIRFLOW_READER
    with pytest.raises(KeyError):
        reader_for("no-such-tool")


class TestAirflow:
    reader = AIRFLOW_READER

    def test_discovery_keeps_the_dag_folders_setting(self):
        assert self.reader.option == "dagFolders"
        assert self.reader.default_paths == DEFAULT_DAG_FOLDERS
        assert self.reader.annotation_files == (ANNOTATION_FILE,)
        paths = self.reader.clean_paths(["dags", "../x", "orchestration/dags"])
        assert paths == ["dags", "orchestration/dags"]
        assert self.reader.watch_globs(paths) == ["dags/**/*.py", "orchestration/dags/**/*.py"]
        assert self.reader.watches("dags/sub/d.py", paths)
        assert not self.reader.watches("dags/d.txt", paths)
        assert not self.reader.watches("other/d.py", paths)

    def test_reading_is_the_dag_scan(self):
        root = str(IMPACT_SHOP)
        found = self.reader.read(root, root, ["**/dags"], None)
        assert json.loads(json.dumps(found)) == found  # JSON, for the child process to write
        assert found == scan_airflow(root, root, ["**/dags"], None)

    def test_linking_then_not_analysed_then_unlinked(self):
        graph = index_project(IMPACT_SHOP).graph
        before = set(graph.nodes)
        root = str(IMPACT_SHOP)
        summary = self.reader.link(graph, self.reader.read(root, root, ["**/dags"], None))
        added = {n.id for n in graph.nodes.values() if n.tool == "airflow"}
        assert added and summary.analysed and graph.summaries["airflow"] is summary
        assert {graph.nodes[n].kind for n in added} == {NodeKind.PIPELINE, NodeKind.TASK}

        self.reader.not_analysed(graph, "the DAG files are still being read")
        assert set(graph.nodes) == before
        assert graph.summaries["airflow"].reason == "the DAG files are still being read"

        self.reader.unlink(graph)
        assert set(graph.nodes) == before and "airflow" not in graph.summaries
