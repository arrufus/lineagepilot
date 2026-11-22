"""The Dagster reader behind the reader contract (task 8.2): its setting, its files, its summary."""

from __future__ import annotations

from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.graph.model import (
    DAGSTER,
    PIPELINE_KINDS,
    Confidence,
    Evidence,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    ReaderSummary,
    Unresolved,
)
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.contract import Reader
from lineagepilot_core.readers.dagster.reader import DagsterReader
from lineagepilot_core.readers.registry import READERS, reader
from lineagepilot_core.readers.views import reader_summaries

DAGSTER_READER = DagsterReader()


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class TestRegistry:
    def test_dagster_is_the_second_reader(self):
        assert isinstance(reader(DAGSTER), DagsterReader) and isinstance(DAGSTER_READER, Reader)
        assert sorted(READERS) == ["airflow", DAGSTER]
        assert DAGSTER_READER.option == "dagsterPaths"
        assert DAGSTER_READER.default_paths == (".",) and DAGSTER_READER.kinds == PIPELINE_KINDS
        assert DAGSTER_READER.origins == frozenset({Origin.DAGSTER_DEFS})


class TestDiscovery:
    def test_the_setting_is_cleaned_before_use(self):
        found = DAGSTER_READER.clean_paths(
            ["orchestration", "../escape", "/abs", 7, "**/defs", "."]
        )
        assert found == ["orchestration", "**/defs", "."]

    def test_its_globs_are_its_files_under_each_folder(self):
        assert DAGSTER_READER.watch_globs([".", "../escape"]) == [
            "**/*.py",
            "**/defs.yaml",
            "**/pyproject.toml",
            "**/workspace.yaml",
            "**/dagster_cloud.yaml",
        ]
        assert DAGSTER_READER.watch_globs(["orchestration"])[:2] == [
            "orchestration/**/*.py",
            "orchestration/**/defs.yaml",
        ]

    @pytest.mark.parametrize(
        ("relative", "paths", "owned"),
        [
            ("definitions.py", ["."], True),
            ("a/b/defs.yaml", ["."], True),
            ("a/pyproject.toml", ["."], True),
            ("workspace.yaml", ["."], True),
            ("a/README.md", ["."], False),
            ("models/orders.sql", ["."], False),
            ("orchestration/defs.py", ["orchestration"], True),
            ("other/defs.py", ["orchestration"], False),
            ("team/defs/dbt/defs.yaml", ["**/defs"], True),
        ],
    )
    def test_which_files_are_its_own(self, relative, paths, owned):
        assert DAGSTER_READER.owns(relative, paths) is owned


class TestLifecycle:
    def test_read_link_unlink(self, tmp_path):
        root = copy_impact_shop(tmp_path / "impact_shop")
        write(root, "orchestration/definitions.py", "from dagster import Definitions\n")
        write(root, "orchestration/defs/dbt/defs.yaml", "type: dagster_dbt.DbtProjectComponent\n")
        graph = index_project(root).graph
        before = graph.stats()
        scan = DAGSTER_READER.read(root, root, DAGSTER_READER.default_paths, None)
        assert scan["tool"] == DAGSTER and scan["project"] == "." and scan["paths"] == ["."]
        assert scan["files"]["python"] == ["orchestration/definitions.py"]
        assert scan["files"]["components"] == ["orchestration/defs/dbt/defs.yaml"]
        summary = DAGSTER_READER.link(graph, scan)
        assert summary is graph.readers[DAGSTER] and summary.analysed and summary.files == 2
        assert summary.reason is None and summary.paths == (".",)
        DAGSTER_READER.unlink(graph)
        assert graph.stats() == before and DAGSTER not in graph.readers

    def test_nothing_found_says_where_it_looked(self, tmp_path):
        graph = MetadataGraph()
        summary = DAGSTER_READER.link(graph, DAGSTER_READER.read(tmp_path, tmp_path, ["."], None))
        assert summary.analysed and summary.files == 0
        assert summary.reason == "no Dagster files found in the workspace"

    def test_folders_someone_chose_that_hold_nothing_are_not_analysed(self, tmp_path):
        """As the DAG scan does: the definitions may be elsewhere, and a review says so."""
        graph = MetadataGraph()
        scan = DAGSTER_READER.read(tmp_path, tmp_path, ["orchestration", "**/defs"], None)
        summary = DAGSTER_READER.link(graph, scan)
        assert not summary.analysed and summary.reason == (
            "no Dagster files were found in orchestration, **/defs; "
            "set lineagePilot.dagster.paths if they are elsewhere"
        )
        summary = DAGSTER_READER.link(graph, {"paths": [], "files": {}})
        assert not summary.analysed and summary.reason.startswith(
            "no Dagster files were found in the folders set;"
        )

    def test_unlink_takes_out_its_nodes_and_unknowns_only(self):
        graph = MetadataGraph()
        graph.add_node(Node(id="dagster:x", kind=NodeKind.PIPELINE, name="x", tool=DAGSTER))
        graph.add_node(Node(id="airflow:d", kind=NodeKind.PIPELINE, name="d", tool="airflow"))
        for origin in (Origin.DAGSTER_DEFS, Origin.DAG_STATIC):
            graph.add_unresolved(
                Unresolved(
                    subject="s",
                    reference=origin.value,
                    reason="r",
                    evidence=Evidence(origin=origin, confidence=Confidence.HEURISTIC),
                )
            )
        DAGSTER_READER.link(graph, {"files": {"python": ["a.py"]}})
        DAGSTER_READER.unlink(graph)
        assert list(graph.nodes) == ["airflow:d"]
        assert [u.reference for u in graph.unresolved] == ["dag_static"]

    def test_not_analysed_says_why(self):
        graph = MetadataGraph()
        DAGSTER_READER.mark_not_analysed(graph, "the Dagster definitions are still being read")
        summary = graph.readers[DAGSTER]
        assert isinstance(summary, ReaderSummary) and not summary.analysed
        assert summary.reason == "the Dagster definitions are still being read"

    def test_a_malformed_read_is_not_trusted(self):
        graph = MetadataGraph()
        summary = DAGSTER_READER.link(
            graph,
            {
                "files": {"python": ["a.py", 3, None], "components": "x"},
                "paths": "x",
                "problems": [1],
            },
        )
        assert (summary.files, summary.paths, summary.problems) == (1, (), ())


class TestSummaryJson:
    def test_as_the_protocol_has_it(self):
        graph = MetadataGraph()
        DAGSTER_READER.link(
            graph, {"paths": ["."], "files": {"python": ["a.py"]}, "problems": ["p"]}
        )
        assert DAGSTER_READER.summary_json(graph) == {
            "tool": DAGSTER,
            "analysed": True,
            "reason": None,
            "paths": ["."],
            "files": 1,
            "pipelines": 0,
            "tasks": 0,
            "consumers": 0,
            "links": 0,
            "unknowns": 0,
            "opaqueTasks": 0,
            "problems": ["p"],
        }

    def test_before_any_read(self):
        for graph in (None, MetadataGraph()):
            found = DAGSTER_READER.summary_json(graph)
            assert (
                not found["analysed"] and found["reason"] == "the Dagster definitions were not read"
            )

    def test_text_from_the_workspace_is_clipped(self):
        graph = MetadataGraph()
        DAGSTER_READER.link(graph, {"paths": ["p" * 5000], "problems": ["q" * 5000]})
        found = DAGSTER_READER.summary_json(graph)
        assert len(found["paths"][0]) == len(found["problems"][0]) == 4000


class TestInTheReview:
    """A reader that was never asked to read, or read and found none of its files, says nothing
    in a review: the review of a project with no Dagster is as it was before task 8.2."""

    def test_a_reader_is_listed_once_it_has_read_or_been_marked(self):
        graph = MetadataGraph()
        assert [s["tool"] for s in reader_summaries(graph, PIPELINE_KINDS)] == ["airflow"]
        DAGSTER_READER.mark_not_analysed(graph, "not yet")
        assert [s["tool"] for s in reader_summaries(graph, PIPELINE_KINDS)] == ["airflow", DAGSTER]
        assert [s["tool"] for s in reader_summaries(None, PIPELINE_KINDS)] == ["airflow"]
