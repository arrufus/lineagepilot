"""Dagster's unknowns (task 8.8): each pattern of design_next_phase section 6.4 at the line that
causes it, with the lineage.yml entry that answers it; and annotations with tool: dagster."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.graph.model import DAGSTER, Confidence, EdgeKind, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.airflow.link import link_airflow
from lineagepilot_core.readers.airflow.scan import scan_airflow
from lineagepilot_core.readers.common.categories import explanation
from lineagepilot_core.readers.dagster.reader import DagsterReader

READER = DagsterReader()
S = "model.impact_shop"
LINEAGE = "dbt/.lineagepilot/lineage.yml"
HEAD = """
import os
import subprocess
from pathlib import Path
from dagster import AssetSpec, Definitions, asset, multi_asset, op
from dagster_dbt import DagsterDbtTranslator, dbt_assets

M = Path(__file__).parent.parent / "dbt" / "target" / "manifest.json"
"""


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    copy_impact_shop(root / "dbt")
    return root


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")


def linked(repo: Path, body: str, annotations: str | None = None):
    write(repo, "orch/defs.py", HEAD + textwrap.dedent(body))
    if annotations is not None:
        write(repo, LINEAGE, annotations)
    graph = index_project(repo / "dbt").graph
    scan = READER.read(repo / "dbt", repo, ["."], None)
    READER.link(graph, scan)
    return graph, scan


def unknowns(graph) -> list:
    return [u for u in graph.unresolved if u.subject.startswith(f"{DAGSTER}:")]


def the_unknown(graph, category_label: str):
    found = [u for u in unknowns(graph) if u.reference == category_label]
    assert len(found) == 1, [(u.subject, u.reference, u.reason) for u in unknowns(graph)]
    return found[0]


# Each pattern: (the code, the unknown's label, its subject, its line, words in the reason, and the
# lineage.yml that answers it)
PATTERNS = {
    "custom translator": (
        """
        class Custom(DagsterDbtTranslator): ...
        @dbt_assets(manifest=M, select="orders", dagster_dbt_translator=Custom())
        def shop(context, dbt): ...

        defs = Definitions(assets=[shop])
        """,
        "custom translator",
        "dagster:orch.defs.shop",
        11,
        "Custom(), a translator of the workspace's own",
        "pipelines:\n  - pipeline_id: orch.defs\n    tool: dagster\n    tasks:\n"
        "      - {task_id: shop, transforms: [model.impact_shop.orders]}\n",
    ),
    "keys at run time": (
        """
        @dbt_assets(manifest=M, select="orders")
        def shop(context, dbt): ...

        @asset(key=make_key(), deps=["orders"])
        def exported(): ...

        defs = Definitions(assets=[shop, exported])
        """,
        "value not known",
        "dagster:orch.defs.asset_13",
        13,
        "the asset's key is not known here",
        "acknowledged:\n  - {file: orch/defs.py, line: 13}\n",
    ),
    "specs worked out": (
        """
        @dbt_assets(manifest=M, select="orders")
        def shop(context, dbt): ...

        @multi_asset(specs=[AssetSpec(n, deps=["orders"]) for n in ["a", "b"]])
        def many(): ...

        defs = Definitions(assets=[shop, many])
        """,
        "loop or factory",
        "dagster:orch.defs.many",
        13,
        "the multi_asset's specs",
        "acknowledged:\n  - {task_id: many}\n",
    ),
    "a factory": (
        """
        def make(select):
            @dbt_assets(manifest=M, select=select, name="made")
            def made(context, dbt): ...
            return made
        """,
        "loop or factory",
        "dagster:orch.defs.made",
        11,
        "never called at module level",
        "acknowledged:\n  - {dag_id: orch.defs, task_id: made}\n",
    ),
    "assets from outside the workspace": (
        """
        from shared_lib.dbt import shared_assets

        defs = Definitions(assets=[shared_assets])
        """,
        "call not followed",
        "dagster:orch.defs",
        11,
        "shared_lib.dbt.shared_assets, from a module that is not in the workspace",
        "acknowledged:\n  - {dag_id: orch.defs}\n",
    ),
    "dbt from an op": (
        """
        @op
        def run_dbt(context):
            subprocess.run(["dbt", "build", "--select", "orders"], check=True)
        """,
        "dbt from Python",
        "dagster:orch.defs",
        10,
        "run_dbt() runs dbt from Python (subprocess.run)",
        "acknowledged:\n  - {file: orch/defs.py}\n",
    ),
    "Dagster Pipes": (
        """
        @asset
        def piped(context, pipes_subprocess_client):
            return pipes_subprocess_client.run(command=["dbt", "build"], context=context)
        """,
        "dbt from Python",
        "dagster:orch.defs.piped",
        10,
        "(Dagster Pipes)",
        "pipelines:\n  - pipeline_id: orch.defs\n    tool: dagster\n    tasks:\n"
        "      - {task_id: piped, transforms: [model.impact_shop.orders]}\n",
    ),
}


class TestEachPattern:
    @pytest.mark.parametrize("name", list(PATTERNS))
    def test_it_is_an_unknown_at_the_line_that_causes_it(self, repo, name):
        body, label, subject, line, words, _ = PATTERNS[name]
        graph, _ = linked(repo, body)
        found = the_unknown(graph, label)
        assert found.subject == subject
        assert found.reason.startswith(f"../orch/defs.py:{line}: "), found.reason
        assert words in found.reason, found.reason
        assert found.evidence.origin is Origin.DAGSTER_DEFS

    @pytest.mark.parametrize("name", list(PATTERNS))
    def test_the_annotation_file_answers_it(self, repo, name):
        body, label, subject, _, _, answer = PATTERNS[name]
        graph, _ = linked(repo, body, answer)
        assert not [u for u in unknowns(graph) if u.reference == label]

    def test_the_explanations_name_no_airflow_operator(self):
        for category in ("loop_or_factory", "wrapper", "call_not_followed", "python_dbt_call"):
            assert "operator" not in explanation(category)
            assert "Airflow" not in explanation(category)


class TestAnnotations:
    def test_a_declared_task_reads_and_runs_what_it_says(self, repo):
        graph, _ = linked(
            repo,
            """
            @asset
            def exported(): ...
            defs = Definitions(assets=[exported])
            """,
            "pipelines:\n  - pipeline_id: orch.defs\n    tool: dagster\n    owner: data-eng\n"
            "    tasks:\n      - task_id: exported\n        consumes: [model.impact_shop.orders]\n"
            "        transforms: [model.impact_shop.stg_orders]\n",
        )
        task = graph.nodes["dagster:orch.defs.exported"]
        assert task.owner == "data-eng" and task.subkind == "asset"
        [reads] = graph.out_edges(task.id, (EdgeKind.CONSUMES,))
        [runs] = graph.out_edges(task.id, (EdgeKind.ORCHESTRATES,))
        assert (reads.downstream, runs.downstream) == (f"{S}.orders", f"{S}.stg_orders")
        [evidence] = reads.evidence
        assert (evidence.origin, evidence.confidence) == (Origin.ANNOTATION, Confidence.DECLARED)
        assert graph.readers[DAGSTER].analysed

    def test_a_name_that_matches_nothing_is_an_unknown_and_is_taken_out_again(self, repo):
        annotations = (
            "pipelines:\n  - pipeline_id: loc\n    tool: dagster\n    tasks:\n"
            "      - {task_id: t, consumes: [model.impact_shop.gone]}\n"
        )
        graph, scan = linked(repo, "defs = Definitions(assets=[])\n", annotations)
        [found] = unknowns(graph)
        assert found.subject == "dagster:loc.t" and found.evidence.origin is Origin.ANNOTATION
        # The DAG scan, linked again, leaves the Dagster reader's alone
        link_airflow(graph, scan_airflow(repo / "dbt", repo))
        assert [u.subject for u in unknowns(graph)] == ["dagster:loc.t"]
        READER.link(graph, {**scan, "annotations": {}})
        assert unknowns(graph) == [] and "dagster:loc.t" not in graph.nodes

    def test_the_dag_scan_no_longer_says_dagster_has_no_reader(self, repo):
        write(
            repo,
            LINEAGE,
            "pipelines:\n  - pipeline_id: loc\n    tool: dagster\n"
            "    tasks:\n      - {task_id: t}\n",
        )
        scan = scan_airflow(repo / "dbt", repo)
        assert not any("dagster" in p for p in scan["annotations"]["problems"])


class TestEnvironment:
    def test_a_conditional_expression_takes_the_unset_branch_as_a_guess(self, repo):
        graph, scan = linked(
            repo,
            """
            MANIFEST = M if not os.getenv("PARSE_ON_LOAD") else parse()

            @dbt_assets(manifest=MANIFEST, select="orders")
            def shop(context, dbt): ...
            defs = Definitions(assets=[shop])
            """,
        )
        [found] = scan["definitions"]
        assert found["nodes"] == [f"{S}.orders"] and found["confidence"] == "heuristic"
        assert any("PARSE_ON_LOAD" in note for note in found["notes"])
