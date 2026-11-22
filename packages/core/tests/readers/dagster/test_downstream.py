"""Non-dbt assets downstream of dbt assets (task 8.7, D-b): @asset, AssetIn, AssetSpec and
@multi_asset, matched to the dbt nodes they read, and linked as tasks that consume them."""

from __future__ import annotations

import ast
import importlib.util
import json
import textwrap
from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.analysis.impact import impact
from lineagepilot_core.graph.model import DAGSTER, EdgeKind, NodeKind
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.common.values import CallV, Const, ListV, Unknown
from lineagepilot_core.readers.dagster import deps
from lineagepilot_core.readers.dagster.python import _inputs
from lineagepilot_core.readers.dagster.reader import DagsterReader
from lineagepilot_core.readers.dagster.values import AssetsV
from lineagepilot_core.readers.views import consumers_view, pipelines_view

READER = DagsterReader()
S = "model.impact_shop"
CORE = Path(__file__).resolve().parents[3]
RECORD = CORE / "tests" / "fixtures" / "dagster" / "asset_keys.json"


def script():
    path = CORE / "scripts" / "dagster_key_conformance.py"
    spec = importlib.util.spec_from_file_location("dagster_key_conformance", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestAgainstDagster:
    def test_the_reader_reads_what_dagster_resolves(self, tmp_path):
        module = script()
        recorded = {a["key"]: a["nodes"] for a in json.loads(RECORD.read_text())["assets"]}
        assert list(recorded) == [key for key, _ in module.DOWNSTREAM]
        project = module.write_workspace(tmp_path)
        ours = module.reader_jobs(tmp_path, project)
        assert {key: ours[key] for key in recorded} == recorded

    def test_the_cases_differ_so_this_tests_something(self):
        recorded = {a["key"]: a["nodes"] for a in json.loads(RECORD.read_text())["assets"]}
        # A key of one part named for a dbt model joins it only in the model's group
        assert recorded["d_unmatched"] == [] and recorded["d_group"] == [f"{S}.orders"]
        assert len({tuple(n) for n in recorded.values()}) >= 6


class TestDependencyForms:
    def test_a_string_is_one_part(self):
        assert deps.asset_key(Const("a/b")) == ["a/b"]
        assert deps.asset_key(ListV([Const("a"), Const("b")])) == ["a", "b"]
        assert deps.asset_key(CallV("dagster.AssetKey", [Const("a")], {})) == ["a"]
        user = CallV("dagster.AssetKey.from_user_string", [Const("a/b")], {})
        assert deps.asset_key(user) == ["a", "b"]

    def test_each_form_of_a_dependency(self):
        definition = AssetsV("dagster_dbt.dbt_assets", [], {}, key="f.py:3")
        model = CallV(
            "dagster_dbt.get_asset_key_for_model", [ListV([definition]), Const("orders")], {}
        )
        found, why = deps.dependencies(
            ListV([Const("a"), definition, model, CallV("dagster.AssetDep", [Const("b")], {})])
        )
        assert why is None
        assert found == [
            {"key": ["a"]},
            {"definition": "f.py:3"},
            {"model": "orders", "definition": "f.py:3"},
            {"key": ["b"]},
        ]

    def test_what_cannot_be_read_says_why(self):
        _, why = deps.dependencies(ListV([Const("a"), Unknown("a runtime value")]))
        assert "a runtime value" in why
        _, why = deps.dependencies(Unknown("computed"))
        assert "not a list" in why

    def test_asset_in(self):
        assert deps.asset_in("x", CallV("dagster.AssetIn", [Const("k")], {})) == [{"key": ["k"]}]
        prefixed = CallV("dagster.AssetIn", [], {"key_prefix": ListV([Const("p")])})
        assert deps.asset_in("x", prefixed) == [{"key": ["p", "x"]}]
        assert deps.asset_in("x", CallV("dagster.AssetIn", [], {})) == [{"key": ["x"]}]
        assert "not an AssetIn" in deps.asset_in("x", Const(1))

    def test_inputs_are_the_parameters_but_context_config_and_resources(self):
        tree = ast.parse(
            "def f(context, orders, dbt: DbtCliResource, config, x: pd.DataFrame, y): ..."
        )
        assert _inputs(tree.body[0], {"y"}) == ["orders", "x"]
        typed = ast.parse("def f(ctx: AssetExecutionContext, orders): ...")
        assert _inputs(typed.body[0], set()) == ["orders"]


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    copy_impact_shop(root / "dbt")
    return root


HEAD = """
from pathlib import Path
from dagster import AssetIn, AssetKey, AssetSpec, Definitions, asset, multi_asset
from dagster_dbt import dbt_assets

M = Path(__file__).parent.parent / "dbt" / "target" / "manifest.json"

@dbt_assets(manifest=M, select="+orders"{translator})
def shop(context, dbt):
    ...
"""


def read(repo: Path, body: str, assets: str, translator: str = "") -> dict:
    text = HEAD.format(translator=translator) + textwrap.dedent(body)
    text += f"\ndefs = Definitions(assets=[shop, {assets}])\n"
    path = repo / "orch" / "defs.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")
    return READER.read(repo / "dbt", repo, ["."], None)


def only(scan: dict) -> dict:
    assert len(scan["assets"]) == 1, scan["assets"]
    return scan["assets"][0]


class TestUnknowns:
    def test_deps_that_cannot_be_read_are_an_unknown_at_the_asset(self, repo):
        found = only(read(repo, "@asset(deps=pick())\ndef out(): ...\n", "out"))
        assert (found["status"], found["category"], found["name"]) == (
            "unknown",
            "not_literal",
            "out",
        )
        assert found["line"] == 11  # the def

    def test_an_unreadable_dependency_beside_one_that_reads_dbt(self, repo):
        found = only(read(repo, '@asset(deps=["orders", pick()])\ndef out(): ...\n', "out"))
        assert found["nodes"] == [f"{S}.orders"]
        assert found["deps_unknown"]["category"] == "not_literal"

    def test_specs_worked_out_are_a_loop_or_factory(self, repo):
        body = '@multi_asset(specs=[AssetSpec(n) for n in ["a", "b"]])\ndef many(): ...\n'
        found = only(read(repo, body, "many"))
        assert found["category"] == "loop_or_factory"

    def test_an_asset_in_a_loop_that_reads_dbt_is_dynamic(self, repo):
        body = """
        made = []
        for n in ["a", "b"]:
            @asset(name=n, deps=["orders"])
            def one(): ...
            made.append(one)
        """
        found = only(read(repo, body, "*made"))
        assert found["category"] == "loop_or_factory"

    def test_a_key_that_may_be_a_hidden_dbt_key(self, repo):
        found = only(
            read(
                repo,
                '@asset(deps=["mart_orders"])\ndef out(): ...\n',
                "out",
                ", dagster_dbt_translator=T()",
            )
        )
        assert found["category"] == "custom_translator" and "mart_orders" in found["reason"]

    def test_an_asset_that_reads_no_dbt_is_left_out(self, repo):
        scan = read(repo, '@asset(deps=["elsewhere"])\ndef out(): ...\n', "out")
        assert scan["assets"] == []


class TestLinked:
    @pytest.fixture
    def graph(self, repo):
        body = """
        @asset(deps=["orders"], key_prefix=["ml"])
        def features(): ...

        dashboard = AssetSpec(["bi", "revenue"], deps=[AssetKey("stg_orders")])
        """
        scan = read(repo, body, "features, dashboard")
        graph = index_project(repo / "dbt").graph
        READER.link(graph, scan)
        return graph

    def test_a_downstream_asset_is_a_task_that_consumes_what_it_reads(self, graph):
        task = graph.nodes["dagster:orch.defs.ml/features"]
        assert (task.kind, task.subkind, task.tool) == (NodeKind.TASK, "asset", DAGSTER)
        assert task.properties["pipeline"] == "orch.defs"
        [edge] = graph.out_edges(task.id, (EdgeKind.CONSUMES,))
        assert (
            edge.downstream == f"{S}.orders" and edge.transformation == "reads, as a Dagster asset"
        )
        assert "dagster:orch.defs.bi/revenue" in graph.nodes

    def test_a_change_reaches_it(self, graph):
        assert "dagster:orch.defs.ml/features" in impact(graph, [f"{S}.orders"]).nodes

    def test_the_pipelines_view_lists_it_under_its_code_location(self, graph):
        view = pipelines_view(graph, [f"{S}.orders"], [DAGSTER])
        [location] = [p for p in view["pipelines"] if p["id"] == "dagster:orch.defs"]
        task = next(t for t in location["tasks"] if t["id"] == "dagster:orch.defs.ml/features")
        assert task["nodes"] == [f"{S}.orders"] and task["how"] == ["reads, as a Dagster asset"]

    def test_the_consumers_view_keeps_to_consumers(self, graph):
        ids = {c["id"] for c in consumers_view(graph, [f"{S}.orders"])["consumers"]}
        assert not any(i.startswith("dagster:") for i in ids)
        # The semantic layer still reads the mart
        assert any(i.startswith("semantic_model.") for i in ids)


def test_the_review_lists_a_downstream_asset_in_pipelines_with_what_it_reads(repo):
    from lineagepilot_core.analysis.review import ChangedFile, FileStatus, review_changes
    from lineagepilot_core.project import read_project

    scan = read(repo, '@asset(deps=["orders"])\ndef features(): ...\n', "features")
    graph = index_project(repo / "dbt").graph
    READER.link(graph, scan)
    review = review_changes(
        graph,
        [ChangedFile("models/marts/orders.sql", FileStatus.MODIFIED)],
        read_project(repo / "dbt"),
    )
    rows = {n["id"]: n for n in review["impacted"]}
    row = rows["dagster:orch.defs.features"]
    assert (row["kind"], row["tool"], row["pipeline"]["runs"]) == ("task", DAGSTER, ["orders"])
    assert row["pipeline"]["how"] == ["reads, as a Dagster asset"]
    markdown = review["markdown"]
    pipelines = markdown.split("### Pipelines")[1].split("### Owners")[0]
    assert "| `orch.defs` | asset `features` | reads, as a Dagster asset: `orders` |" in pipelines
    # Not a consumer: the review has no Consumers section for it (D-b)
    assert "### Consumers" not in markdown
