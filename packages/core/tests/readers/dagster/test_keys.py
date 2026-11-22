"""Dagster asset keys for dbt nodes (task 8.5): dagster-dbt's default rule, a component's
template and prefix, the legacy loaders' prefix, and translators that cannot be read."""

from __future__ import annotations

import importlib.util
import json
import textwrap
from pathlib import Path

import pytest
from support import copy_impact_shop

from lineagepilot_core.graph.model import DAGSTER, EdgeKind, MetadataGraph, Origin
from lineagepilot_core.indexer import index_project
from lineagepilot_core.readers.common.manifest import manifest_from_dict
from lineagepilot_core.readers.dagster import keys
from lineagepilot_core.readers.dagster.keys import KeyRule, asset_keys, default_key, render
from lineagepilot_core.readers.dagster.reader import DagsterReader

READER = DagsterReader()
S = "model.impact_shop"
CORE = Path(__file__).resolve().parents[3]
RECORD = CORE / "tests" / "fixtures" / "dagster" / "asset_keys.json"


def script():
    """scripts/dagster_key_conformance.py, which CI runs with dagster-dbt itself."""
    path = CORE / "scripts" / "dagster_key_conformance.py"
    spec = importlib.util.spec_from_file_location("dagster_key_conformance", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def node(**fields):
    raw = {"resource_type": "model", "name": "orders", "config": {}, **fields}
    uid = f"{raw['resource_type']}.p.{raw['name']}"
    return manifest_from_dict({"nodes": {uid: raw}, "metadata": {}}).nodes[uid]


@pytest.fixture(scope="module")
def recorded():
    """dagster-dbt's own answers, recorded by the conformance script."""
    return json.loads(RECORD.read_text(encoding="utf-8"))


class TestAgainstDagsterDbt:
    def test_the_reader_gives_dagster_dbt_s_keys(self, recorded):
        assert recorded["dagster_dbt"] == "0.29.29" and recorded["dagster"] == "1.13.29"
        assert script().compare(recorded["cases"]) == []

    def test_every_rule_is_reached_so_this_tests_something(self, recorded):
        module = script()
        variants = {
            c["unique_id"]: c["translator"]
            for c in recorded["cases"]
            if c["manifest"] == "impact-shop-variants"
        }
        assert {uid: variants[uid] for uid in module.CONTROLS} == module.CONTROLS
        kinds = {c["node"]["resource_type"] for c in recorded["cases"]}
        assert {"model", "seed", "source"} <= kinds
        assert any(c["dbt_assets"] for c in recorded["cases"])

    def test_a_reader_that_ignored_a_rule_would_be_caught(self, recorded, monkeypatch):
        module = script()
        monkeypatch.setattr(module, "default_key", lambda n: [n.name])
        assert len(module.compare(recorded["cases"])) >= 5

    def test_the_record_is_of_the_fixtures_as_they_are(self, recorded):
        module = script()
        now = {
            (label, uid): {key: props.get(key) for key in module.FIELDS}
            for label, raw in module.manifests().items()
            for section in ("nodes", "sources")
            for uid, props in raw[section].items()
        }
        for case in recorded["cases"]:
            fields = {key: case["node"][key] for key in module.FIELDS}
            assert now[(case["manifest"], case["unique_id"])] == fields, case["unique_id"]


class TestDefaultKey:
    def test_the_rules_in_order(self):
        assert default_key(node()) == ["orders"]
        assert default_key(node(config={"schema": "mart"})) == ["mart", "orders"]
        assert default_key(node(version=2, alias="orders_v2")) == ["orders_v2"]
        source = node(resource_type="source", source_name="shop", name="raw_orders")
        assert default_key(source) == ["shop", "raw_orders"]

    def test_meta_from_the_config_first(self):
        set_in_config = node(config={"meta": {"dagster": {"asset_key": ["a", "b"]}}})
        assert default_key(set_in_config) == ["a", "b"]
        assert default_key(node(meta={"dagster": {"asset_key": "one"}})) == ["one"]
        hidden = node(config={"meta": {"owner": "x"}}, meta={"dagster": {"asset_key": ["no"]}})
        assert default_key(hidden) == ["orders"]

    @pytest.mark.parametrize("configured", [[1, 2], {"a": "b"}, ["ok", None]])
    def test_a_key_dagster_would_refuse_is_none(self, configured):
        assert default_key(node(meta={"dagster": {"asset_key": configured}})) is None


class TestTemplates:
    @pytest.mark.parametrize(
        ("template", "key"),
        [
            ("dbt/{{ node.name }}", ["dbt", "orders"]),
            ("{{node.schema}}/{{ node.name }}", ["main", "orders"]),
            ("{{ node.config.materialized }}_{{ node.name }}", ["table_orders"]),
            ("{{ node.meta.team }}/{{ node.resource_type }}", ["finance", "model"]),
        ],
    )
    def test_node_fields_fill_the_template(self, template, key):
        found = node(schema="main", config={"materialized": "table"}, meta={"team": "finance"})
        assert keys.check_template(template) is None
        assert render(template, found) == key

    def test_a_field_the_node_does_not_have(self):
        assert render("{{ node.meta.team }}", node()) is None
        assert render("{{ node.tags }}", node()) is None

    @pytest.mark.parametrize(
        "template", ["{{ node.name | upper }}", "{% if x %}a{% endif %}", "{{ env('X') }}", "a}}"]
    )
    def test_anything_else_is_not_read(self, template):
        assert keys.check_template(template) is not None

    def test_asset_keys_with_a_prefix_and_what_cannot_be_given(self):
        manifest = manifest_from_dict(
            {
                "nodes": {
                    "model.p.a": {"resource_type": "model", "name": "a", "config": {}},
                    "model.p.b": {
                        "resource_type": "model",
                        "name": "b",
                        "config": {},
                        "meta": {"dagster": {"asset_key": [1]}},
                    },
                },
                "metadata": {},
            }
        )
        found, why = asset_keys(
            manifest, ["model.p.a", "model.p.b", "model.p.gone"], KeyRule(prefix=("x",))
        )
        assert found == {"model.p.a": ["x", "a"]}
        assert "the meta.dagster.asset_key of b" in why


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    copy_impact_shop(root / "dbt")
    (root / "orch").mkdir()
    return root


def write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")


def read(repo: Path) -> dict:
    return READER.read(repo / "dbt", repo, ["."], None)


def definition(repo: Path, arguments: str, imports: str = "") -> dict:
    write(
        repo,
        "orch/assets.py",
        f"""
        from pathlib import Path
        from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, dbt_assets
        {imports}
        M = Path(__file__).parent.parent / "dbt" / "target" / "manifest.json"

        @dbt_assets(manifest=M, select="orders raw_orders", {arguments})
        def shop(context, dbt):
            ...
        """,
    )
    scan = read(repo)
    assert len(scan["definitions"]) == 1, (scan["unknowns"], scan["problems"])
    return scan["definitions"][0]


class TestPythonDefinitions:
    WANTED = {f"{S}.orders": ["orders"], "seed.impact_shop.raw_orders": ["raw", "raw_orders"]}

    @pytest.mark.parametrize(
        "translator",
        [
            "dagster_dbt_translator=None",
            "dagster_dbt_translator=DagsterDbtTranslator()",
            "dagster_dbt_translator=DagsterDbtTranslator("
            "settings=DagsterDbtTranslatorSettings(enable_asset_checks=False))",
        ],
    )
    def test_dagster_dbt_s_own_translator_gives_the_default_keys(self, repo, translator):
        found = definition(repo, translator)
        assert found["keys"] == self.WANTED and "keys_unknown" not in found

    @pytest.mark.parametrize(
        ("translator", "imports", "words"),
        [
            (
                "dagster_dbt_translator=Custom()",
                "class Custom(DagsterDbtTranslator):\n            pass",
                "Custom(), a translator of the workspace's own",
            ),
            ("dagster_dbt_translator=make()", "from .t import make", "make()"),
            ("dagster_dbt_translator=TRANSLATOR", "", "not known here"),
        ],
    )
    def test_any_other_translator_is_a_named_unknown_and_the_links_stand(
        self, repo, translator, imports, words
    ):
        found = definition(repo, translator, imports)
        assert found["nodes"] == [f"{S}.orders", "seed.impact_shop.raw_orders"]
        assert found["keys_unknown"]["category"] == "custom_translator"
        assert words in found["keys_unknown"]["reason"] and "keys" not in found

    @pytest.mark.parametrize(
        ("prefix", "key"),
        [('"dbt"', ["dbt", "orders"]), ('["a", "b"]', ["a", "b", "orders"])],
    )
    def test_a_legacy_loader_s_key_prefix(self, repo, prefix, key):
        write(
            repo,
            "orch/assets.py",
            f"""
            from dagster_dbt import load_assets_from_dbt_project
            from dagster import file_relative_path
            shop = load_assets_from_dbt_project(
                file_relative_path(__file__, "../dbt"), select="orders", key_prefix={prefix}
            )
            """,
        )
        [found] = read(repo)["definitions"]
        assert found["keys"] == {f"{S}.orders": key}

    @pytest.mark.parametrize(
        ("argument", "words"),
        [
            ("key_prefix=PREFIX", "key_prefix is not a literal"),
            ("node_info_to_asset_key=lambda info: info", "node_info_to_asset_key"),
        ],
    )
    def test_what_a_legacy_loader_sets_another_way(self, repo, argument, words):
        write(
            repo,
            "orch/assets.py",
            f"""
            from dagster_dbt import load_assets_from_dbt_project
            from dagster import file_relative_path
            shop = load_assets_from_dbt_project(
                file_relative_path(__file__, "../dbt"), select="orders", {argument}
            )
            """,
        )
        [found] = read(repo)["definitions"]
        assert words in found["keys_unknown"]["reason"]


class TestLinked:
    def linked(self, repo: Path, arguments: str, imports: str = "") -> MetadataGraph:
        definition(repo, arguments, imports)
        graph = index_project(repo / "dbt").graph
        READER.link(graph, read(repo))
        return graph

    def test_the_definition_has_each_node_s_key(self, repo):
        graph = self.linked(repo, "name='shop'")
        task = graph.nodes["dagster:orch.assets.shop"]
        assert task.properties["assetKeys"] == {
            f"{S}.orders": "orders",
            "seed.impact_shop.raw_orders": "raw/raw_orders",
        }

    def test_a_custom_translator_is_an_unknown_beside_the_links(self, repo):
        graph = self.linked(
            repo, "dagster_dbt_translator=Custom()", "class Custom(DagsterDbtTranslator): ..."
        )
        task = graph.nodes["dagster:orch.assets.shop"]
        assert len(graph.out_edges(task.id, (EdgeKind.ORCHESTRATES,))) == 2
        assert "assetKeys" not in task.properties
        [unknown] = [u for u in graph.unresolved if u.evidence.origin is Origin.DAGSTER_DEFS]
        assert unknown.subject == task.id and unknown.reference == "custom translator"
        assert "asset keys come from code that runs only when Dagster loads" in unknown.reason
        summary = graph.readers[DAGSTER]
        assert (summary.links, summary.unknowns) == (2, 1)

    def test_keys_that_are_not_lists_of_strings_are_not_trusted(self):
        from lineagepilot_core.readers.dagster.link import _keys

        assert _keys({"a": ["x", "y"], "b": "x", "c": [1], "d": [], 3: ["x"]}) == {"a": "x/y"}
        assert _keys(None) == {}
