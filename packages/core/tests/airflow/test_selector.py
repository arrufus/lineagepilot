"""The selector engine against real ``dbt ls`` answers recorded on our own fixture project."""

from __future__ import annotations

import copy
import json

import pytest
from airflow_support import FIXTURES, SHOP

from lineagepilot_core.airflow.manifest import manifest_from_dict
from lineagepilot_core.airflow.selector import (
    SelectorEngine,
    UnsupportedSelector,
    _glob_to_regex,
    is_selected_node,
    leaf_wildcards,
    parse_spec,
    select,
)
from lineagepilot_core.airflow.standin import build_standin


def _cases(answers):
    return [pytest.param(c, id=c["label"]) for c in answers["cases"]]


def test_recorded_answers_come_from_real_dbt(dbt_ls_answers):
    assert dbt_ls_answers["dbt_version"].startswith("1.")
    assert len(dbt_ls_answers["cases"]) >= 20
    assert all(c["dbt_returncode"] == 0 for c in dbt_ls_answers["cases"])


def test_engine_matches_dbt_ls_on_every_supported_expression(shop_manifest, dbt_ls_answers):
    disagreements = []
    for c in dbt_ls_answers["cases"]:
        got = select(shop_manifest, c["select"], c["exclude"], c["selector"])
        if c["label"].startswith("UNSUPPORTED"):
            assert got.unknown, f"{c['label']} must be unknown, not a guess"
            continue
        if got.unknown or sorted(got.ids) != c["dbt_ids"]:
            disagreements.append((c["label"], got.reason, sorted(got.ids or []), c["dbt_ids"]))
    assert disagreements == []


def test_standin_manifest_matches_dbt_ls_on_our_project(dbt_ls_answers):
    """The approximate stand-in, built from files only, also agrees with dbt on this project."""
    standin = build_standin(str(SHOP))
    assert standin.approximate
    bad = []
    for c in dbt_ls_answers["cases"]:
        if c["label"].startswith("UNSUPPORTED"):
            continue
        got = select(standin, c["select"], c["exclude"], c["selector"])
        if c["selector"]:
            assert got.unknown  # YAML selectors live in the manifest, not in the stand-in
            continue
        if got.unknown or sorted(got.ids) != c["dbt_ids"]:
            bad.append(c["label"])
    assert bad == []


def test_selection_notes_mark_approximate_manifests():
    standin = build_standin(str(SHOP))
    got = select(standin, ["stg_orders"])
    assert got.ids == {"model.shop.stg_orders"}
    assert any("approximate" in n for n in got.notes)


@pytest.mark.parametrize(
    "raw, method, args, value",
    [
        ("tag:daily", "tag", [], "daily"),
        ("config.materialized:view", "config", ["materialized"], "view"),
        ("models/staging", "path", [], "models/staging"),
        ("fct_orders.sql", "file", [], "fct_orders.sql"),
        ("2+fct_orders+3", "fqn", [], "fct_orders"),
    ],
)
def test_parse_spec(raw, method, args, value):
    c = parse_spec(raw)
    assert (c.method, c.args, c.value) == (method, args, value)


def test_parse_spec_depths():
    c = parse_spec("2+fct_orders+3")
    assert c.parents and c.parents_depth == 2 and c.children and c.children_depth == 3


@pytest.mark.parametrize("raw", ["@+x", "nosuch:x", "state:modified", "config:x"])
def test_unsupported_specs_raise(raw):
    with pytest.raises(UnsupportedSelector):
        parse_spec(raw)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"select": ["resource_type:bogus"]},
        {"select": ["source:a.b.c.d"]},
        {"select": ["path:/abs/path"]},
        {"selector": "missing"},
        {"select": ["selector:missing"]},
        {"select": ["exposure:weekly_report"]},
    ],
)
def test_unsupported_selections_are_unknown_not_empty(shop_manifest, kwargs):
    got = select(shop_manifest, **kwargs)
    assert got.unknown and got.reason


def test_selector_overrides_select_like_dbt(shop_manifest):
    got = select(shop_manifest, ["stg_orders"], ["fct_orders"], "finance_and_children")
    assert "model.shop.fct_orders" in got.ids
    assert any("ignored" in n for n in got.notes)


def test_selector_method_in_cli_string(shop_manifest):
    got = select(shop_manifest, ["selector:upstream_of_ltv"])
    assert "model.shop.customer_ltv" in got.ids


def test_other_resource_types_and_external_nodes():
    data = {
        "metadata": {"project_name": "p"},
        "nodes": {
            "model.p.a": {
                "resource_type": "model",
                "name": "a",
                "package_name": "p",
                "fqn": ["p", "a"],
                "original_file_path": "models/a.sql",
                "depends_on": {"nodes": ["model.q.ext"]},
            },
            "model.q.ext": {
                "resource_type": "model",
                "name": "ext",
                "package_name": "q",
                "fqn": ["q", "ext"],
                "original_file_path": "",
                "depends_on": {"nodes": []},
            },
            "test.p.t": {
                "resource_type": "test",
                "name": "t",
                "package_name": "p",
                "fqn": ["p", "t"],
                "original_file_path": "tests/t.sql",
                "config": {"severity": "WARN"},
                "depends_on": {"nodes": ["model.p.a"]},
            },
        },
        "sources": {},
    }
    m = manifest_from_dict(data)
    assert select(m, ["+a"]).ids == {"model.p.a"}
    assert select(m, ["+a"], include_external=True).ids == {"model.p.a", "model.q.ext"}
    assert select(m, ["config.severity:warn"], resource_types=("test",)).ids == {"test.p.t"}
    assert select(m, ["package:this"], resource_types=None).ids == {"model.p.a", "test.p.t"}


def test_config_booleans_and_lists():
    data = {
        "metadata": {"project_name": "p"},
        "nodes": {
            "model.p.a": {
                "resource_type": "model",
                "name": "a",
                "package_name": "p",
                "fqn": ["p", "a"],
                "original_file_path": "models/a.sql",
                "config": {"contract": {"enforced": True}, "grants": ["x", True]},
                "depends_on": {"nodes": []},
            }
        },
    }
    m = manifest_from_dict(data)
    assert select(m, ["config.contract.enforced:true"]).ids == {"model.p.a"}
    assert select(m, ["config.grants:true"]).ids == {"model.p.a"}
    assert select(m, ["config.grants:false"]).ids == set()
    assert select(m, ["config.missing:x"]).ids == set()


def test_glob_translation():
    rx = _glob_to_regex("models/**/stg_*.sql")
    assert (
        rx.match("models/a/b/stg_x.sql")
        and rx.match("models/stg_x.sql")
        and not rx.match("models/a/x.sql")
    )
    assert _glob_to_regex("m/[ab].sql").match("m/a.sql")
    assert _glob_to_regex("m/[ab.sql").match("m/[ab.sql")
    assert _glob_to_regex("m/?.sql").match("m/x.sql")


def test_is_selected_node_edges():
    assert not is_selected_node([], "x", False)
    assert not is_selected_node(["p", "a"], "p.a.b", False)
    assert is_selected_node(["p", "m", "v2"], "m", True)
    assert not is_selected_node(["p", "dir", "a"], "p.*.b", False)


def test_yaml_definition_shapes(shop_manifest):
    eng = SelectorEngine(shop_manifest)
    assert eng._definition({"tag": "core"}, 1) == eng.union(["tag:core"])
    assert eng._definition(
        {
            "method": "tag",
            "value": "finance",
            "exclude": [{"method": "fqn", "value": "fct_orders"}],
        },
        1,
    ) == eng.union(["tag:finance"]) - eng.union(["fct_orders"])
    assert eng._definition(
        {"intersection": [{"exclude": ["tag:hourly"]}, "tag:finance"]}, 1
    ) == eng.union(["tag:finance"]) - eng.union(["tag:hourly"])
    for bad in (
        ["not", "a", "dict"],
        {"union": "x"},
        {"method": "tag"},
        {"method": "tag", "value": "x", "weird": 1},
        {"method": "nosuch", "value": "x"},
        {"method": "state", "value": "x"},
        {"method": "tag", "value": "x", "exclude": "y"},
    ):
        with pytest.raises(UnsupportedSelector):
            eng._definition(bad, 1)
    assert eng._definition({"union": []}, 1) == set()
    assert eng._definition({"intersection": []}, 1) == set()
    assert eng._definition(
        {"method": "selector", "value": "nightly"}, 1
    ) == eng.select_yaml_selector("nightly")
    with pytest.raises(UnsupportedSelector):
        eng.select_yaml_selector("nightly", _depth=99)


def test_yaml_selector_without_definition():
    m = manifest_from_dict(
        {"metadata": {"project_name": "p"}, "nodes": {}, "selectors": {"s": {"name": "s"}}}
    )
    assert select(m, selector="s").unknown


class TestSemanticsFollowTheManifestsDbtVersion:
    """dbt 1.10 made a bare wildcard also match a node's leaf name. These answers are real dbt's,
    from scripts/selector_conformance.py, which CI runs on both sides of that change."""

    @pytest.fixture
    def older(self):
        path = FIXTURES / "dbt" / "shop_dbt_ls_1.9.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def manifest_of(self, manifest, version):
        pinned = copy.copy(manifest)
        pinned.metadata = {**manifest.metadata, "dbt_version": version}
        pinned._engine = None  # its selector engine knows which version it is for
        return pinned

    def test_the_answers_differ_so_this_tests_something(self, older, dbt_ls_answers):
        differs = {
            new["label"]
            for old, new in zip(older["cases"], dbt_ls_answers["cases"], strict=True)
            if old["dbt_ids"] != new["dbt_ids"]
        }
        assert differs == {"wildcard prefix"}
        assert older["dbt_version"].startswith("1.9") and dbt_ls_answers["dbt_version"] >= "1.10"

    def test_a_manifest_from_before_1_10_gets_the_older_answers(self, shop_manifest, older):
        manifest = self.manifest_of(shop_manifest, "1.9.10")
        for case in older["cases"]:
            if case["label"].startswith("UNSUPPORTED"):
                continue
            got = select(manifest, case["select"], case["exclude"], case["selector"], ("model",))
            assert sorted(got.ids) == case["dbt_ids"], case["label"]

    def test_applying_the_newer_rule_there_would_be_wrong(self, shop_manifest, older):
        wildcard = next(c for c in older["cases"] if c["label"] == "wildcard prefix")
        newer = select(self.manifest_of(shop_manifest, "1.10.0"), ["stg_*"], [], None, ("model",))
        assert sorted(newer.ids) != wildcard["dbt_ids"]

    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            ("1.9.10", False),
            ("1.8.0", False),
            ("0.21.1", False),
            ("1.10.0", True),
            ("1.12.6", True),
            ("2.0.9", True),  # Fusion
            ("not a version", True),
            (None, True),
        ],
    )
    def test_the_version_gate(self, version, expected):
        assert leaf_wildcards(version) is expected


class TestWhatIsWorkedOutOnce:
    NAMES = (
        "stg_customers",
        "fct_orders",
        "fct_revenue",
        "fct_revenue.v1",
        "fct_revenue.v2",
        "fct_revenue_v1",
        "fct_revenue_v2",
        "shop",
        "marts",
        "staging",
        "marts.finance",
        "shop.staging",
        "shop.staging.stg_orders",
        "shop.marts.finance.fct_orders",
        "finance.fct_orders",
        "nothing_called_this",
        "shop.nothing",
    )

    @pytest.mark.parametrize("name", NAMES)
    def test_the_names_index_says_what_the_definition_says(self, shop_manifest, name):
        by_definition = {
            n.unique_id
            for n in shop_manifest.nodes.values()
            if n.resource_type != "source"
            and n.fqn
            and (
                is_selected_node(n.fqn, name, n.version is not None)
                or is_selected_node(n.fqn[1:], name, n.version is not None)
            )
        }
        assert SelectorEngine(shop_manifest)._select_by(parse_spec(name)) == by_definition

    def test_a_manifest_has_one_engine(self, shop_manifest):
        first = shop_manifest.selector_engine(SelectorEngine)
        assert shop_manifest.selector_engine(SelectorEngine) is first
        select(shop_manifest, ["stg_customers"])
        assert ("fqn", (), "stg_customers") in first._methods

    def test_what_is_kept_cannot_be_changed_by_a_later_selection(self, shop_manifest):
        first = select(shop_manifest, ["stg_customers+"]).ids
        select(shop_manifest, ["stg_customers"], ["stg_customers"])
        assert select(shop_manifest, ["stg_customers+"]).ids == first
