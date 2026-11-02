"""Inference rules: layer, domain, PII, semantic type, tags and catalog enrichment.

Ported from data-capsule ``backend/tests/parsers/test_dbt_parser.py``. These run on the
legacy hand-written fixture, because it exercises the meta and tag paths that the real
fixtures do not use. Its shape is not trusted: tests that depend on real manifest shape
live in test_dbt_parser_real.py.
"""

from __future__ import annotations

import pytest
from support import project_config

from lineagepilot_core.parsers import DbtParser, ParseErrorSeverity, ParseResult


@pytest.fixture(scope="module")
def result() -> ParseResult:
    return DbtParser().parse(project_config("legacy-v11"))


def node(result: ParseResult, name: str, resource_type: str | None = None):
    return next(
        n for n in result.nodes if n.name == name and resource_type in (None, n.resource_type)
    )


def column(result: ParseResult, node_name: str, column_name: str):
    node_id = node(result, node_name).unique_id
    return next(c for c in result.columns if c.node_id == node_id and c.name == column_name)


class TestParsing:
    def test_project_and_version(self, result):
        assert result.source_type == "dbt"
        assert result.project_name == "jaffle_shop"
        assert result.dbt_version == "1.7.0"

    def test_models(self, result):
        models = {n.name for n in result.nodes if n.resource_type == "model"}
        assert models == {
            "stg_customers",
            "stg_orders",
            "int_customer_orders",
            "dim_customers",
            "fct_orders",
        }

    def test_sources_are_bronze(self, result):
        sources = [n for n in result.nodes if n.resource_type == "source"]
        assert {s.name for s in sources} == {"customers", "orders"}
        assert {s.layer for s in sources} == {"bronze"}

    def test_seeds_are_bronze(self, result):
        (seed,) = [n for n in result.nodes if n.resource_type == "seed"]
        assert seed.name == "country_codes"
        assert seed.layer == "bronze"

    def test_without_catalog(self):
        result = DbtParser().parse(project_config("legacy-v11", catalog=False))
        assert not result.has_errors
        assert result.nodes

    def test_only_the_schema_warning(self, result):
        assert [e.severity for e in result.errors] == [ParseErrorSeverity.WARNING]

    def test_identity_is_the_unique_id(self, result):
        stg = node(result, "stg_customers")
        assert stg.unique_id == "model.jaffle_shop.stg_customers"
        assert column(result, "stg_customers", "customer_id").node_id == stg.unique_id

    def test_summary_counts(self, result):
        summary = result.summary()
        assert summary["nodes"] == len(result.nodes)
        assert summary["columns"] == len(result.columns)
        assert summary["edges"] == len(result.edges)
        assert summary["unresolved"] == len(result.unresolved)
        assert summary["domains"] == len(result.domains)
        assert summary["tags"] == len(result.tags)
        assert (summary["warnings"], summary["errors"]) == (1, 0)


class TestLayerInference:
    def test_staging_and_intermediate_are_silver(self, result):
        for name in ("stg_customers", "stg_orders", "int_customer_orders"):
            assert node(result, name).layer == "silver", name

    def test_marts_are_gold(self, result):
        assert node(result, "dim_customers").layer == "gold"
        assert node(result, "fct_orders").layer == "gold"

    def test_meta_layer_wins(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        manifest["nodes"]["model.jaffle_shop.stg_orders"]["config"]["meta"] = {"layer": "Gold"}
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert node(result, "stg_orders").layer == "gold"

    def test_layer_tag_wins_over_patterns(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        manifest["nodes"]["model.jaffle_shop.stg_orders"]["tags"] = ["Presentation"]
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert node(result, "stg_orders").layer == "gold"

    def test_windows_paths_match(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        stg = manifest["nodes"]["model.jaffle_shop.stg_orders"]
        stg["name"] = "orders_cleaned"  # no name pattern, so only the path can decide
        stg["original_file_path"] = "models\\staging\\orders_cleaned.sql"
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert node(result, "orders_cleaned").layer == "silver"

    def test_no_match_is_none(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        stg = manifest["nodes"]["model.jaffle_shop.stg_orders"]
        stg["name"], stg["original_file_path"] = "orders_x", "models/misc/orders_x.sql"
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert node(result, "orders_x").layer is None


class TestDomainInference:
    def test_domain_from_meta(self, result):
        assert node(result, "dim_customers").domain_name == "core"

    def test_domains_extracted(self, result):
        assert "core" in {d.name for d in result.domains}

    def test_domain_from_tag_then_path(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        manifest["nodes"]["model.jaffle_shop.orders"]["tags"] = ["Domain:Finance"]
        manifest["nodes"]["model.jaffle_shop.customers"]["original_file_path"] = (
            "models/marts/_private/crm/customers.sql"
        )
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert node(result, "orders").domain_name == "finance"
        assert node(result, "customers").domain_name == "crm"

    def test_folder_that_is_not_a_name_is_ignored(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        manifest["nodes"]["model.jaffle_shop.orders"]["original_file_path"] = "models/v2/orders.sql"
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert node(result, "orders").domain_name is None


class TestPIIDetection:
    def test_pii_from_meta_flag(self, result):
        first_name = column(result, "stg_customers", "first_name")
        assert (first_name.pii_type, first_name.pii_detected_by) == ("name", "tag")

    def test_pii_from_string_meta(self, result):
        email = column(result, "stg_customers", "email")
        assert (email.pii_type, email.pii_detected_by) == ("email", "tag")

    def test_pii_from_sensitive_meta(self, result):
        email = column(result, "dim_customers", "email")
        assert (email.pii_type, email.pii_detected_by) == ("sensitive", "tag")

    def test_pii_from_name_pattern(self, result):
        assert any(
            c.name == "email" and c.pii_type == "email" and c.pii_detected_by == "pattern"
            for c in result.columns
        )

    def test_pii_detection_can_be_disabled(self):
        config = {**project_config("legacy-v11"), "pii_detection_enabled": False}
        result = DbtParser().parse(config)
        assert not [c for c in result.columns if c.pii_detected_by == "pattern"]

    def test_pii_from_tags_and_untyped_flag(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        columns = manifest["nodes"]["model.jaffle_shop.customers"]["columns"]
        columns["customer_id"]["tags"] = ["PII"]
        columns["customer_name"]["tags"] = ["pii:Name"]
        columns["customer_type"]["meta"] = {"pii": True, "pii_type": 7}
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert column(result, "customers", "customer_id").pii_type == "unknown"
        assert column(result, "customers", "customer_name").pii_type == "name"
        assert column(result, "customers", "customer_type").pii_type == "unknown"


class TestColumns:
    def test_columns_extracted(self, result):
        node_id = node(result, "stg_customers").unique_id
        names = {c.name for c in result.columns if c.node_id == node_id}
        assert {"customer_id", "first_name", "email"} <= names

    def test_types_and_position_from_catalog(self, result):
        customer_id = column(result, "stg_customers", "customer_id")
        assert customer_id.data_type == "NUMBER"
        assert customer_id.ordinal_position == 1

    def test_stats_from_catalog(self, result):
        assert column(result, "stg_customers", "customer_id").stats["row_count"] == 100

    def test_hand_written_data_tests_are_ignored(self, result):
        """The fixture lists data_tests on columns; real manifests never do (S2
        suspicion 2). Tests are taken only from test.* nodes, and it has none."""
        customer_id = column(result, "stg_customers", "customer_id")
        assert (customer_id.has_tests, customer_id.test_count) == (False, 0)


class TestSemanticTypeInference:
    def test_timestamp(self, result):
        assert column(result, "stg_customers", "created_at").semantic_type == "timestamp"

    def test_metric(self, result):
        assert column(result, "stg_orders", "total_amount").semantic_type == "metric"

    @pytest.mark.parametrize(
        ("name", "semantic_type"),
        [
            ("id", "surrogate_key"),
            ("order_pk", "surrogate_key"),
            ("customer_id", "foreign_key"),
            ("surrogate_key", "surrogate_key"),
            ("business_key", "business_key"),
            ("customer_bk_id", "business_key"),
            ("notes", None),
        ],
    )
    def test_keys(self, name, semantic_type):
        parser = DbtParser()
        assert parser._infer_semantic_type(name, {"meta": {}}, None) == semantic_type

    def test_meta_and_pii_win(self):
        parser = DbtParser()
        assert parser._infer_semantic_type("x", {"meta": {"semantic_type": "code"}}, None) == "code"
        assert parser._infer_semantic_type("customer_id", {"meta": {}}, "email") == "pii"


class TestLineageAndTags:
    def test_source_to_staging_edge(self, result):
        source = node(result, "customers", "source").unique_id
        stg = node(result, "stg_customers").unique_id
        edge = next(e for e in result.edges if (e.upstream_id, e.downstream_id) == (source, stg))
        assert edge.transformation == "source"

    def test_model_to_model_edge(self, result):
        stg = node(result, "stg_customers").unique_id
        intermediate = node(result, "int_customer_orders").unique_id
        edge = next(
            e for e in result.edges if (e.upstream_id, e.downstream_id) == (stg, intermediate)
        )
        assert edge.transformation == "ref"

    @pytest.mark.parametrize(
        ("upstream", "transformation"),
        [
            ("source.p.raw.orders", "source"),
            ("model.p.orders", "ref"),
            ("seed.p.codes", "ref"),
            ("snapshot.p.orders_snapshot", "dependency"),
        ],
    )
    def test_transformation_follows_the_upstream_type(self, upstream, transformation):
        from lineagepilot_core.parsers.dbt_parser import _transformation

        assert _transformation(upstream) == transformation

    def test_tags_extracted_and_categorised(self, result):
        tags = {t.name: t.category for t in result.tags}
        assert "staging" in tags
        assert tags["pii"] == "sensitivity"
        assert all(tags[t] == "sensitivity" for t in tags if t.startswith("pii"))

    def test_domain_tag_category(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        manifest["nodes"]["model.jaffle_shop.orders"]["tags"] = ["domain:finance"]
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        assert {t.name: t.category for t in result.tags}["domain:finance"] == "domain"


class TestRuleInputs:
    def test_layer_source_says_how_the_layer_was_decided(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        nodes = manifest["nodes"]
        nodes["model.jaffle_shop.orders"]["config"]["meta"] = {"layer": "gold"}
        nodes["model.jaffle_shop.customers"]["tags"] = ["mart"]
        nodes["model.jaffle_shop.locations"]["original_file_path"] = "models/misc/locations.sql"
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        sources = {n.name: n.layer_source for n in result.nodes if n.resource_type == "model"}
        assert sources["orders"] == "meta"
        assert sources["customers"] == "tag"
        assert sources["stg_orders"] == "pattern"
        assert sources["locations"] is None
        assert {n.layer_source for n in result.nodes if n.resource_type != "model"} == {"default"}

    def test_contract_primary_keys(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        orders = manifest["nodes"]["model.jaffle_shop.orders"]
        orders["constraints"] = [
            {"type": "primary_key", "columns": ["order_id", "location_id"]},
            {"type": "check", "expression": "order_total >= 0"},
            "not-a-constraint",
        ]
        customers = manifest["nodes"]["model.jaffle_shop.customers"]
        customers["columns"]["customer_id"]["constraints"] = [{"type": "primary_key"}]
        customers["constraints"] = "not-a-list"
        result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
        by_name = {n.name: n for n in result.nodes}
        assert by_name["orders"].primary_key == ["order_id", "location_id"]
        assert by_name["customers"].primary_key == ["customer_id"]
        assert by_name["stg_orders"].primary_key == []
