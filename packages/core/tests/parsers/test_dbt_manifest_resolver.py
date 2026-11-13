"""dbt selector resolution against a manifest.

Ported from data-capsule ``backend/tests/unit/parsers/test_dbt_manifest_resolver.py``.
The URN test went with ``model_to_capsule_urn``. Phase 5 replaces the selector semantics.
"""

from __future__ import annotations

import pytest
from support import DBT_FIXTURES

from lineagepilot_core.parsers.dbt_manifest_resolver import (
    DbtManifestResolver,
    DbtModelReference,
)


@pytest.fixture
def sample_manifest():
    return {
        "metadata": {"dbt_version": "1.5.0", "project_name": "analytics"},
        "nodes": {
            "model.analytics.customers": {
                "unique_id": "model.analytics.customers",
                "name": "customers",
                "schema": "marts",
                "database": "analytics_db",
                "package_name": "analytics",
                "tags": ["core", "customers"],
                "config": {"materialized": "table"},
                "depends_on": {"nodes": ["source.analytics.raw_customers"]},
            },
            "model.analytics.orders": {
                "unique_id": "model.analytics.orders",
                "name": "orders",
                "schema": "marts",
                "database": "analytics_db",
                "package_name": "analytics",
                "tags": ["core", "orders"],
                "config": {"materialized": "incremental"},
                "depends_on": {
                    "nodes": ["source.analytics.raw_orders", "model.analytics.customers"]
                },
            },
            "model.analytics.finance_revenue_by_month": {
                "unique_id": "model.analytics.finance_revenue_by_month",
                "name": "revenue_by_month",
                "schema": "finance",
                "database": "analytics_db",
                "package_name": "analytics",
                "tags": ["finance", "reporting"],
                "config": {"materialized": "table"},
                "depends_on": {"nodes": ["model.analytics.orders"]},
            },
            "source.analytics.raw_customers": {
                "unique_id": "source.analytics.raw_customers",
                "name": "raw_customers",
                "source_name": "raw",
            },
        },
    }


@pytest.fixture
def manifest_file(sample_manifest, write_json):
    return str(write_json(sample_manifest))


@pytest.fixture
def resolver(manifest_file):
    resolver = DbtManifestResolver(manifest_file)
    assert resolver.load_manifest()
    return resolver


class TestDbtManifestResolver:
    def test_load_only_models(self, resolver):
        assert set(resolver._models_cache) == {
            "model.analytics.customers",
            "model.analytics.orders",
            "model.analytics.finance_revenue_by_month",
        }

    def test_load_real_manifest(self):
        resolver = DbtManifestResolver()
        assert resolver.load_manifest(str(DBT_FIXTURES / "pilot-1.12" / "manifest.json"))
        assert len(resolver._models_cache) == 11

    def test_load_missing_file(self):
        assert not DbtManifestResolver().load_manifest("/nonexistent/manifest.json")

    def test_load_without_a_path(self):
        assert not DbtManifestResolver().load_manifest()

    def test_load_invalid_json(self, tmp_path):
        path = tmp_path / "manifest.json"
        path.write_text("{not json")
        assert not DbtManifestResolver().load_manifest(str(path))

    def test_load_non_object(self, write_json):
        assert not DbtManifestResolver().load_manifest(str(write_json([1, 2])))

    def test_malformed_nodes_are_skipped(self, write_json):
        manifest = {"nodes": {"model.p.bad": [], "model.p.ok": {"name": "ok", "tags": "x"}}}
        resolver = DbtManifestResolver()
        assert resolver.load_manifest(str(write_json(manifest)))
        (model,) = resolver._models_cache.values()
        assert (model.name, model.tags, model.materialization) == ("ok", [], "table")

    def test_resolve_by_tag(self, resolver):
        assert [m.name for m in resolver.resolve_selector("tag:finance")] == ["revenue_by_month"]
        assert {m.name for m in resolver.resolve_selector("tag:core")} == {"customers", "orders"}

    def test_resolve_by_name(self, resolver):
        (model,) = resolver.resolve_selector("customers")
        assert (model.name, model.schema) == ("customers", "marts")

    def test_resolve_by_package_model(self, resolver):
        (model,) = resolver.resolve_selector("analytics.customers")
        assert (model.name, model.package_name) == ("customers", "analytics")

    def test_resolve_by_path(self, resolver):
        """Simplified semantics, unchanged from data-capsule: a unique_id substring."""
        assert [m.name for m in resolver.resolve_selector("finance/*")] == ["revenue_by_month"]

    def test_resolve_without_manifest(self):
        assert DbtManifestResolver().resolve_selector("customers") == []

    def test_get_model_by_name(self, resolver):
        model = resolver.get_model_by_name("orders")
        assert model is not None and model.materialization == "incremental"
        assert resolver.get_model_by_name("nonexistent") is None

    def test_get_model_by_name_with_duplicates(self, write_json):
        manifest = {
            "nodes": {
                "model.a.x": {"name": "x", "package_name": "a"},
                "model.b.x": {"name": "x", "package_name": "b"},
            }
        }
        resolver = DbtManifestResolver(str(write_json(manifest)))
        resolver.load_manifest()
        assert resolver.get_model_by_name("x").package_name == "a"

    def test_operator_config_with_select(self, resolver):
        result = resolver.extract_from_operator_config({"select": "tag:finance"})
        assert result == {
            "selector": "tag:finance",
            "models": ["revenue_by_month"],
            "operation_type": "merge",
        }

    def test_operator_config_with_models_string(self, resolver):
        result = resolver.extract_from_operator_config({"models": "orders"})
        assert (result["selector"], result["models"]) == ("orders", ["orders"])

    def test_operator_config_with_models_list(self):
        result = DbtManifestResolver().extract_from_operator_config(
            {"models": ["customers", "orders"]}
        )
        assert result["models"] == ["customers", "orders"]
        assert result["selector"] is None

    def test_operator_config_without_selector(self, resolver):
        assert resolver.extract_from_operator_config({})["models"] == []


class TestDbtModelReference:
    def test_fqn(self):
        model = DbtModelReference(
            unique_id="model.analytics.customers",
            name="customers",
            schema="marts",
            database="analytics_db",
        )
        assert model.fqn == "analytics_db.marts.customers"

    def test_fqn_without_database(self):
        model = DbtModelReference(unique_id="model.a.c", name="customers", schema="marts")
        assert model.fqn == "marts.customers"
