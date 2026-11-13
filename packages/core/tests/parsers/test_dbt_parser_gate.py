"""The manifest version gate, structural checks and hostile input.

Each test starts from a real manifest and changes one thing, so a failure points at the
check under test rather than at an unrealistic fixture.
"""

from __future__ import annotations

import pytest
from support import DBT_FIXTURES, project_config

from lineagepilot_core.parsers import DbtParser, DbtParserConfig, ParseErrorSeverity, SchemaSupport
from lineagepilot_core.parsers.dbt_parser import (
    classify_schema,
    index_tests,
    schema_version_number,
    structural_problems,
)

V = "https://schemas.getdbt.com/dbt/manifest/v{}.json"


def parse_manifest(write_json, manifest, **extra):
    return DbtParser().parse({"manifest_path": str(write_json(manifest)), **extra})


def errors(result, severity=ParseErrorSeverity.ERROR):
    return [e.message for e in result.errors if e.severity == severity]


# Version gate


@pytest.mark.parametrize(
    ("url", "number"),
    [
        (V.format(12), 12),
        (V.format(9), 9),
        ("https://schemas.getdbt.com/dbt/manifest/v120.json", 120),
        ("https://schemas.getdbt.com/dbt/catalog/v1.json", None),
        ("v12", None),
        (None, None),
        (12, None),
    ],
)
def test_schema_version_number(url, number):
    assert schema_version_number(url) == number


def test_classify_schema():
    assert classify_schema(12) is SchemaSupport.SUPPORTED
    assert classify_schema(11) is SchemaSupport.OLDER
    assert classify_schema(13) is SchemaSupport.NEWER
    assert classify_schema(None) is SchemaSupport.UNVERSIONED


def test_renamed_depends_on_in_a_newer_schema_is_an_error_not_an_empty_graph(
    write_json, manifest_of
):
    """S2 finding 4i: this manifest used to parse 'successfully' with no edges or errors."""
    manifest = manifest_of("synth-24")
    manifest["metadata"]["dbt_schema_version"] = V.format(99)
    for node in manifest["nodes"].values():
        node["parents"] = node.pop("depends_on", {})
    result = parse_manifest(write_json, manifest)
    assert result.has_errors
    assert any("have no 'depends_on'" in m for m in errors(result))
    assert result.nodes == [] and result.edges == []
    assert result.schema_support is SchemaSupport.NEWER


def test_an_intact_newer_schema_parses_with_a_warning(write_json, manifest_of):
    manifest = manifest_of("synth-24")
    manifest["metadata"]["dbt_schema_version"] = V.format(13)
    result = parse_manifest(write_json, manifest)
    assert not result.has_errors
    assert result.schema_support is SchemaSupport.NEWER
    (warning,) = errors(result, ParseErrorSeverity.WARNING)
    assert "v13 is newer than v12" in warning
    assert len(result.edges) == 45


def test_a_missing_schema_version_parses_with_a_warning(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    del manifest["metadata"]["dbt_schema_version"]
    result = parse_manifest(write_json, manifest)
    assert not result.has_errors
    assert result.schema_support is SchemaSupport.UNVERSIONED
    assert (
        "no readable metadata.dbt_schema_version" in errors(result, ParseErrorSeverity.WARNING)[0]
    )


def test_an_older_schema_parses_with_a_warning():
    result = DbtParser().parse(project_config("legacy-v11"))
    assert not result.has_errors
    assert result.schema_support is SchemaSupport.OLDER
    assert "v11 is older than v12" in errors(result, ParseErrorSeverity.WARNING)[0]
    assert len(result.nodes) == 8


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (lambda m: m.pop("parent_map"), "'parent_map' is missing"),
        (
            lambda m: m["parent_map"].update({"model.pilot_shop.customers": []}),
            "disagrees with 'parent_map' for 1 nodes",
        ),
        (
            lambda m: m.update(
                nodes={k: v for k, v in m["nodes"].items() if v["resource_type"] != "model"}
            ),
            "no models found",
        ),
    ],
)
def test_newer_schema_structural_checks(write_json, manifest_of, change, problem):
    manifest = manifest_of("pilot-1.12")
    manifest["metadata"]["dbt_schema_version"] = V.format(13)
    change(manifest)
    result = parse_manifest(write_json, manifest)
    assert result.has_errors
    assert any(problem in m for m in errors(result)), errors(result)


def test_supported_schema_skips_the_newer_schema_checks(manifest_of):
    manifest = manifest_of("pilot-1.12")
    manifest.pop("parent_map")
    assert structural_problems(manifest, SchemaSupport.SUPPORTED) == []
    assert structural_problems(manifest, SchemaSupport.NEWER) == ["'parent_map' is missing"]


@pytest.mark.parametrize(
    ("manifest", "problem"),
    [
        ([], "not a JSON object"),
        ({"metadata": {}}, "'nodes' is missing or not an object"),
        ({"metadata": [], "nodes": {}}, "'metadata' is missing or not an object"),
        ({"metadata": {}, "nodes": {}, "sources": []}, "'sources' is not an object"),
        ({"metadata": {}, "nodes": {}, "exposures": "x"}, "'exposures' is not an object"),
    ],
)
def test_basic_shape_is_checked_for_every_version(write_json, manifest, problem):
    result = parse_manifest(write_json, manifest)
    assert result.has_errors
    assert any(problem in m for m in errors(result)), errors(result)


# Files and configuration


def test_invalid_json_is_an_error(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text("{not json")
    result = DbtParser().parse({"manifest_path": str(path)})
    assert result.has_errors
    assert "Invalid JSON" in errors(result)[0]


def test_unreadable_manifest_is_an_error(tmp_path):
    path = tmp_path / "manifest.json"
    path.mkdir()  # exists, ends in .json, but cannot be read as a file
    result = DbtParser().parse({"manifest_path": str(path)})
    assert result.has_errors
    assert "Cannot read" in errors(result)[0]


def test_missing_manifest_is_an_error(tmp_path):
    result = DbtParser().parse({"manifest_path": str(tmp_path / "nonexistent.json")})
    assert result.has_errors
    assert "Manifest file not found" in errors(result)[0]


def test_missing_manifest_path_is_an_error():
    result = DbtParser().parse({})
    assert result.has_errors
    assert "Invalid configuration" in errors(result)[0]


def test_parse_accepts_a_config_object():
    config = DbtParserConfig(manifest_path=DBT_FIXTURES / "pilot-1.12" / "manifest.json")
    assert len(DbtParser().parse(config).nodes) == 21


def test_validate_config():
    parser = DbtParser()
    assert parser.validate_config({}) == ["manifest_path is required"]
    assert parser.validate_config(project_config("pilot-1.12")) == []
    assert "Invalid configuration" in parser.validate_config({"manifest_path": None})[0]
    assert parser.source_type == "dbt"


def test_unreadable_catalog_is_a_warning(write_json, tmp_path):
    catalog = tmp_path / "catalog.json"
    catalog.write_text("{not json")
    result = DbtParser().parse({**project_config("pilot-1.12"), "catalog_path": str(catalog)})
    assert not result.has_errors
    assert "Invalid JSON" in errors(result, ParseErrorSeverity.WARNING)[0]
    assert len(result.nodes) == 21


def test_catalog_that_is_not_an_object_is_a_warning(write_json):
    catalog = write_json([1, 2], name="catalog.json")
    result = DbtParser().parse({**project_config("pilot-1.12"), "catalog_path": str(catalog)})
    assert not result.has_errors
    assert "not a JSON object" in errors(result, ParseErrorSeverity.WARNING)[0]


# Hostile or malformed content costs one node at most


def test_a_malformed_node_costs_only_that_node(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    del manifest["nodes"]["model.pilot_shop.stg_customers"]["name"]
    manifest["nodes"]["model.pilot_shop.orders"]["columns"] = {"x": None, "y": [1]}
    manifest["nodes"]["model.pilot_shop.stg_orders"]["tags"] = "not-a-list"
    manifest["nodes"]["model.pilot_shop.stg_products"]["config"]["meta"] = {"layer": 3}
    manifest["sources"]["source.pilot_shop.ecom.raw_orders"] = ["not", "an", "object"]
    result = parse_manifest(write_json, manifest)

    assert not result.has_errors
    warnings = errors(result, ParseErrorSeverity.WARNING)
    assert len(warnings) == 2
    assert "model.pilot_shop.stg_customers" in warnings[0] and "'name'" in warnings[0]
    assert "source.pilot_shop.ecom.raw_orders" in warnings[1]
    ids = {n.unique_id for n in result.nodes}
    assert len(ids) == 19
    assert {"model.pilot_shop.orders", "model.pilot_shop.stg_orders"} <= ids
    # Edges into the two lost nodes are kept as unresolved, never dropped
    assert {(u.node_id, u.reference) for u in result.unresolved} == {
        ("model.pilot_shop.customers", "model.pilot_shop.stg_customers"),
        ("model.pilot_shop.stg_orders", "source.pilot_shop.ecom.raw_orders"),
    }


def test_a_malformed_exposure_costs_only_that_exposure(write_json, manifest_of):
    manifest = manifest_of("synth-24")
    (uid,) = manifest["exposures"]
    manifest["exposures"]["exposure.synthetic.broken"] = {"owner": "nobody"}
    result = parse_manifest(write_json, manifest)
    assert "exposure.synthetic.broken" in errors(result, ParseErrorSeverity.WARNING)[0]
    assert uid in {n.unique_id for n in result.nodes}


def test_unresolved_dependencies_are_recorded_with_a_reason(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    deps = manifest["nodes"]["model.pilot_shop.customers"]["depends_on"]["nodes"]
    deps += ["model.pilot_shop.ghost", "metric.pilot_shop.revenue", deps[0]]
    result = parse_manifest(write_json, manifest)
    unresolved = {(u.node_id, u.reference): u.reason for u in result.unresolved}
    assert unresolved == {
        ("model.pilot_shop.customers", "model.pilot_shop.ghost"): (
            "not found among the parsed nodes"
        ),
        ("model.pilot_shop.customers", "metric.pilot_shop.revenue"): (
            "metric nodes are not indexed"
        ),
    }
    # The duplicated dependency gives one edge
    upstream = [e for e in result.edges if e.downstream_id == "model.pilot_shop.customers"]
    assert len(upstream) == len({e.upstream_id for e in upstream}) == 2


@pytest.mark.parametrize(
    ("raw", "version"), [(2, "2"), (2.0, "2"), (2.5, "2.5"), ("3", "3"), (True, None), (None, None)]
)
def test_model_versions_are_normalised(write_json, manifest_of, raw, version):
    manifest = manifest_of("pilot-1.12")
    manifest["nodes"]["model.pilot_shop.customers"]["version"] = raw
    result = parse_manifest(write_json, manifest)
    customers = next(n for n in result.nodes if n.unique_id == "model.pilot_shop.customers")
    assert customers.version == version


def test_patch_path_without_a_project_prefix_is_kept(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    manifest["nodes"]["model.pilot_shop.customers"]["patch_path"] = "models/x.yml"
    result = parse_manifest(write_json, manifest)
    customers = next(n for n in result.nodes if n.unique_id == "model.pilot_shop.customers")
    assert customers.patch_path == "models/x.yml"


# Test attribution edge cases


def test_singular_tests_count_against_every_node_they_depend_on():
    tests = index_tests(
        {
            "test.p.check": {
                "resource_type": "test",
                "attached_node": None,
                "depends_on": {"nodes": ["model.p.a", "model.p.b"]},
                "column_name": "id",
            }
        }
    )
    assert tests.for_node("model.p.a") == ["singular"]
    assert tests.for_node("model.p.b") == ["singular"]
    # A column cannot be attributed when the test has several subjects
    assert tests.column_tests == {}


def test_a_source_relationships_test_attaches_to_the_source_only():
    tests = index_tests(
        {
            "test.p.rel": {
                "resource_type": "test",
                "attached_node": None,
                "test_metadata": {"name": "relationships"},
                "depends_on": {"nodes": ["model.p.customers", "source.p.raw.orders"]},
                "column_name": "customer_id",
            },
            "model.p.customers": {"resource_type": "model"},
            "not-a-dict": [],
        }
    )
    assert tests.for_node("source.p.raw.orders") == ["relationships"]
    assert tests.for_node("model.p.customers") == []
    assert tests.for_column("source.p.raw.orders", "Customer_ID") == ["relationships"]
