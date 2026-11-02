"""DbtParser on real dbt artifacts (dbt Core 1.12, Fusion 2.0.9, and a synthetic project).

Expected values are computed straight from the manifest JSON, so these tests check the
parser against the artifact, not against another reading of the parser. Each S2 finding
fixed in task 1.2 has a test here.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import pytest
from support import REAL_PROJECTS, project_config, raw_manifest

from lineagepilot_core.parsers import DbtParser, ParseResult, SchemaSupport

INDEXED_NODE_TYPES = {"model", "seed", "snapshot"}


def parse(name: str, catalog: bool = True) -> ParseResult:
    return DbtParser().parse(project_config(name, catalog))


@pytest.fixture(scope="module", params=REAL_PROJECTS)
def project(request: pytest.FixtureRequest) -> tuple[str, dict[str, Any], ParseResult]:
    name = request.param
    return name, raw_manifest(name), parse(name)


def expected_node_ids(manifest: dict[str, Any]) -> set[str]:
    return (
        {uid for uid, n in manifest["nodes"].items() if n["resource_type"] in INDEXED_NODE_TYPES}
        | set(manifest["sources"])
        | set(manifest["exposures"])
    )


def test_parses_cleanly_as_a_supported_schema(project):
    name, manifest, result = project
    assert result.errors == []
    assert result.schema_version == 12
    assert result.schema_support is SchemaSupport.SUPPORTED
    assert result.dbt_version == manifest["metadata"]["dbt_version"]
    assert result.generated_at == manifest["metadata"]["generated_at"]
    assert result.adapter_type == "duckdb"
    assert result.project_name == manifest["metadata"]["project_name"]


def test_every_node_source_and_exposure_is_present_once(project):
    _, manifest, result = project
    ids = [n.unique_id for n in result.nodes]
    assert len(ids) == len(set(ids))
    assert set(ids) == expected_node_ids(manifest)


def test_edges_match_depends_on_exactly(project):
    _, manifest, result = project
    indexed = expected_node_ids(manifest)
    expected = set()
    for uid in indexed:
        entry = manifest["nodes"].get(uid) or manifest["exposures"].get(uid) or {}
        expected |= {(dep, uid) for dep in entry.get("depends_on", {}).get("nodes", [])}
    assert {(e.upstream_id, e.downstream_id) for e in result.edges} == expected
    assert len(result.edges) == len(expected)
    assert result.unresolved == []


def test_edges_agree_with_parent_map(project):
    _, manifest, result = project
    parents: dict[str, set[str]] = {}
    for edge in result.edges:
        parents.setdefault(edge.downstream_id, set()).add(edge.upstream_id)
    for uid, expected in manifest["parent_map"].items():
        if uid.startswith(("model.", "seed.", "snapshot.", "exposure.")):
            assert parents.get(uid, set()) == set(expected), uid


def test_node_tests_come_from_test_nodes(project):
    """S2 suspicion 2: has_tests was False on every real manifest."""
    _, manifest, result = project
    expected: Counter[str] = Counter()
    for node in manifest["nodes"].values():
        if node["resource_type"] != "test":
            continue
        if node.get("attached_node"):
            expected[node["attached_node"]] += 1
        else:  # tests on sources
            (source,) = [d for d in node["depends_on"]["nodes"] if d.startswith("source.")]
            expected[source] += 1
    for node in result.nodes:
        assert node.test_count == expected[node.unique_id], node.unique_id
        assert node.has_tests is (expected[node.unique_id] > 0)
    assert any(n.has_tests for n in result.nodes)


def test_column_tests_come_from_test_nodes(project):
    _, manifest, result = project
    expected: Counter[tuple[str, str]] = Counter()
    for node in manifest["nodes"].values():
        if node["resource_type"] == "test" and node.get("column_name"):
            subject = node.get("attached_node") or next(
                d for d in node["depends_on"]["nodes"] if d.startswith("source.")
            )
            expected[(subject, node["column_name"].lower())] += 1
    tested = {(c.node_id, c.name.lower()): c.test_count for c in result.columns if c.has_tests}
    assert tested == dict(expected)


def test_jaffle_shop_tested_model_count_matches_s2():
    """S2: 9 of the 13 jaffle-shop models have tests."""
    for name in ("jaffle-1.12", "jaffle-fusion-2.0"):
        models = [n for n in parse(name).nodes if n.resource_type == "model"]
        assert (len(models), sum(m.has_tests for m in models)) == (13, 9), name


def test_test_types_name_the_generic_tests():
    customers = next(n for n in parse("jaffle-1.12").nodes if n.name == "customers")
    assert customers.test_types == ["accepted_values", "expression_is_true", "not_null", "unique"]
    customer_id = next(
        c
        for c in parse("jaffle-1.12").columns
        if c.node_id == customers.unique_id and c.name == "customer_id"
    )
    assert customer_id.test_types == ["not_null", "unique"]


def test_source_tests_attach_through_depends_on():
    """Generic tests on sources have no attached_node (S2, field differences)."""
    result = parse("synth-24")
    source = next(n for n in result.nodes if n.unique_id == "source.synthetic.app.devices_1")
    assert source.test_types == ["not_null", "unique"]
    column = next(c for c in result.columns if c.node_id == source.unique_id and c.name == "id")
    assert column.test_types == ["not_null", "unique"]
    assert sum(n.has_tests for n in result.nodes if n.resource_type == "source") == 4


def test_versioned_models_do_not_collide():
    """S2 finding 4d: both versions shared one URN, and one edge was dropped."""
    result = parse("synth-24")
    base = "model.synthetic.fct_inventory__fct_claims_1"
    versions = {n.unique_id: n for n in result.nodes if n.unique_id.startswith(base)}
    assert set(versions) == {f"{base}.v1", f"{base}.v2"}
    assert {n.version for n in versions.values()} == {"1", "2"}
    assert {n.name for n in versions.values()} == {"fct_inventory__fct_claims_1"}
    upstream = "model.synthetic.int_sales__refunds_enriched_1"
    edges = {(e.upstream_id, e.downstream_id) for e in result.edges}
    assert {(upstream, f"{base}.v1"), (upstream, f"{base}.v2")} <= edges
    assert {c.node_id for c in result.columns if c.node_id.startswith(base)} == set(versions)


def test_exposures_are_nodes_with_owner_and_edges():
    """S2 finding 4e: exposures were not parsed at all."""
    manifest = raw_manifest("synth-24")
    ((uid, raw),) = manifest["exposures"].items()
    result = parse("synth-24")
    exposure = next(n for n in result.nodes if n.unique_id == uid)
    assert exposure.resource_type == "exposure"
    assert exposure.owner == raw["owner"]["name"]
    assert exposure.properties["owner_email"] == raw["owner"]["email"]
    assert exposure.properties["type"] == raw["type"]
    assert exposure.original_file_path == raw["original_file_path"]
    assert exposure.depends_on == raw["depends_on"]["nodes"]
    assert {e.upstream_id for e in result.edges if e.downstream_id == uid} == set(
        raw["depends_on"]["nodes"]
    )


def test_file_paths_are_kept(project):
    """S2: sources lost original_file_path, and models lost patch_path."""
    _, manifest, result = project
    by_id = {n.unique_id: n for n in result.nodes}
    for uid, source in manifest["sources"].items():
        assert by_id[uid].original_file_path == source["original_file_path"]
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] in INDEXED_NODE_TYPES:
            assert by_id[uid].original_file_path == node["original_file_path"]
            if node.get("patch_path"):
                project_prefix, path = node["patch_path"].split("://", 1)
                assert by_id[uid].patch_path == path
                assert project_prefix == node["package_name"]
            else:
                assert by_id[uid].patch_path is None


def test_relation_names_are_kept(project):
    _, manifest, result = project
    by_id = {n.unique_id: n for n in result.nodes}
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] in INDEXED_NODE_TYPES:
            assert by_id[uid].relation_name == node.get("relation_name")
    for uid, source in manifest["sources"].items():
        assert by_id[uid].relation_name == source["relation_name"]


def test_owner_and_group_come_from_the_manifest(project):
    _, manifest, result = project
    by_id = {n.unique_id: n for n in result.nodes}
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] in INDEXED_NODE_TYPES:
            assert by_id[uid].owner == node["config"].get("meta", {}).get("owner"), uid
            assert by_id[uid].group == (node.get("group") or node["config"].get("group"))


def test_catalog_supplies_column_types():
    with_catalog = parse("jaffle-1.12")
    without = parse("jaffle-1.12", catalog=False)
    typed = [c for c in with_catalog.columns if c.data_type]
    assert typed and len(typed) == len(with_catalog.columns)
    assert len(with_catalog.columns) > len(without.columns)  # undocumented columns


def test_fusion_and_core_agree_on_the_graph():
    """Same project, two engines: same nodes, edges and tests (S2, Fusion outcome)."""
    core, fusion = parse("jaffle-1.12", catalog=False), parse("jaffle-fusion-2.0")

    def shape(r: ParseResult) -> tuple[Any, ...]:
        return (
            sorted((n.unique_id, n.test_count) for n in r.nodes),
            sorted((e.upstream_id, e.downstream_id) for e in r.edges),
        )

    assert shape(core) == shape(fusion)
    # Fusion omits column data_type (S2); with no catalog the type is simply unknown
    assert all(c.data_type is None for c in fusion.columns)


def test_parsing_writes_nothing_to_stdout(capfd):
    """stdout is the language server's JSON-RPC channel (S2 finding 4b)."""
    for name in REAL_PROJECTS:
        parse(name)
    out, _ = capfd.readouterr()
    assert out == ""
