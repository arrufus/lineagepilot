"""Building the graph from real dbt artifacts."""

from __future__ import annotations

import hashlib
import json

import pytest
from support import DBT_FIXTURES, REAL_PROJECTS, project_config, raw_manifest

from lineagepilot_core.graph import (
    Confidence,
    EdgeKind,
    MetadataGraph,
    NodeKind,
    Origin,
    UntrustedParseResult,
    build_dbt_graph,
)
from lineagepilot_core.parsers import DbtParser


def build(name: str, catalog: bool = True) -> MetadataGraph:
    return build_dbt_graph(DbtParser().parse(project_config(name, catalog)))


@pytest.fixture(scope="module", params=REAL_PROJECTS)
def project(request):
    name = request.param
    result = DbtParser().parse(project_config(name))
    return name, result, build_dbt_graph(result)


def test_every_parsed_node_and_column_is_in_the_graph(project):
    _, result, graph = project
    dbt_nodes = {n.id for n in graph.nodes.values() if n.kind is not NodeKind.RELATION}
    assert dbt_nodes == {n.unique_id for n in result.nodes}
    assert len(graph.columns) == len(result.columns)
    for column in graph.columns.values():
        assert column.id == f"{column.node_id}#{column.name.lower()}"  # duckdb folds to lower


def test_depends_on_edges_match_the_parse(project):
    _, result, graph = project
    edges = {(e.upstream, e.downstream) for e in graph.edges if e.kind is EdgeKind.DEPENDS_ON}
    assert edges == {(e.upstream_id, e.downstream_id) for e in result.edges}


def test_every_relation_is_materialised_by_its_node(project):
    """Logical-to-physical mappings are explicit edges (design section 5.1)."""
    _, result, graph = project
    with_relation = [n for n in result.nodes if n.relation_name]
    assert with_relation
    for raw in with_relation:
        node = graph.node(raw.unique_id)
        assert node.relation_id and node.relation_id.startswith("relation:")
        assert graph.node(node.relation_id).kind is NodeKind.RELATION
        (edge,) = graph.out_edges(node.id, [EdgeKind.MATERIALISES])
        assert edge.downstream == node.relation_id
    exposures = [n for n in graph.nodes.values() if n.kind is NodeKind.EXPOSURE]
    assert all(n.relation_id is None for n in exposures)


def test_manifest_evidence_names_the_artifact_and_the_file(project):
    name, result, graph = project
    digest = hashlib.sha256((DBT_FIXTURES / name / "manifest.json").read_bytes()).hexdigest()
    metadata = raw_manifest(name)["metadata"]
    for edge in graph.edges:
        (evidence,) = edge.evidence
        assert (evidence.origin, evidence.confidence) == (Origin.MANIFEST, Confidence.DECLARED)
        assert evidence.artifact.sha256 == digest
        assert evidence.artifact.generated_at == metadata["generated_at"]
        assert evidence.artifact.dbt_version == metadata["dbt_version"]
        source = graph.node(edge.downstream if edge.kind is EdgeKind.DEPENDS_ON else edge.upstream)
        assert evidence.location.path == source.file_path


def test_parse_records_artifact_hashes():
    result = DbtParser().parse(project_config("pilot-1.12"))
    for path, digest in (
        (result.manifest_path, result.manifest_sha256),
        (result.catalog_path, result.catalog_sha256),
    ):
        with open(path, "rb") as f:
            assert digest == hashlib.sha256(f.read()).hexdigest()


def test_versioned_models_materialise_different_relations():
    graph = build("synth-24")
    base = "model.synthetic.fct_inventory__fct_claims_1"
    v1, v2 = graph.node(f"{base}.v1"), graph.node(f"{base}.v2")
    assert v1.relation_id == "relation:fixture.main.fct_inventory__fct_claims_1_v1"
    assert v2.relation_id == "relation:fixture.main.fct_inventory__fct_claims_1_v2"


def test_a_source_and_a_model_on_one_table_reconcile(write_json, manifest_of):
    """Review finding F09: two logical nodes on one physical table meet at one relation."""
    manifest = manifest_of("pilot-1.12")
    source = manifest["sources"]["source.pilot_shop.ecom.raw_orders"]
    source["relation_name"] = manifest["nodes"]["model.pilot_shop.orders"]["relation_name"].upper()
    result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
    graph = build_dbt_graph(result)
    relation = graph.node("model.pilot_shop.orders").relation_id
    assert {n.id for n in graph.logical_nodes_for(relation)} == {
        "model.pilot_shop.orders",
        "source.pilot_shop.ecom.raw_orders",
    }


def test_snowflake_folds_column_names_to_upper(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    manifest["metadata"]["adapter_type"] = "snowflake"
    graph = build_dbt_graph(DbtParser().parse({"manifest_path": str(write_json(manifest))}))
    ids = {c.id for c in graph.columns_of("model.pilot_shop.customers")}
    assert "model.pilot_shop.customers#CUSTOMER_ID" in ids
    assert graph.node("model.pilot_shop.customers").relation_id == "relation:pilot.main.customers"
    assert graph.adapter_type == "snowflake"


def test_unresolved_records_carry_evidence(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    manifest["nodes"]["model.pilot_shop.customers"]["depends_on"]["nodes"].append(
        "metric.pilot_shop.revenue"
    )
    graph = build_dbt_graph(DbtParser().parse({"manifest_path": str(write_json(manifest))}))
    (record,) = graph.unresolved_for("model.pilot_shop.customers")
    assert record.reference == "metric.pilot_shop.revenue"
    assert record.reason == "metric nodes are not indexed"
    assert record.evidence.location.path == "models/marts/customers.sql"


def test_unresolved_record_for_a_lost_node_has_no_location():
    from lineagepilot_core.parsers.base import ParseResult, RawUnresolved

    result = ParseResult(adapter_type="duckdb")
    result.unresolved.append(RawUnresolved("model.p.gone", "model.p.x", "not found"))
    (record,) = build_dbt_graph(result).unresolved
    assert record.evidence.location is None


def test_columns_that_collide_under_case_rules_are_unresolved():
    from lineagepilot_core.parsers.base import ParseResult, RawColumn, RawNode

    result = ParseResult(adapter_type="snowflake")
    result.nodes.append(RawNode("model.p.a", "model", "a", original_file_path="models/a.sql"))
    result.columns += [RawColumn("model.p.a", "id"), RawColumn("model.p.a", "ID")]
    graph = build_dbt_graph(result)
    assert [c.name for c in graph.columns_of("model.p.a")] == ["id"]
    (record,) = graph.unresolved
    assert record.reference == "ID" and "collides" in record.reason


def test_files_map_to_their_nodes():
    graph = build("pilot-1.12")
    assert [n.id for n in graph.nodes_for_file("models/marts/customers.sql")] == [
        "model.pilot_shop.customers"
    ]
    documented = {n.id for n in graph.nodes_for_file("models/marts/customers.yml")}
    assert "model.pilot_shop.customers" in documented
    assert {n.kind for n in graph.nodes_for_file("models/staging/__sources.yml")} == {
        NodeKind.SOURCE
    }


def test_an_untrusted_parse_is_refused(write_json):
    result = DbtParser().parse({"manifest_path": str(write_json({"nodes": []}))})
    with pytest.raises(UntrustedParseResult, match="could not be trusted"):
        build_dbt_graph(result)


def test_stats_on_synth():
    stats = build("synth-24").stats()
    assert stats["node_kinds"]["exposure"] == 1
    assert stats["edge_kinds"]["depends_on"] == 45
    assert stats["edge_kinds"]["materialises"] == 34  # every node except the exposure


def test_catalog_presence_is_recorded():
    graph = build("pilot-1.12")
    catalog = json.loads((DBT_FIXTURES / "pilot-1.12" / "catalog.json").read_bytes())
    catalogued = set(catalog["nodes"]) | set(catalog["sources"])
    for node in graph.nodes.values():
        if node.kind is not NodeKind.RELATION:
            assert node.in_catalog is (node.id in catalogued), node.id
    assert all(c.in_catalog for c in graph.columns.values())
    assert not any(c.in_catalog for c in build("pilot-1.12", catalog=False).columns.values())
    customers = graph.node("model.pilot_shop.customers")
    assert (customers.language, customers.compiled_path) == (
        "sql",
        "target/compiled/pilot_shop/models/marts/customers.sql",
    )
