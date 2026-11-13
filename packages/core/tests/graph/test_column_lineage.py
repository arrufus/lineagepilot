"""Column lineage in the graph, on real dbt artifacts."""

from __future__ import annotations

import hashlib
from collections import Counter

import pytest
from support import project_config, raw_manifest

from lineagepilot_core.graph import EdgeKind, MetadataGraph, Origin, build_dbt_graph
from lineagepilot_core.graph.column_lineage import (
    add_column_lineage,
    catalog_schema,
    compiled_sql,
)
from lineagepilot_core.graph.model import Confidence, LineageState
from lineagepilot_core.parsers import DbtParser
from lineagepilot_core.parsers.base import ParseResult
from lineagepilot_core.parsers.sql_lineage import build_schema

PILOT = "model.pilot_shop"


def graph_of(name: str, catalog: bool = True) -> tuple[ParseResult, MetadataGraph]:
    result = DbtParser().parse(project_config(name, catalog))
    return result, build_dbt_graph(result)


@pytest.fixture
def pilot():
    result, graph = graph_of("pilot-1.12")
    return result, graph, compiled_sql(result)


def derived(graph: MetadataGraph, column: str) -> dict[str, str]:
    return {e.upstream: e.transformation for e in graph.in_edges(column, [EdgeKind.DERIVES])}


def test_every_pilot_model_resolves(pilot):
    _, graph, sql = pilot
    statuses = add_column_lineage(graph, sql)
    assert len(statuses) == 11
    assert {s.state for s in statuses.values()} == {LineageState.RESOLVED}
    assert graph.column_lineage == statuses
    assert graph.unresolved == []


def test_pilot_customers_columns_trace_to_their_sources(pilot):
    """Checked by hand against models/marts/customers.sql."""
    _, graph, sql = pilot
    add_column_lineage(graph, sql)
    assert derived(graph, f"{PILOT}.customers#customer_id") == {
        f"{PILOT}.stg_customers#customer_id": "passthrough"
    }
    assert derived(graph, f"{PILOT}.customers#lifetime_spend") == {
        f"{PILOT}.orders#order_total": "aggregate"
    }
    # case when count(distinct order_id) > 1 then 'returning' else 'new' end
    assert derived(graph, f"{PILOT}.customers#customer_type") == {
        f"{PILOT}.orders#order_id": "aggregate"
    }


def test_staging_columns_reach_sources_through_renames(pilot):
    _, graph, sql = pilot
    add_column_lineage(graph, sql)
    # stg_customers: id as customer_id, name as customer_name
    assert derived(graph, f"{PILOT}.stg_customers#customer_id") == {
        "source.pilot_shop.ecom.raw_customers#id": "passthrough"
    }
    assert derived(graph, f"{PILOT}.stg_customers#customer_name") == {
        "source.pilot_shop.ecom.raw_customers#name": "passthrough"
    }


def test_derives_evidence_is_inferred_from_compiled_sql(pilot):
    _, graph, sql = pilot
    add_column_lineage(graph, sql)
    metadata = raw_manifest("pilot-1.12")["metadata"]
    edges = [e for e in graph.edges if e.kind is EdgeKind.DERIVES]
    assert len(edges) == 59
    for edge in edges:
        (evidence,) = edge.evidence
        node = graph.node(edge.downstream.split("#")[0])
        assert (evidence.origin, evidence.confidence) == (Origin.COMPILED_SQL, Confidence.INFERRED)
        assert evidence.artifact.path == node.compiled_path
        assert evidence.artifact.sha256 == hashlib.sha256(sql[node.id].encode()).hexdigest()
        assert evidence.artifact.generated_at == metadata["generated_at"]
        assert evidence.location.path == node.file_path


def test_no_catalog_means_unknown_never_no_lineage():
    result, graph = graph_of("pilot-fusion-2.0")
    statuses = add_column_lineage(graph, compiled_sql(result))
    assert {(s.state, s.reason) for s in statuses.values()} == {
        (LineageState.UNKNOWN, "no catalog.json; run dbt docs generate")
    }
    assert not [e for e in graph.edges if e.kind is EdgeKind.DERIVES]
    assert Counter(u.reference for u in graph.unresolved) == {"column lineage": 11}


def test_missing_sql_and_python_models_are_unknown(pilot):
    _, graph, sql = pilot
    del sql[f"{PILOT}.orders"]
    graph.node(f"{PILOT}.customers").language = "python"
    statuses = add_column_lineage(graph, sql, [f"{PILOT}.orders", f"{PILOT}.customers"])
    assert statuses[f"{PILOT}.orders"].reason == (
        "no compiled SQL; run dbt compile or dbt docs generate"
    )
    assert statuses[f"{PILOT}.customers"].reason == "Python models are not analysed"
    unknown = {u.subject: u for u in graph.unresolved}
    assert unknown[f"{PILOT}.orders"].evidence.artifact.sha256 is None


def test_an_unparseable_model_is_unknown_with_its_reason(pilot):
    _, graph, sql = pilot
    sql[f"{PILOT}.orders"] = "select from where ("
    status = add_column_lineage(graph, sql, [f"{PILOT}.orders"])[f"{PILOT}.orders"]
    assert status.state is LineageState.UNKNOWN
    assert status.reason.startswith("SQL could not be parsed")
    (record,) = graph.unresolved
    assert (record.subject, record.reference) == (f"{PILOT}.orders", "column lineage")


def test_recomputing_replaces_the_previous_result(pilot):
    _, graph, sql = pilot
    add_column_lineage(graph, sql)
    before = sorted((e.upstream, e.downstream) for e in graph.edges if e.kind is EdgeKind.DERIVES)
    add_column_lineage(graph, sql)
    after = sorted((e.upstream, e.downstream) for e in graph.edges if e.kind is EdgeKind.DERIVES)
    assert before == after

    customers = f"{PILOT}.customers"
    sql[customers] = 'select customer_id from "pilot"."main"."stg_customers"'
    add_column_lineage(graph, sql, [customers])
    into_customers = [
        e for e in graph.edges if e.kind is EdgeKind.DERIVES and e.downstream.startswith(customers)
    ]
    assert [(e.upstream, e.downstream) for e in into_customers] == [
        (f"{PILOT}.stg_customers#customer_id", f"{customers}#customer_id")
    ]
    sql[customers] = "nonsense"
    add_column_lineage(graph, sql, [customers])
    add_column_lineage(graph, sql, [customers])
    assert [u.subject for u in graph.unresolved] == [customers]


def test_a_column_the_catalog_lacks_makes_the_model_unknown(pilot):
    """sqlglot refuses an unknown column outright: the catalog is stale."""
    _, graph, sql = pilot
    customers = f"{PILOT}.customers"
    sql[customers] = 'select o.zz as customer_id from "pilot"."main"."orders" o'
    status = add_column_lineage(graph, sql, [customers])[customers]
    assert status.state is LineageState.UNKNOWN
    assert status.reason == "column lineage failed: OptimizeError: Unknown column: zz"


def test_unresolved_references_make_a_model_partial(pilot):
    _, graph, sql = pilot
    customers = f"{PILOT}.customers"
    # A schema that knows a column the graph does not, as when the catalog is newer
    schema = build_schema(
        [((("pilot", True), ("main", True), ("orders", True)), ["order_id", "extra"])], "duckdb"
    )
    sql[customers] = (
        'select o.extra as customer_id, o.order_id as not_a_column from "pilot"."main"."orders" o'
    )
    status = add_column_lineage(graph, sql, [customers], schema=schema)[customers]
    assert status.state is LineageState.PARTIAL
    assert status.reason == "2 column references did not resolve"
    reasons = {u.reference: (u.subject, u.reason) for u in graph.unresolved}
    assert reasons["pilot.main.orders.extra"] == (
        f"{customers}#customer_id",
        f"column 'extra' is not in {PILOT}.orders",
    )
    assert reasons["not_a_column"][0] == customers
    assert "not in the catalog or the docs" in reasons["not_a_column"][1]


def test_a_table_outside_the_project_is_unresolved(pilot):
    _, graph, sql = pilot
    schema = build_schema(
        [((("pilot", True), ("main", True), ("external", True)), ["customer_id"])], "duckdb"
    )
    customers = f"{PILOT}.customers"
    sql[customers] = 'select customer_id from "pilot"."main"."external"'
    add_column_lineage(graph, sql, [customers], schema=schema)
    (record,) = graph.unresolved
    assert record.reason == "the table is not part of the dbt project"
    assert record.subject == f"{customers}#customer_id"


def test_two_nodes_on_one_table_prefer_the_declared_upstream(write_json, manifest_of):
    manifest = manifest_of("pilot-1.12")
    source = manifest["sources"]["source.pilot_shop.ecom.raw_orders"]
    orders = manifest["nodes"][f"{PILOT}.orders"]
    source["relation_name"] = orders["relation_name"]
    catalog = write_json(raw_catalog_with(source_id="source.pilot_shop.ecom.raw_orders"), "c.json")
    result = DbtParser().parse(
        {"manifest_path": str(write_json(manifest)), "catalog_path": str(catalog)}
    )
    graph = build_dbt_graph(result)
    sql = compiled_sql(result)
    # customers declares a dependency on the orders model, so its columns win
    add_column_lineage(graph, sql, [f"{PILOT}.customers"])
    assert derived(graph, f"{PILOT}.customers#lifetime_spend") == {
        f"{PILOT}.orders#order_total": "aggregate"
    }
    # A model that declares neither cannot choose
    sql[f"{PILOT}.locations"] = f"select order_id as location_id from {orders['relation_name']}"
    add_column_lineage(graph, sql, [f"{PILOT}.locations"])
    (record,) = graph.unresolved_for(f"{PILOT}.locations#location_id")
    assert record.reason.startswith("the table is materialised by several nodes")


def raw_catalog_with(source_id: str) -> dict:
    """The pilot catalog, with the orders model's entry also given to a source."""
    import json

    from support import DBT_FIXTURES

    catalog = json.loads((DBT_FIXTURES / "pilot-1.12" / "catalog.json").read_bytes())
    catalog["sources"][source_id] = catalog["nodes"][f"{PILOT}.orders"]
    return catalog


def test_an_incremental_model_reading_itself_gets_no_self_edge(pilot):
    _, graph, sql = pilot
    orders = f"{PILOT}.orders"
    sql[orders] = 'select order_id from "pilot"."main"."orders"'
    assert add_column_lineage(graph, sql, [orders])[orders].state is LineageState.RESOLVED
    assert graph.in_edges(f"{orders}#order_id", [EdgeKind.DERIVES]) == []


def test_only_models_and_snapshots(pilot):
    _, graph, sql = pilot
    with pytest.raises(ValueError, match="only models and snapshots"):
        add_column_lineage(graph, sql, ["source.pilot_shop.ecom.raw_orders"])


def test_large_sql_is_unknown(pilot):
    _, graph, sql = pilot
    status = add_column_lineage(graph, sql, [f"{PILOT}.orders"], max_chars=10)
    assert "over the 10 limit" in status[f"{PILOT}.orders"].reason


def test_synthetic_project_is_resolved_except_the_uncatalogued_snapshot():
    result, graph = graph_of("synth-24")
    statuses = add_column_lineage(graph, compiled_sql(result))
    states = Counter(s.state for s in statuses.values())
    assert states == {LineageState.RESOLVED: 24, LineageState.PARTIAL: 1}
    (partial,) = [k for k, v in statuses.items() if v.state is LineageState.PARTIAL]
    assert partial == "snapshot.synthetic.snap_first_staging"
    assert not graph.node(partial).in_catalog
    # The two versions of a versioned model have their own column lineage
    v1, v2 = (f"model.synthetic.fct_inventory__fct_claims_1.v{n}#fct_claims_1_id" for n in (1, 2))
    assert derived(graph, v1) and derived(graph, v2)


def test_catalog_schema_covers_catalogued_relations_once():
    _, graph = graph_of("pilot-1.12")
    schema = catalog_schema(graph)
    tables = [
        (db, s, t)
        for db, schemas in schema.mapping.items()
        for s, ts in schemas.items()
        for t in ts
    ]
    assert len(tables) == 16  # 11 models and 5 seeds; the sources share the seeds' tables
    assert not catalog_schema(graph_of("pilot-fusion-2.0")[1]).mapping


def test_a_schema_of_direct_upstreams_gives_one_model_the_same_lineage(pilot):
    _, full, sql = pilot
    add_column_lineage(full, sql)
    _, graph = graph_of("pilot-1.12")
    model = f"{PILOT}.customers"
    upstream = [e.upstream for e in graph.in_edges(model, [EdgeKind.DEPENDS_ON])]
    schema = catalog_schema(graph, upstream)
    assert sorted(t for s in schema.mapping.values() for ts in s.values() for t in ts) == [
        "orders",
        "stg_customers",
    ]
    add_column_lineage(graph, sql, [model], schema=schema)
    for column in full.columns_of(model):
        assert derived(graph, column.id) == derived(full, column.id)
    assert graph.column_lineage[model] == full.column_lineage[model]


class TestCompiledSql:
    def test_from_the_manifest(self):
        result, _ = graph_of("pilot-1.12")
        sql = compiled_sql(result)
        assert len(sql) == 11
        assert sql[f"{PILOT}.customers"].lstrip().startswith("with")

    def test_from_target_compiled(self, tmp_path):
        result, _ = graph_of("pilot-1.12")
        node = next(n for n in result.nodes if n.unique_id == f"{PILOT}.customers")
        node.compiled_code = None
        compiled = tmp_path / node.compiled_path
        compiled.parent.mkdir(parents=True)
        compiled.write_text("select 1 as x", encoding="utf-8")
        sql = compiled_sql(result, project_root=tmp_path)
        assert sql[f"{PILOT}.customers"] == "select 1 as x"
        # Without a project root there is nowhere to look
        assert f"{PILOT}.customers" not in compiled_sql(result)

    def test_paths_outside_the_project_are_ignored(self, tmp_path):
        result, _ = graph_of("pilot-1.12")
        secret = tmp_path / "secret.sql"
        secret.write_text("select 'stolen' as x")
        root = tmp_path / "project"
        root.mkdir()
        node = next(n for n in result.nodes if n.unique_id == f"{PILOT}.customers")
        node.compiled_code, node.compiled_path = None, "../secret.sql"
        assert f"{PILOT}.customers" not in compiled_sql(result, project_root=root)

    def test_unreadable_and_missing_files_are_skipped(self, tmp_path):
        result, _ = graph_of("pilot-1.12")
        a, b = (n for n in result.nodes if n.unique_id in (f"{PILOT}.customers", f"{PILOT}.orders"))
        a.compiled_code = b.compiled_code = None
        bad = tmp_path / a.compiled_path
        bad.parent.mkdir(parents=True)
        bad.write_bytes(b"\xff\xfe not utf-8")
        sql = compiled_sql(result, project_root=tmp_path)
        assert a.unique_id not in sql and b.unique_id not in sql

    def test_python_models_are_skipped(self):
        result, _ = graph_of("pilot-1.12")
        next(n for n in result.nodes if n.unique_id == f"{PILOT}.orders").language = "python"
        assert f"{PILOT}.orders" not in compiled_sql(result)
