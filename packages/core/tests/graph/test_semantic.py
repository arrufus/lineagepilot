"""The dbt semantic layer in the graph (task 7.8): semantic models, metrics and saved queries as
consumers, their fields as columns, and what each reads, on fixtures/impact-shop as real dbt wrote
it, in both forms of its YAML."""

from __future__ import annotations

import copy
import json
from functools import cache
from typing import Any

import pytest
from support import IMPACT_SHOP, SEMANTIC_V2

from lineagepilot_core.analysis import Granularity, impact
from lineagepilot_core.analysis.traversal import Direction, steps, traverse
from lineagepilot_core.export import graph_view
from lineagepilot_core.graph.build import build_dbt_graph
from lineagepilot_core.graph.model import (
    Confidence,
    EdgeKind,
    LineageState,
    MetadataGraph,
    NodeKind,
)
from lineagepilot_core.graph.semantic import MAX_EXPR_CHARS
from lineagepilot_core.parsers import DbtParser
from lineagepilot_core.parsers.dbt_semantic import MAX_REFERENCES
from lineagepilot_core.snapshot import from_dict, to_dict

MODEL = "model.impact_shop.orders"
SEMANTIC = "semantic_model.impact_shop.orders"
METRIC = "metric.impact_shop"
QUERY = "saved_query.impact_shop.weekly_revenue"
CATALOG = IMPACT_SHOP / "target" / "catalog.json"


@cache
def _manifest() -> dict[str, Any]:
    return json.loads((IMPACT_SHOP / "target" / "manifest.json").read_bytes())


@pytest.fixture
def manifest() -> dict[str, Any]:
    """A fresh, mutable copy of impact-shop's manifest."""
    return copy.deepcopy(_manifest())


@pytest.fixture
def build(write_json):
    def build(manifest: dict[str, Any] | None = None, catalog: bool = True) -> MetadataGraph:
        path = (
            write_json(manifest)
            if manifest is not None
            else IMPACT_SHOP / "target" / "manifest.json"
        )
        config = {"manifest_path": str(path)}
        if catalog:
            config["catalog_path"] = str(CATALOG)
        result = DbtParser().parse(config)
        assert not result.has_errors, result.errors
        return build_dbt_graph(result)

    return build


@pytest.fixture
def shop(build) -> MetadataGraph:
    return build()


def reads(graph: MetadataGraph, ref: str) -> dict[str, str]:
    """What a node or column consumes, with the confidence of each."""
    return {e.downstream: e.confidence.value for e in graph.out_edges(ref, (EdgeKind.CONSUMES,))}


def semantic_model(manifest: dict[str, Any]) -> dict[str, Any]:
    return manifest["semantic_models"][SEMANTIC]


def field(manifest: dict[str, Any], role: str, name: str) -> dict[str, Any]:
    return next(f for f in semantic_model(manifest)[role] if f["name"] == name)


def unknowns(graph: MetadataGraph) -> dict[tuple[str, str], str]:
    return {(u.subject, u.reference): u.reason for u in graph.unresolved}


# Nodes and fields


def test_the_semantic_layer_is_consumers_with_dbt_s_ids(shop):
    semantic = {n.id: n for n in shop.nodes.values() if n.tool == "dbt_semantic"}
    assert {(n.id, n.subkind) for n in semantic.values()} == {
        (SEMANTIC, "semantic_model"),
        (f"{METRIC}.revenue", "metric"),
        (f"{METRIC}.order_count", "metric"),
        (f"{METRIC}.large_orders", "metric"),
        (f"{METRIC}.average_order_value", "metric"),
        (QUERY, "saved_query"),
    }
    assert {n.kind for n in semantic.values()} == {NodeKind.CONSUMER}
    assert {n.file_path for n in semantic.values()} == {"models/marts/semantic.yml"}
    # Owners: meta.owner, or the dbt group
    assert semantic[SEMANTIC].owner == "finance-data"
    assert semantic[f"{METRIC}.revenue"].owner == "finance-analytics"
    assert (semantic[QUERY].owner, semantic[QUERY].group) == (None, "operations")
    assert semantic[f"{METRIC}.revenue"].properties == {"label": "Revenue", "type": "simple"}
    # A changed semantic.yml finds them
    assert {n.id for n in shop.nodes_for_file("models/marts/semantic.yml")} == set(semantic)


def test_a_semantic_model_s_fields_are_its_columns(shop):
    fields = {c.name: (c.semantic_type, c.data_type) for c in shop.columns_of(SEMANTIC)}
    assert fields == {
        "order": ("entity", "primary"),
        "customer": ("entity", "foreign"),
        "ordered_at": ("dimension", "time"),
        "order_size": ("dimension", "categorical"),
        "order_total": ("measure", "sum"),
        "orders_placed": ("measure", "count"),
    }
    assert shop.columns[f"{SEMANTIC}#order_total"].description == "Revenue, summed."


def test_each_field_reads_the_columns_its_expression_names_or_its_own(shop):
    """A name is declared; an expression is parsed, so what it reads is inferred."""
    assert reads(shop, SEMANTIC) == {MODEL: "declared"}
    assert {
        name: reads(shop, f"{SEMANTIC}#{name}") for name in ("order", "order_size", "order_total")
    } == {
        "order": {f"{MODEL}#order_id": "declared"},  # expr: order_id
        "order_size": {f"{MODEL}#order_total": "inferred"},  # case when order_total >= 100 ...
        "order_total": {f"{MODEL}#order_total": "declared"},  # no expr: its own name
    }
    assert reads(shop, f"{SEMANTIC}#ordered_at") == {f"{MODEL}#ordered_at": "declared"}


def test_metrics_read_their_measures_and_their_filters_dimensions(shop):
    assert reads(shop, f"{METRIC}.revenue") == {
        SEMANTIC: "declared",
        f"{SEMANTIC}#order_total": "declared",
    }
    # Filtered on order_size: the filter reads it too
    assert reads(shop, f"{METRIC}.large_orders") == {
        SEMANTIC: "declared",
        f"{SEMANTIC}#orders_placed": "declared",
        f"{SEMANTIC}#order_size": "declared",
    }
    # A ratio reads its two metrics, whole
    assert reads(shop, f"{METRIC}.average_order_value") == {
        f"{METRIC}.revenue": "declared",
        f"{METRIC}.order_count": "declared",
    }


def test_a_saved_query_reads_its_metrics_and_what_it_groups_by_and_lists_its_exports(shop):
    assert reads(shop, QUERY) == {
        f"{METRIC}.revenue": "declared",
        f"{METRIC}.average_order_value": "declared",
        f"{SEMANTIC}#ordered_at": "declared",  # TimeDimension('order__ordered_at', 'week')
        SEMANTIC: "declared",  # so model-level traversal sees the read
    }
    assert shop.nodes[QUERY].properties["exports"] == [
        {"name": "weekly_revenue", "as": "table", "relation": "exports.weekly_revenue"}
    ]


def test_everything_the_fixture_reads_is_known(shop):
    semantic = [n for n in shop.nodes.values() if n.tool == "dbt_semantic"]
    assert {shop.consumer_lineage[n.id].state for n in semantic} == {LineageState.RESOLVED}
    assert shop.unresolved == []
    assert shop.column_lineage == {}  # a model's column lineage is not the semantic layer's


def test_the_newer_yaml_reads_the_same_columns(build):
    """dbt 1.12's YAML has no measures: a metric aggregates over its semantic model's model, and
    reads that model's columns itself."""
    with open(SEMANTIC_V2, "rb") as f:
        graph = build(json.load(f))
    assert {c.name for c in graph.columns_of(SEMANTIC)} == {
        "order",
        "customer",
        "ordered_at",
        "order_size",
    }
    assert reads(graph, f"{METRIC}.revenue") == {
        SEMANTIC: "declared",
        MODEL: "declared",
        f"{MODEL}#order_total": "declared",  # expr: order_total
    }
    assert reads(graph, f"{METRIC}.large_orders") == {
        SEMANTIC: "declared",
        MODEL: "declared",
        f"{MODEL}#order_id": "declared",
        f"{SEMANTIC}#order_size": "declared",
    }
    assert graph.unresolved == []
    assert graph.nodes[f"{METRIC}.revenue"].owner == "finance-analytics"


# Impact


@pytest.mark.parametrize("form", ["older", "newer"])
def test_a_renamed_column_reaches_the_metrics_and_the_saved_query_that_use_it(build, form):
    if form == "older":
        graph = build()
    else:
        with open(SEMANTIC_V2, "rb") as f:
            graph = build(json.load(f))
    found = impact(graph, [f"{MODEL}#order_total"])
    semantic = {n: hit for n, hit in found.nodes.items() if graph.nodes[n].tool}
    assert set(semantic) == {
        SEMANTIC,
        f"{METRIC}.revenue",
        f"{METRIC}.large_orders",
        f"{METRIC}.average_order_value",
        QUERY,
    }, "order_count counts order_id only"
    revenue = semantic[f"{METRIC}.revenue"]
    assert revenue.granularity is Granularity.COLUMN
    measure = f"{SEMANTIC}#order_total" if form == "older" else f"{MODEL}#order_total"
    assert (revenue.columns, revenue.reads) == ([], [measure])
    large = semantic[f"{METRIC}.large_orders"]
    assert large.reads == [f"{SEMANTIC}#order_size"]
    assert large.confidence is Confidence.INFERRED  # through the parsed case expression
    whole = semantic[QUERY]
    assert (whole.granularity, whole.reason) == (Granularity.MODEL, "uses the whole node")
    # Counted as dbt's own nodes are, by what each is
    counts = found.counts_by_tool()
    semantic_kinds = ("metric", "saved_query", "semantic_model")
    assert {key: n for key, n in counts.items() if key[0] in semantic_kinds} == {
        ("metric", None): 3,
        ("saved_query", None): 1,
        ("semantic_model", None): 1,
    }


def test_a_change_to_the_whole_model_reaches_every_metric_on_it(shop):
    found = impact(shop, [MODEL])
    assert {n for n in found.nodes if shop.nodes[n].tool} == {
        SEMANTIC,
        f"{METRIC}.revenue",
        f"{METRIC}.order_count",
        f"{METRIC}.large_orders",
        f"{METRIC}.average_order_value",
        QUERY,
    }


def test_an_expression_that_cannot_be_read_puts_the_semantic_model_at_model_level(build, manifest):
    field(manifest, "dimensions", "order_size")["expr"] = "case when ("
    graph = build(manifest)
    (reason,) = [r for (s, _), r in unknowns(graph).items() if s == f"{SEMANTIC}#order_size"]
    assert reason.startswith("the expression could not be parsed: ParseError")
    status = graph.consumer_lineage[SEMANTIC]
    assert status.state is LineageState.UNKNOWN
    assert status.reason == "the expression of order_size cannot be read"
    # Which columns it reads is no longer known, so a change to any of them reaches it all
    found = impact(graph, [f"{MODEL}#customer_id"])
    hit = found.nodes[SEMANTIC]
    assert hit.granularity is Granularity.MODEL
    assert hit.reason == "column lineage unknown: the expression of order_size cannot be read"
    assert f"{METRIC}.order_count" in found.nodes
    assert {u.subject for u in found.unknowns} == {f"{SEMANTIC}#order_size"}


@pytest.mark.parametrize(
    ("expr", "reason"),
    [
        ("{{ var('size_column') }}", "the expression is templated"),
        ("(select max(order_total) from orders)", "the expression has a subquery"),
        ("x" * (MAX_EXPR_CHARS + 1), "over the 10,000 limit"),
    ],
)
def test_what_an_expression_cannot_say_is_an_unknown(build, manifest, expr, reason):
    field(manifest, "dimensions", "order_size")["expr"] = expr
    graph = build(manifest)
    (found,) = [r for (s, _), r in unknowns(graph).items() if s == f"{SEMANTIC}#order_size"]
    assert reason in found
    assert graph.consumer_lineage[SEMANTIC].state is LineageState.UNKNOWN


def test_a_column_the_model_does_not_list_is_unresolved(build, manifest):
    """Without the catalog, only the documented columns are known: ordered_at is not."""
    graph = build(catalog=False)
    assert unknowns(graph) == {
        (f"{SEMANTIC}#ordered_at", "orders.ordered_at"): (
            "model.impact_shop.orders has no column 'ordered_at' in the catalog or the docs; "
            "run dbt docs generate"
        ),
    }
    status = graph.consumer_lineage[SEMANTIC]
    assert (status.state, status.reason) == (LineageState.PARTIAL, "1 reference did not resolve")


def test_an_expr_written_as_a_number_reads_no_column(build, manifest):
    field(manifest, "measures", "orders_placed")["expr"] = 1
    graph = build(manifest)
    assert reads(graph, f"{SEMANTIC}#orders_placed") == {}
    assert graph.consumer_lineage[SEMANTIC].state is LineageState.RESOLVED


def test_a_semantic_model_with_no_model_is_unknown(build, manifest):
    semantic_model(manifest)["depends_on"]["nodes"] = []
    graph = build(manifest)
    assert unknowns(graph) == {(SEMANTIC, "model"): "the manifest names no model for it"}
    assert graph.consumer_lineage[SEMANTIC].reason == "its model is not in the graph"
    assert reads(graph, f"{SEMANTIC}#order_total") == {}


# Names in filters and group-bys


def test_names_no_semantic_model_has_are_unresolved(build, manifest):
    metric = manifest["metrics"][f"{METRIC}.large_orders"]
    metric["filter"]["where_filters"][0]["where_sql_template"] = (
        "{{ Dimension('order__nope') }} = 1 and {{ Entity('nobody') }} is not null"
    )
    graph = build(manifest)
    assert unknowns(graph) == {
        (f"{METRIC}.large_orders", "order__nope"): "no semantic model has a dimension named 'nope'",
        (f"{METRIC}.large_orders", "nobody"): "no semantic model has an entity named 'nobody'",
    }
    status = graph.consumer_lineage[f"{METRIC}.large_orders"]
    assert (status.state, status.reason) == (LineageState.PARTIAL, "2 references did not resolve")


def test_a_reference_that_cannot_be_read_is_an_unknown(build, manifest):
    metric = manifest["metrics"][f"{METRIC}.large_orders"]
    metric["filter"]["where_filters"][0]["where_sql_template"] = "{{ Dimension(size) }} = 'large'"
    graph = build(manifest)
    assert unknowns(graph) == {
        (f"{METRIC}.large_orders", "{{ Dimension(size) }} = 'large'"): (
            "a reference here cannot be read as a name"
        )
    }
    assert graph.consumer_lineage[f"{METRIC}.large_orders"].state is LineageState.UNKNOWN


def test_metric_time_and_a_grain_in_a_name_are_understood(build, manifest):
    query = manifest["saved_queries"][QUERY]["query_params"]
    query["group_by"] = [
        "TimeDimension('metric_time', 'month')",
        "Dimension('order__ordered_at__month')",
        "Dimension('metric_time__week')",
        "Entity('order__customer')",
    ]
    graph = build(manifest)
    assert graph.unresolved == []
    assert {r for r in reads(graph, QUERY) if "#" in r} == {
        f"{SEMANTIC}#ordered_at",
        f"{SEMANTIC}#customer",
    }


def test_a_name_two_semantic_models_have_is_read_in_both_as_heuristic(build, manifest):
    twin = copy.deepcopy(semantic_model(manifest))
    twin.update(name="orders_copy", unique_id="semantic_model.impact_shop.orders_copy")
    manifest["semantic_models"][twin["unique_id"]] = twin
    for item in twin["measures"]:
        item["name"] += "_copy"  # MetricFlow wants measure names unique; dimensions may repeat
    graph = build(manifest)
    filtered = reads(graph, f"{METRIC}.large_orders")
    assert filtered[f"{SEMANTIC}#order_size"] == "heuristic"
    assert filtered["semantic_model.impact_shop.orders_copy#order_size"] == "heuristic"
    # A measure is looked for in the semantic models the metric depends on first
    assert reads(graph, f"{METRIC}.revenue")[f"{SEMANTIC}#order_total"] == "declared"


def test_a_measure_in_a_semantic_model_the_metric_does_not_name_is_still_found(build, manifest):
    manifest["metrics"][f"{METRIC}.revenue"]["depends_on"]["nodes"] = []
    graph = build(manifest)
    assert reads(graph, f"{METRIC}.revenue") == {
        f"{SEMANTIC}#order_total": "declared",
        SEMANTIC: "declared",
    }


def test_references_are_capped(build, manifest):
    query = manifest["saved_queries"][QUERY]["query_params"]
    query["group_by"] = [f"Dimension('order__d{i}')" for i in range(MAX_REFERENCES * 3)]
    graph = build(manifest)
    assert len(graph.unresolved_for(QUERY)) == MAX_REFERENCES


# Untrusted input


def test_a_malformed_entry_costs_only_that_node_or_field(write_json, manifest):
    semantic_model(manifest)["entities"] = "not-a-list"
    semantic_model(manifest)["measures"].append({"agg": "sum"})  # no name
    manifest["metrics"][f"{METRIC}.order_count"] = ["not", "an", "object"]
    manifest["saved_queries"][QUERY]["exports"] = [{"config": {}}, "x"]
    result = DbtParser().parse({"manifest_path": str(write_json(manifest))})
    assert not result.has_errors
    (warning,) = [e.message for e in result.errors]
    assert f"metric {METRIC}.order_count" in warning
    graph = build_dbt_graph(result)
    assert {c.name for c in graph.columns_of(SEMANTIC)} == {
        "ordered_at",
        "order_size",
        "order_total",
        "orders_placed",
    }
    assert graph.nodes[QUERY].properties["exports"] == []
    # The ratio still reads revenue; order_count is gone, and said to be
    assert f"{METRIC}.revenue" in reads(graph, f"{METRIC}.average_order_value")
    assert (f"{METRIC}.average_order_value", f"{METRIC}.order_count") in {
        (u.subject, u.reference) for u in graph.unresolved
    }


# Traversal, snapshots and exports


def test_model_level_traversal_never_steps_into_a_column(shop):
    up = traverse(shop, [f"{METRIC}.revenue"], Direction.UPSTREAM)
    assert all("#" not in ref for ref in up.reached)
    assert {SEMANTIC, MODEL} <= set(up.reached)
    down = traverse(shop, [MODEL], Direction.DOWNSTREAM)
    assert {SEMANTIC, f"{METRIC}.revenue", QUERY} <= set(down.reached)
    # Column-level traversal stays column to column: a metric is not a column
    column = [
        s.ref for s in steps(shop, f"{MODEL}#order_total", Direction.DOWNSTREAM, Granularity.COLUMN)
    ]
    assert f"{SEMANTIC}#order_total" in column and f"{METRIC}.revenue" not in column


def test_a_snapshot_keeps_what_the_semantic_layer_reads(build, manifest):
    field(manifest, "dimensions", "order_size")["expr"] = "case when ("
    graph = build(manifest)
    again = from_dict(json.loads(json.dumps(to_dict(graph))))
    assert again.consumer_lineage == graph.consumer_lineage
    assert reads(again, f"{METRIC}.revenue") == reads(graph, f"{METRIC}.revenue")


def test_an_export_with_columns_keeps_a_metric_s_read_of_a_measure(shop):
    view = graph_view(shop, [SEMANTIC, f"{METRIC}.revenue"], columns=True)
    consumes = {(e.upstream, e.downstream) for e in view.edges if e.kind == "consumes"}
    assert (f"{METRIC}.revenue", f"{SEMANTIC}#order_total") in consumes
    assert (f"{METRIC}.revenue", SEMANTIC) in consumes
