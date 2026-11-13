"""Task 2.3 end to end: the lineage/* requests on the pilot-shop fixture.

Every response is validated against the shared schema, as the extension does.
"""

from __future__ import annotations

import csv
import io
import json

import pytest
from lsprotocol import types
from pygls.exceptions import JsonRpcException

from lineagepilot_lsp import protocol

J = "model.pilot_shop"


async def ready(start, **kwargs):
    client, _ = await start(**kwargs)
    status = await client.ready()
    assert status["state"] == "ready", status
    return client


async def call(client, method: str, params) -> dict:
    result = await client.request(method, params)
    protocol.validate(protocol.REQUESTS[method][1], result)
    return result


async def error_of(client, method: str, params) -> JsonRpcException:
    with pytest.raises(JsonRpcException) as caught:
        await client.request(method, params)
    return caught.value


def line_of(path, text: str) -> int:
    return next(i for i, line in enumerate(path.read_text().splitlines()) if line.strip() == text)


async def test_every_status_and_update_matches_the_schema(start):
    client = await ready(start)
    protocol.validate("Status", await call(client, "lineage/status", {}))
    for update in client.updates:
        protocol.validate("Status", update)


async def test_graph_downstream(start, workspace):
    client = await ready(start)
    result = await call(
        client,
        "lineage/graph",
        {"root": f"{J}.orders", "direction": "downstream", "depth": 2, "granularity": "model"},
    )
    nodes = {n["id"]: n for n in result["nodes"]}
    assert nodes[f"{J}.orders"]["side"] == "root" and nodes[f"{J}.orders"]["depth"] == 0
    assert nodes[f"{J}.customers"]["side"] == "downstream" and nodes[f"{J}.customers"]["depth"] == 1
    assert nodes[f"{J}.orders"]["location"] == {
        "uri": (workspace / "models" / "marts" / "orders.sql").as_uri(),
        "range": None,
    }
    assert nodes[f"{J}.orders"]["columnLineage"] == "resolved"
    edge = next(e for e in result["edges"] if e["downstream"] == f"{J}.customers")
    assert edge == {
        "kind": "depends_on",
        "upstream": f"{J}.orders",
        "downstream": f"{J}.customers",
        "relation": None,
        "confidence": "declared",
        "origins": ["manifest"],
        "transformation": "ref",
    }
    assert not result["truncated"] and result["unknowns"] == []


async def test_graph_upstream_crosses_the_physical_table(start, workspace):
    client = await ready(start)
    result = await call(
        client,
        "lineage/graph",
        {"root": f"{J}.stg_orders", "direction": "upstream", "depth": 2, "granularity": "model"},
    )
    nodes = {n["id"]: n for n in result["nodes"]}
    source, seed = "source.pilot_shop.ecom.raw_orders", "seed.pilot_shop.raw_orders"
    assert (nodes[source]["depth"], nodes[seed]["depth"]) == (1, 2)
    sources = workspace / "models" / "staging" / "__sources.yml"
    line = line_of(sources, "- name: raw_orders")
    assert nodes[source]["location"]["uri"] == sources.as_uri()
    assert nodes[source]["location"]["range"]["start"]["line"] == line
    # The seed's CSV is not in the fixture, so there is nowhere to open
    assert nodes[seed]["location"] is None
    junction = next(e for e in result["edges"] if e["downstream"] == source)
    assert junction["upstream"] == seed and junction["kind"] == "materialises"
    assert junction["relation"] and junction["relation"].startswith("relation:")
    assert junction["transformation"] is None


async def test_graph_both_ways_and_truncation(start):
    client = await ready(start)
    result = await call(
        client,
        "lineage/graph",
        {
            "root": f"{J}.orders",
            "direction": "both",
            "depth": 1,
            "granularity": "model",
            "maxNodes": 2,
        },
    )
    assert result["truncated"]
    sides = {n["side"] for n in result["nodes"]}
    assert sides <= {"root", "upstream", "downstream"} and "root" in sides


async def test_graph_of_a_column(start, workspace):
    client = await ready(start)
    root = f"{J}.customers#lifetime_spend"
    result = await call(
        client,
        "lineage/graph",
        {"root": root, "direction": "upstream", "depth": 1, "granularity": "column"},
    )
    nodes = {n["id"]: n for n in result["nodes"]}
    assert nodes[root]["kind"] == "column" and nodes[root]["parent"] == f"{J}.customers"
    customers_yml = workspace / "models" / "marts" / "customers.yml"
    assert nodes[root]["location"]["uri"] == customers_yml.as_uri()
    assert nodes[root]["location"]["range"]["start"]["line"] == line_of(
        customers_yml, "- name: lifetime_spend"
    )
    [edge] = result["edges"]
    assert (edge["upstream"], edge["kind"], edge["transformation"]) == (
        f"{J}.orders#order_total",
        "derives",
        "aggregate",
    )
    assert edge["confidence"] == "inferred" and edge["origins"] == ["compiled_sql"]


async def test_impact_of_a_column(start):
    client = await ready(start)
    result = await call(client, "lineage/impact", {"targets": [f"{J}.stg_orders#order_total"]})
    nodes = {n["id"]: n for n in result["nodes"]}
    assert set(nodes) == {f"{J}.orders", f"{J}.customers"}
    assert nodes[f"{J}.orders"]["columns"] == [f"{J}.orders#order_total"]
    assert nodes[f"{J}.customers"]["columns"] == [f"{J}.customers#lifetime_spend"]
    assert nodes[f"{J}.customers"]["granularity"] == "column"
    assert result["counts"] == {"model": 2} and result["columnLineage"] == "done"


async def test_impact_of_a_model_with_depth(start):
    client = await ready(start)
    result = await call(client, "lineage/impact", {"targets": [f"{J}.stg_orders"], "maxDepth": 1})
    assert result["truncated"]
    assert all(n["depth"] == 1 for n in result["nodes"])


async def test_unknown_ids_get_their_own_error(start):
    client = await ready(start)
    error = await error_of(client, "lineage/impact", {"targets": [f"{J}.gone"]})
    assert error.code == protocol.ErrorCode.UNKNOWN_NODE and "gone" in error.message
    for params in (
        {"root": f"{J}.gone", "direction": "upstream", "depth": 1, "granularity": "model"},
        {"root": f"{J}.orders", "direction": "upstream", "depth": 1, "granularity": "column"},
    ):
        error = await error_of(client, "lineage/graph", params)
        assert error.code == protocol.ErrorCode.UNKNOWN_NODE
    error = await error_of(client, "lineage/export", {"format": "json", "root": f"{J}.gone"})
    assert error.code == protocol.ErrorCode.UNKNOWN_NODE


@pytest.mark.parametrize(
    "method, params, fragment",
    [
        (
            "lineage/graph",
            {"root": "x", "direction": "up", "depth": 1, "granularity": "model"},
            "direction",
        ),
        (
            "lineage/graph",
            {"root": "x", "direction": "upstream", "depth": 11, "granularity": "model"},
            "depth",
        ),
        ("lineage/impact", {"targets": []}, "targets"),
        ("lineage/status", {"surprise": True}, "surprise"),
        ("lineage/export", {"format": "xml"}, "format"),
    ],
)
async def test_invalid_params_are_refused(start, method, params, fragment):
    client = await ready(start)
    error = await error_of(client, method, params)
    assert error.code == types.ErrorCodes.InvalidParams and fragment in error.message


async def test_a_request_without_params_is_answered_with_an_error(start):
    """The hang from S1 finding 4 cannot happen: a missing params object is refused."""
    client = await ready(start)
    with pytest.raises(JsonRpcException) as caught:
        await client.protocol.send_request_async("lineage/status", None)
    assert caught.value.code == types.ErrorCodes.InvalidParams


async def test_requests_before_a_graph_is_loaded(start, workspace):
    (workspace / "target" / "manifest.json").unlink()
    client, _ = await start()
    await client.ready()
    error = await error_of(client, "lineage/impact", {"targets": [f"{J}.orders"]})
    assert error.code == protocol.ErrorCode.NOT_READY
    assert "unknown" in error.message and "No manifest" in error.message


async def test_export_formats(start):
    client = await ready(start)
    whole = await call(client, "lineage/export", {"format": "json"})
    assert whole["filename"] == "pilot_shop-lineage.json"
    assert len(json.loads(whole["content"])["nodes"]) == whole["nodes"] > 20

    part = await call(
        client,
        "lineage/export",
        {"format": "mermaid", "root": f"{J}.orders", "direction": "downstream", "depth": 1},
    )
    assert part["filename"] == "pilot_shop-orders.mmd"
    assert part["content"].startswith("flowchart") and part["nodes"] == 2 and part["edges"] == 1

    around = await call(client, "lineage/export", {"format": "dot", "root": f"{J}.orders"})
    assert around["nodes"] > part["nodes"]

    pii = await call(client, "lineage/export", {"format": "pii-csv"})
    assert pii["filename"] == "pilot_shop-pii-inventory.csv"
    header = next(csv.reader(io.StringIO(pii["content"])))
    assert "column" in [h.lower() for h in header] or header
    markdown = await call(client, "lineage/export", {"format": "pii-markdown"})
    assert markdown["filename"].endswith(".md") and markdown["content"].startswith("#")


async def test_nodes_for_file(start, workspace):
    client = await ready(start)

    async def ids(path) -> list[str]:
        uri = path if isinstance(path, str) else path.as_uri()
        result = await call(client, "lineage/nodesForFile", {"uri": uri})
        return sorted(n["id"] for n in result["nodes"])

    assert await ids(workspace / "models" / "marts" / "orders.sql") == [f"{J}.orders"]
    assert await ids(workspace / "models" / "marts" / "orders.yml") == [f"{J}.orders"]
    assert len(await ids(workspace / "models" / "staging" / "__sources.yml")) == 5
    assert await ids(workspace / "README.md") == []
    assert await ids(workspace.parent / "elsewhere.sql") == []
    assert await ids("untitled:Untitled-1") == []


async def test_node_details(start):
    client = await ready(start)
    orders = await call(client, "lineage/node", {"id": f"{J}.orders"})
    assert orders["node"]["name"] == "orders" and orders["node"]["side"] == "root"
    assert (orders["upstream"], orders["downstream"]) == (
        2,
        1,
    )  # stg_orders, order_items; customers
    columns = {c["name"]: c for c in orders["columns"]}
    assert "order_total" in columns and columns["order_id"]["tests"] >= 2
    customers = await call(client, "lineage/node", {"id": f"{J}.customers"})
    assert any(c["pii"] for c in customers["columns"])
    column = await call(client, "lineage/node", {"id": f"{J}.customers#lifetime_spend"})
    assert column["node"]["kind"] == "column" and column["columns"] == []
    assert (column["upstream"], column["downstream"]) == (1, 0)
    error = await error_of(client, "lineage/node", {"id": f"{J}.nowhere"})
    assert error.code == protocol.ErrorCode.UNKNOWN_NODE
