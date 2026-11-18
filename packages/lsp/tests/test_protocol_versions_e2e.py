"""Protocol 2 beside protocol 1, end to end (task 7.5, design section 9).

The server speaks both. A protocol 2 client gets the generic kinds with a tool, the readers'
status, lineage/pipelines and lineage/consumers. A protocol 1 client, such as one built before
protocol 2, gets what it always did: airflow_task and lineage/airflow, and nothing its validators,
generated from protocol/v1/, would refuse.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from conftest import REPO, age_sources, validate_v1

from lineagepilot_lsp import protocol

SHOP = REPO / "fixtures" / "impact-shop"
ORDERS = "model.impact_shop.orders"
SEMANTIC = "semantic_model.impact_shop.orders"
GROUP = "airflow:daily_marts.marts"


@pytest.fixture
def shop(tmp_path) -> Path:
    root = tmp_path / "impact_shop"
    ignore = shutil.ignore_patterns("changes", "make_fixture.py", "PROVENANCE.json")
    shutil.copytree(SHOP, root, ignore=ignore)
    age_sources(root)
    return root


async def ready(start, root, versions):
    client, init = await start(root=root, options={"protocolVersions": versions})
    assert init.capabilities.experimental["lineagePilot"]["protocolVersion"] == versions[-1]
    status = await client.wait_for_update(
        lambda u: not u["indexing"] and u["airflow"]["stage"] in ("done", "failed")
    )
    assert status["state"] == "ready", status
    return client


def graph_params(root: str = ORDERS) -> dict:
    return {"root": root, "direction": "downstream", "depth": 1, "granularity": "model"}


class TestProtocol2:
    async def test_the_status_has_every_reader(self, start, shop):
        client = await ready(start, shop, [1, 2])
        status = await client.request("lineage/status")
        protocol.validate("Status", status)
        [airflow] = status["readers"]
        assert airflow["tool"] == "airflow" and airflow["stage"] == "done"
        assert airflow["summary"]["analysed"] and airflow["summary"]["files"] >= 1
        assert airflow["summary"]["pipelines"] == status["airflow"]["summary"]["dags"]
        assert status["counts"]["nodeKinds"]["task"] >= 1
        assert "airflow_task" not in status["counts"]["nodeKinds"]

    async def test_nodes_have_their_tool(self, start, shop):
        client = await ready(start, shop, [2])
        graph = await client.request("lineage/graph", graph_params())
        protocol.validate("GraphResult", graph)
        task = next(n for n in graph["nodes"] if n["id"] == GROUP)
        assert (task["kind"], task["tool"], task["subkind"]) == ("task", "airflow", "task_group")
        assert next(n for n in graph["nodes"] if n["id"] == ORDERS)["tool"] is None
        found = await client.request("lineage/impact", {"targets": [ORDERS]})
        protocol.validate("ImpactResult", found)
        assert found["counts"]["task"] >= 1
        assert next(n for n in found["nodes"] if n["id"] == GROUP)["tool"] == "airflow"

    async def test_pipelines_by_tool(self, start, shop):
        client = await ready(start, shop, [2])
        result = await client.request("lineage/pipelines", {"nodes": [ORDERS]})
        protocol.validate("PipelinesResult", result)
        assert [r["tool"] for r in result["readers"]] == ["airflow"]
        tasks = {t["id"]: t for p in result["pipelines"] for t in p["tasks"]}
        assert tasks[GROUP]["kind"] == "task" and tasks[GROUP]["tool"] == "airflow"
        assert ORDERS in tasks[GROUP]["nodes"] and tasks[GROUP]["location"] is not None
        daily = next(p for p in result["pipelines"] if p["id"] == "airflow:daily_marts")
        assert daily["tool"] == "airflow" and daily["subkind"] == "dag"
        everything = await client.request("lineage/pipelines", {"nodes": None, "tools": None})
        assert len(everything["pipelines"]) >= len(result["pipelines"])
        other = await client.request("lineage/pipelines", {"nodes": None, "tools": ["dagster"]})
        assert other == {"readers": [], "pipelines": [], "unknowns": []}

    async def test_consumers_of_a_node(self, start, shop):
        client = await ready(start, shop, [2])
        result = await client.request("lineage/consumers", {"nodes": [ORDERS]})
        protocol.validate("ConsumersResult", result)
        # The semantic model on the mart reads it, field by field (task 7.8)
        (consumer,) = result["consumers"]
        assert {k: consumer[k] for k in ("id", "tool", "subkind", "name", "owner")} == {
            "id": "semantic_model.impact_shop.orders",
            "tool": "dbt_semantic",
            "subkind": "semantic_model",
            "name": "orders",
            "owner": "finance-data",
        }
        assert consumer["reads"] == [
            {
                "node": ORDERS,
                "columns": ["customer_id", "order_id", "order_total", "ordered_at"],
                "confidence": "declared",
                "origins": ["manifest"],
            }
        ]
        location = consumer["location"]
        assert location["uri"].endswith("models/marts/semantic.yml")
        semantic_yml = (shop / "models" / "marts" / "semantic.yml").read_text().splitlines()
        assert semantic_yml[location["range"]["start"]["line"]].strip() == "- name: orders"
        with pytest.raises(Exception, match="not in the lineage graph"):
            await client.request("lineage/consumers", {"nodes": ["model.nope.nope"]})

    async def test_the_semantic_layer(self, start, shop):
        """Task 7.8: metrics, the semantic model and the saved query, down to their fields."""
        client = await ready(start, shop, [2])
        graph = await client.request(
            "lineage/graph",
            {"root": ORDERS, "direction": "downstream", "depth": 4, "granularity": "model"},
        )
        protocol.validate("GraphResult", graph)
        nodes = {n["id"]: n for n in graph["nodes"]}
        revenue = nodes["metric.impact_shop.revenue"]
        assert (revenue["kind"], revenue["tool"], revenue["subkind"]) == (
            "consumer",
            "dbt_semantic",
            "metric",
        )
        assert nodes[SEMANTIC]["columnLineage"] == "resolved"
        found = await client.request("lineage/impact", {"targets": [f"{ORDERS}#order_total"]})
        protocol.validate("ImpactResult", found)
        hits = {n["id"]: n for n in found["nodes"]}
        assert hits["metric.impact_shop.revenue"]["granularity"] == "column"
        assert "metric.impact_shop.order_count" not in hits
        assert hits["saved_query.impact_shop.weekly_revenue"]["owner"] is None  # its group's
        files = await client.request(
            "lineage/nodesForFile", {"uri": (shop / "models" / "marts" / "semantic.yml").as_uri()}
        )
        assert len(files["nodes"]) == 6
        details = await client.request("lineage/node", {"id": SEMANTIC})
        protocol.validate("NodeResult", details)
        # Its fields are its columns
        assert {c["name"] for c in details["columns"]} == {
            "order",
            "customer",
            "ordered_at",
            "order_size",
            "order_total",
            "orders_placed",
        }

    async def test_declared_consumers_and_a_change_to_their_file(self, start, shop):
        """Task 7.4: the annotation file's consumers, read with the DAG files, and again when the
        file changes."""
        folder = shop / ".lineagepilot"
        folder.mkdir()
        annotation = folder / "lineage.yml"
        consumer = f"{{id: tableau:gl, owner: finance, consumes: [{ORDERS}#order_total]}}"
        annotation.write_text(f"consumers:\n  - {consumer}\n")
        client = await ready(start, shop, [2])
        result = await client.request("lineage/consumers", {"nodes": [ORDERS]})
        protocol.validate("ConsumersResult", result)
        gl = next(c for c in result["consumers"] if c["id"] == "tableau:gl")
        assert (gl["tool"], gl["owner"]) == ("tableau", "finance")
        assert gl["reads"][0]["columns"] == ["order_total"]
        assert gl["reads"][0]["origins"] == ["annotation"]
        status = await client.request("lineage/status")
        (airflow,) = [r for r in status["readers"] if r["tool"] == "airflow"]
        assert airflow["summary"]["consumers"] == 1
        generation = airflow["generation"]
        annotation.write_text(f"consumers:\n  - {{id: looker:gl, consumes: [{ORDERS}]}}\n")
        client.file_changed(annotation)
        await client.wait_for_update(
            lambda u: any(
                r["tool"] == "airflow" and r["generation"] > generation and r["stage"] == "done"
                for r in u["readers"]
            )
        )
        result = await client.request("lineage/consumers", {"nodes": [ORDERS]})
        ids = {c["id"] for c in result["consumers"]}
        assert "looker:gl" in ids and "tableau:gl" not in ids

    async def test_lineage_airflow_stays_for_one_version(self, start, shop):
        client = await ready(start, shop, [2])
        result = await client.request("lineage/airflow", {"nodes": [ORDERS]})
        protocol.validate("AirflowResult", result)
        kinds = {t["kind"] for d in result["dags"] for t in d["tasks"]}
        assert kinds <= {"airflow_task", "airflow_dag"} and kinds


class TestProtocol1:
    """What a client built before protocol 2 gets: the same answers it always did."""

    async def test_the_status(self, start, shop):
        client = await ready(start, shop, [1])
        status = await client.request("lineage/status")
        validate_v1("Status", status)
        assert "readers" not in status
        assert status["counts"]["nodeKinds"]["airflow_task"] >= 1

    async def test_graph_impact_and_the_file_s_nodes(self, start, shop):
        client = await ready(start, shop, [1])
        graph = await client.request("lineage/graph", graph_params())
        validate_v1("GraphResult", graph)
        assert next(n for n in graph["nodes"] if n["id"] == GROUP)["kind"] == "airflow_task"
        found = await client.request("lineage/impact", {"targets": [ORDERS]})
        validate_v1("ImpactResult", found)
        assert found["counts"]["airflow_task"] >= 1 and "task" not in found["counts"]
        node = await client.request("lineage/node", {"id": GROUP})
        validate_v1("NodeResult", node)
        files = await client.request(
            "lineage/nodesForFile", {"uri": (shop / "models" / "marts" / "orders.sql").as_uri()}
        )
        validate_v1("NodesForFileResult", files)
        # Protocol 1 has no consumers: the semantic layer is left out (task 7.8)
        assert not any(n["kind"] == "consumer" for n in graph["nodes"])
        assert not any(n["id"].startswith("metric.") for n in found["nodes"])
        semantic = await client.request(
            "lineage/nodesForFile", {"uri": (shop / "models" / "marts" / "semantic.yml").as_uri()}
        )
        assert semantic == {"nodes": []}

    async def test_lineage_airflow(self, start, shop):
        client = await ready(start, shop, [1])
        result = await client.request("lineage/airflow", {"nodes": [ORDERS]})
        validate_v1("AirflowResult", result)
        assert any(t["id"] == GROUP for d in result["dags"] for t in d["tasks"])

    @pytest.mark.parametrize(
        ("method", "params"),
        [("lineage/pipelines", {"nodes": None}), ("lineage/consumers", {"nodes": [ORDERS]})],
    )
    async def test_protocol_2_requests_are_refused(self, start, shop, method, params):
        client = await ready(start, shop, [1])
        with pytest.raises(Exception, match="is in protocol 2 to 2, and this client speaks 1"):
            await client.request(method, params)
