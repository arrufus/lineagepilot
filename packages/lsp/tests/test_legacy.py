"""Protocol 1 answers from protocol 2's (task 7.5): what a client built before protocol 2 gets."""

from __future__ import annotations

import pytest
from conftest import validate_v1

from lineagepilot_lsp import legacy, protocol

ORDERS = "model.shop.orders"
TASK = "airflow:daily.marts"
METRIC = "metric.shop.revenue"


def graph_node(node_id: str, kind: str, tool: str | None = None, side: str = "downstream"):
    return {
        "id": node_id,
        "kind": kind,
        "name": node_id.rsplit(".", 1)[-1],
        "side": side,
        "depth": 0 if side == "root" else 1,
        "parent": None,
        "location": None,
        "layer": None,
        "materialization": None,
        "owner": None,
        "description": None,
        "columnLineage": None,
        "columnLineageReason": None,
        "pii": False,
        "tool": tool,
        "subkind": None,
    }


def edge(kind: str, upstream: str, downstream: str):
    return {
        "kind": kind,
        "upstream": upstream,
        "downstream": downstream,
        "relation": None,
        "confidence": "declared",
        "origins": ["annotation"],
        "transformation": None,
    }


def graph_result():
    return {
        "root": ORDERS,
        "direction": "downstream",
        "granularity": "model",
        "nodes": [
            graph_node(ORDERS, "model", side="root"),
            graph_node(TASK, "task", "airflow"),
            graph_node("dagster:nightly.load", "task", "dagster"),
            graph_node(METRIC, "consumer", "dbt_semantic"),
        ],
        "edges": [
            edge("orchestrates", TASK, ORDERS),
            edge("orchestrates", "dagster:nightly.load", ORDERS),
            edge("consumes", METRIC, ORDERS),
        ],
        "truncated": False,
        "unknowns": [],
    }


class TestGraph:
    def test_a_protocol_2_graph_is_valid_protocol_2(self):
        protocol.validate("GraphResult", graph_result())

    def test_protocol_1_keeps_what_it_can_name(self):
        result = legacy.answer("lineage/graph", graph_result())
        validate_v1("GraphResult", result)
        assert [(n["id"], n["kind"]) for n in result["nodes"]] == [
            (ORDERS, "model"),
            (TASK, "airflow_task"),
        ]
        assert [(e["kind"], e["upstream"]) for e in result["edges"]] == [("orchestrates", TASK)]
        assert all("tool" not in n and "subkind" not in n for n in result["nodes"])

    def test_without_the_conversion_protocol_1_would_refuse_it(self):
        with pytest.raises(protocol.ValidationError):
            validate_v1("GraphResult", graph_result())

    def test_a_root_protocol_1_cannot_name_is_an_unknown_node(self):
        result = graph_result()
        result["root"] = METRIC
        with pytest.raises(legacy.Unrepresentable) as raised:
            legacy.answer("lineage/graph", result)
        assert raised.value.ref == METRIC


class TestCountsAndRows:
    def test_impact_counts_follow_the_rows_kept(self):
        def hit(node_id, kind, tool=None):
            return {
                "id": node_id,
                "kind": kind,
                "name": "x",
                "depth": 1,
                "granularity": "model",
                "confidence": "declared",
                "columns": [],
                "reason": None,
                "location": None,
                "owner": None,
                "tool": tool,
                "subkind": None,
            }

        result = {
            "targets": [ORDERS],
            "truncated": False,
            "counts": {"consumer": 1, "model": 1, "task": 1},
            "nodes": [
                hit("model.shop.x", "model"),
                hit(TASK, "task", "airflow"),
                hit(METRIC, "consumer", "x"),
            ],
            "unknowns": [],
            "columnLineage": "done",
        }
        converted = legacy.answer("lineage/impact", result)
        validate_v1("ImpactResult", converted)
        assert converted["counts"] == {"airflow_task": 1, "model": 1}

    def test_a_node_protocol_1_cannot_name_is_an_unknown_node(self):
        result = {
            "node": graph_node(METRIC, "consumer", "x", "root"),
            "columns": [],
            "upstream": 1,
            "downstream": 0,
        }
        with pytest.raises(legacy.Unrepresentable):
            legacy.answer("lineage/node", result)

    def test_other_answers_pass_through(self):
        answer = {"anything": 1}
        assert legacy.answer("lineage/export", answer) is answer
