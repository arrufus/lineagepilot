"""The budget harness's own parts (task 7.10): each reader's workload, and the budget check.

The harness itself runs by hand on the 5,000-model synthetic project (scripts/server_budgets.py).
These check, on fixtures/impact-shop, that a workload is what the readers read, so a measurement
is of real work.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from lineagepilot_core.graph.build import build_dbt_graph
from lineagepilot_core.parsers import DbtParser

from lineagepilot_lsp.locate import Locator
from lineagepilot_lsp.project import read_project

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import server_budgets  # noqa: E402

ARTIFACTS = Path(__file__).resolve().parents[3] / "fixtures" / "impact-shop" / "target"


def parse(project: Path):
    return DbtParser().parse(
        {
            "manifest_path": str(project / "target" / "manifest.json"),
            "catalog_path": str(project / "target" / "catalog.json"),
        }
    )


def test_the_semantic_workload_is_a_layer_the_reader_links_whole(tmp_path):
    project = tmp_path / "project"
    workloads = server_budgets.make_project(ARTIFACTS, project, ["dbt_semantic"])
    # The mart and customer_revenue have a key and numbers; one ratio, one saved query, in
    # the marts folder's YAML and the shared file
    semantic = workloads["dbt_semantic"]
    assert {k: semantic[k] for k in ("semantic_models", "metrics", "saved_queries")} == {
        "semantic_models": 2,
        "metrics": 3,
        "saved_queries": 1,
    }
    assert semantic["yaml_files"] == 2 and semantic["yaml_mb"] >= 0
    graph = build_dbt_graph(parse(project))
    assert graph.unresolved == []
    semantic = [n for n in graph.nodes.values() if n.tool == "dbt_semantic"]
    assert {n.subkind for n in semantic} == {"semantic_model", "metric", "saved_query"}
    # The expression dimension is parsed, and a metric's filter reads it
    band = "semantic_model.impact_shop.orders#order_total_band"
    assert graph.columns[band].semantic_type == "dimension"
    readers = {e.upstream for e in graph.in_edges(band)}
    assert "metric.impact_shop.orders_order_total" in readers
    # The YAML written for it places every node at its own entry, as a project's would
    locator = Locator(read_project(project))
    for node in semantic:
        found = locator.node(node)
        assert found is not None and found[1] is not None, node.id
        line = found[0].read_text().splitlines()[found[1].start_line]
        assert line.strip().endswith(f"name: {node.name}"), (node.id, line)
    linked = server_budgets.link_semantic(project)
    assert linked["unresolved"] == 0 and linked["fields"] > 0 and linked["column_reads"] > 0


def test_the_airflow_workload_and_none(tmp_path):
    project = tmp_path / "with"
    assert server_budgets.make_project(ARTIFACTS, project, ["airflow"]) == {
        "airflow": {"dag_files": 51, "shell_tasks": 2000, "cosmos_dags": 1}
    }
    assert len(list((project / "dags").glob("*.py"))) == 51
    # Without the semantic layer, the manifest is the artifacts' own
    assert (project / "target" / "manifest.json").resolve() == (
        ARTIFACTS / "manifest.json"
    ).resolve()
    bare = tmp_path / "without"
    assert server_budgets.make_project(ARTIFACTS, bare, []) == {}
    assert not (bare / "dags").exists()
    # Every node's files are there, so a review finds the manifest has seen them
    manifest = json.loads((ARTIFACTS / "manifest.json").read_text())
    orders = manifest["nodes"]["model.impact_shop.orders"]
    assert (bare / orders["original_file_path"]).read_text() == orders["raw_code"]


def run_with(**values) -> dict:
    run = {
        "first_graph_s": 0.8,
        "codelens_p95_ms": 1.0,
        "impact_max_ms": 2.0,
        "review_100_models_ms": 120.0,
        "server_rss_mb": 400.0,
    }
    return {**run, **values}


def test_every_budget_is_checked_and_a_miss_is_said():
    held = server_budgets.checked({"cold": run_with(), "warm": run_with()})
    assert len(held) == len(server_budgets.BUDGETS) and all(b["held"] for b in held)
    review = [b for b in held if b["measure"] == "review_100_models_ms"]
    assert {b["budget"] for b in review} == {300} and {b["run"] for b in review} == {"cold", "warm"}
    missed = server_budgets.checked(
        {"cold": run_with(server_rss_mb=610.0), "warm": run_with(review_100_models_ms=None)}
    )
    assert [(b["run"], b["measure"]) for b in missed if not b["held"]] == [
        ("warm", "review_100_models_ms"),
        ("cold", "server_rss_mb"),
    ]


@pytest.mark.parametrize("value", ["nope", "airflow,nope"])
def test_an_unknown_reader_is_refused(value, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["server_budgets.py", str(ARTIFACTS), "--readers", value])
    with pytest.raises(SystemExit):
        server_budgets.main()
    assert "no reader 'nope'" in capsys.readouterr().err
