"""Benchmark harness functions, exercised on the small fixtures."""

from __future__ import annotations

import json

import pytest

from spike import bench as B
from spike import graph as G
from tests.conftest import REAL_FIXTURES, parse


@pytest.fixture(scope="module")
def synth_graph(mods):
    m, c = REAL_FIXTURES["synth-24"]
    g = G.build_graph(parse(mods, m, c))
    G.add_manifest_extras(g, json.loads(m.read_bytes()))
    return g


def test_pct():
    p = B.pct([float(x) for x in range(1, 101)])
    assert p["n"] == 100 and p["max"] == 100 and p["min"] == 1
    assert p["p50"] == pytest.approx(50.5) and p["p95"] == pytest.approx(95.05)
    assert B.pct([]) == {}


def test_env_info_records_arch():
    info = B.env_info()
    assert info["machine"] and info["python"]


def test_cold_index_phases(mods):
    m, c = REAL_FIXTURES["synth-24"]
    timings, result, graph = B.cold_index(m, c, "as-is")
    assert set(timings) >= {"import_s", "parse_s", "graph_s", "total_s", "phases_s"}
    assert {"_load_json", "_process_models", "_build_lineage"} <= set(timings["phases_s"])
    assert timings["total_s"] >= timings["parse_s"] > 0
    t2, _, g2 = B.cold_index(m, c, "optimised")
    assert B.graph_fingerprint(g2) == B.graph_fingerprint(graph)


def test_closure_sizes_match_bfs(synth_graph):
    sizes = B.closure_sizes(synth_graph)
    for u in synth_graph.nodes:
        assert sizes[u] == len(synth_graph.downstream_closure(u))


def test_codelens_and_impact_bench(synth_graph):
    cl = B.bench_codelens(synth_graph, n=50)
    assert cl["ms"]["n"] == 50 and cl["ms"]["p95"] >= 0
    imp = B.bench_impact(synth_graph, repeats=3)
    assert {"high_fanout_source", "median_model", "worst_node"} <= set(imp)
    assert imp["worst_node"]["closure"] >= imp["median_model"]["closure"]


def test_column_lineage_bench_on_jaffle():
    m, _ = REAL_FIXTURES["jaffle-1.12"]
    man = json.loads(m.read_bytes())
    res = B.bench_column_lineage(man, n=3, repeats=1, with_alternative=True)
    assert res["dialect"] == "duckdb" and len(res["models"]) == 3
    # Finding: dbt-style "select * from final" gives a single '*' mapping.
    assert all(r["star_only"] for r in res["models"])
    assert all("alt_sqlglot_lineage" in r for r in res["models"])


def test_event_loop_block_runs():
    m, c = REAL_FIXTURES["synth-24"]
    res = B.event_loop_block(m, c)
    assert res["await_in_loop"]["max_loop_gap_ms"] > 0 and res["to_thread"]["parse_s"] > 0


@pytest.mark.parametrize("task,extra", [
    ("cold", {"variant": "as-is"}),
    ("queries", {}),
    ("json", {"repeats": 1}),
])
def test_worker_cli(task, extra):
    m, c = REAL_FIXTURES["synth-24"]
    res = B.run_worker(task, manifest=m, catalog=c, **extra)
    assert res["task"] == task and res["env"]["machine"]
    if task == "cold":
        assert res["forbidden_modules"] == [] and res["peak_rss_mb"] > 0


def test_snapshot_workers(tmp_path):
    m, c = REAL_FIXTURES["synth-24"]
    w = B.run_worker("snapshot-write", manifest=m, catalog=c, snapshot_dir=tmp_path)
    for store in ("json", "sqlite"):
        r = B.run_worker("warm", manifest=m, snapshot_dir=tmp_path, store=store, json_backend="stdlib")
        assert r["fingerprint"] == w["fingerprint"] and r["total_s"] > 0


def test_warm_refuses_stale_snapshot(tmp_path):
    m, c = REAL_FIXTURES["synth-24"]
    B.run_worker("snapshot-write", manifest=m, catalog=c, snapshot_dir=tmp_path)
    other, _ = REAL_FIXTURES["jaffle-1.12"]
    with pytest.raises(RuntimeError, match="stale"):
        B.run_worker("warm", manifest=other, snapshot_dir=tmp_path, store="json", json_backend="stdlib")
