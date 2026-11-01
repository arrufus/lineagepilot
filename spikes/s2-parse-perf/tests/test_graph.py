"""The throwaway graph builder, its queries and snapshots."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from spike import graph as G
from tests.conftest import FIX, REAL_FIXTURES, parse


@pytest.fixture(scope="module")
def synth(mods):
    m, c = REAL_FIXTURES["synth-24"]
    r = parse(mods, m, c)
    g = G.build_graph(r)
    man = json.loads(m.read_bytes())
    return r, g, man


def test_nodes_and_edges_match_manifest(synth):
    r, g, man = synth
    assert set(g.nodes) == {c.unique_id for c in r.capsules}
    exp = G.expected_edges(man, g.nodes)
    assert len(r.edges) == exp["depends_on_to_capsules"] == exp["parent_map_to_capsules"]
    # The versioned model's two nodes share one URN, so one edge collapses.
    assert g.edge_count == len(r.edges) - g.duplicate_edges
    assert g.unresolved_edges == 0


def test_adjacency_is_symmetric(synth):
    _, g, _ = synth
    fwd = {(s, d) for s, ds in g.down.items() for d in ds}
    rev = {(s, d) for d, ss in g.up.items() for s in ss}
    assert fwd == rev


def test_file_index_covers_models_not_sources(synth):
    _, g, man = synth
    models = [u for u, n in g.nodes.items() if n["kind"] == "model"]
    assert all(u in g.lookup_file(g.nodes[u]["file"]) for u in models)
    # Finding: source capsules carry no original_file_path, so a sources.yml
    # file cannot be mapped to its sources from the ParseResult.
    assert all(n["file"] is None for n in g.nodes.values() if n["kind"] == "source")


def test_urn_collision_is_recorded_for_versioned_model(synth):
    _, g, man = synth
    versioned = sorted(u for u, n in man["nodes"].items() if n.get("version") is not None and n["resource_type"] == "model")
    assert len(versioned) == 2
    assert list(g.urn_collisions.values()) == [versioned]


def test_has_tests_is_always_false_on_real_manifest(synth):
    """Suspicion 2: tests are separate test.* nodes in real manifests."""
    r, g, man = synth
    models = [c for c in r.capsules if c.capsule_type == "model"]
    assert not any(c.has_tests for c in models)
    assert sum(c.test_count for c in models) == 0
    G.add_manifest_extras(g, man)
    assert g.extras["nodes_with_attached_tests"] > 0


def test_closure_and_impact(synth):
    _, g, _ = synth
    src = next(u for u, n in g.nodes.items() if n["kind"] == "source")
    closure = g.downstream_closure(src)
    assert closure and src not in closure
    for n in closure:  # every member is reachable through down edges
        assert any(p == src or p in closure for p in g.up[n])
    imp = g.impact(src)
    assert imp["total"] == len(closure) == sum(imp["by_kind"].values())


def test_codelens_text(synth):
    _, g, man = synth
    G.add_manifest_extras(g, man)
    f = next(n["file"] for n in g.nodes.values() if n["kind"] == "model")
    lens = g.codelens(f)
    assert lens and lens[0].startswith("↓ ")
    assert g.codelens("does/not/exist.sql") == []


@pytest.mark.parametrize("store", ["json", "sqlite"])
def test_snapshot_round_trip(synth, tmp_path, store):
    from spike.bench import graph_fingerprint
    _, g, _ = synth
    snap = g.to_snapshot("abc")
    if store == "json":
        G.write_json_snapshot(snap, tmp_path / "s.json")
        back = G.read_json_snapshot(tmp_path / "s.json")
    else:
        G.write_sqlite_snapshot(snap, tmp_path / "s.sqlite")
        back = G.read_sqlite_snapshot(tmp_path / "s.sqlite")
    g2 = G.Graph.from_snapshot(back)
    assert back["manifest_sha256"] == "abc"
    assert graph_fingerprint(g2) == graph_fingerprint(g)
    assert {u: {k: n.get(k) for k in G.SNAP_FIELDS} for u, n in g2.nodes.items()} == \
           {u: {k: n.get(k) for k in G.SNAP_FIELDS} for u, n in g.nodes.items()}
    assert {k: v for k, v in g2.columns.items() if v} == {k: v for k, v in g.columns.items() if v}


def test_collision_from_hand_built_result():
    caps = [SimpleNamespace(urn="u:x", unique_id=f"model.p.x.v{i}", capsule_type="model", name="x",
                            original_file_path=f"x_v{i}.sql", layer=None, materialization="view",
                            has_tests=False, test_count=0, config={}, meta={}) for i in (1, 2)]
    caps.append(SimpleNamespace(urn="u:y", unique_id="model.p.y", capsule_type="model", name="y",
                                original_file_path="y.sql", layer=None, materialization="view",
                                has_tests=False, test_count=0, config={}, meta={}))
    edges = [SimpleNamespace(source_urn="u:x", target_urn="u:y"), SimpleNamespace(source_urn="u:x", target_urn="u:y"),
             SimpleNamespace(source_urn="u:missing", target_urn="u:y")]
    g = G.build_graph(SimpleNamespace(capsules=caps, columns=[], edges=edges))
    assert g.urn_collisions == {"u:x": ["model.p.x.v1", "model.p.x.v2"]}
    assert g.duplicate_edges == 1 and g.unresolved_edges == 1
    assert g.down["model.p.x.v2"] == ["model.p.y"] and g.down["model.p.x.v1"] == []


def test_file_sha256(tmp_path):
    p = tmp_path / "f"
    p.write_bytes(b"abc")
    assert G.file_sha256(p) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
