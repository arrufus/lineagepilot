"""Snapshots, the indexing pipeline and the headless CLI (task 1.8)."""

from __future__ import annotations

import io
import json
import shutil

import pytest
from support import DBT_FIXTURES, REAL_PROJECTS, project_config

from lineagepilot_core import __version__
from lineagepilot_core.cli import main, resolve_targets
from lineagepilot_core.graph import UntrustedParseResult, build_dbt_graph
from lineagepilot_core.graph.column_lineage import add_column_lineage, compiled_sql
from lineagepilot_core.indexer import IndexStatus, index_project, locate_artifacts
from lineagepilot_core.parsers import DbtParser
from lineagepilot_core.parsers.base import ParseErrorSeverity, ParseResult, RawNode
from lineagepilot_core.snapshot import (
    SnapshotCache,
    SnapshotError,
    file_sha256,
    from_dict,
    load_snapshot,
    save_snapshot,
    snapshot_key,
    to_dict,
)


def full_graph(name: str):
    result = DbtParser().parse(project_config(name))
    graph = build_dbt_graph(result)
    add_column_lineage(graph, compiled_sql(result))
    return graph


def assert_same(a, b):
    assert a.adapter_type == b.adapter_type
    assert (a.project_name, a.schema_version, a.manifest) == (
        b.project_name,
        b.schema_version,
        b.manifest,
    )
    assert list(a.nodes.values()) == list(b.nodes.values())
    assert list(a.columns.values()) == list(b.columns.values())
    assert list(a.edges) == list(b.edges)
    assert a.unresolved == b.unresolved
    assert a.column_lineage == b.column_lineage
    for node_id in a.nodes:
        assert [c.id for c in a.columns_of(node_id)] == [c.id for c in b.columns_of(node_id)]
        assert a.nodes_for_file(a.nodes[node_id].file_path or "") == b.nodes_for_file(
            b.nodes[node_id].file_path or ""
        )


class TestSnapshot:
    @pytest.mark.parametrize("name", REAL_PROJECTS)
    def test_round_trip_is_exact(self, name, tmp_path):
        graph = full_graph(name)
        path = tmp_path / "graph.json"
        save_snapshot(graph, path)
        assert_same(graph, load_snapshot(path))

    def test_round_trip_keeps_unresolved_and_hostile_text(self, write_json, manifest_of):
        manifest = manifest_of("jaffle-1.12")
        node = manifest["nodes"]["model.jaffle_shop.customers"]
        node["description"] = 'quote " back\\slash \ud800 lone surrogate é\n'
        node["depends_on"]["nodes"].append("metric.jaffle_shop.revenue")
        graph = build_dbt_graph(DbtParser().parse({"manifest_path": str(write_json(manifest))}))
        restored = from_dict(json.loads(json.dumps(to_dict(graph))))
        assert_same(graph, restored)
        assert (
            restored.unresolved
            and restored.node("model.jaffle_shop.customers").description == (node["description"])
        )

    def test_writes_are_atomic(self, tmp_path, monkeypatch):
        graph = full_graph("jaffle-1.12")
        path = tmp_path / "graph.json"
        save_snapshot(graph, path)
        before = path.read_bytes()

        def fail(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr("lineagepilot_core.snapshot.os.replace", fail)
        with pytest.raises(OSError):
            save_snapshot(graph, path)
        assert path.read_bytes() == before
        assert [p.name for p in tmp_path.iterdir()] == ["graph.json"]  # no temp files left

    @pytest.mark.parametrize(
        ("data", "message"),
        [
            ([], "not a snapshot"),
            ({"format": 99, "core": __version__}, "does not match"),
            ({"format": 1, "core": "0.0.0"}, "does not match"),
            ({"format": 1, "core": __version__}, "malformed snapshot: KeyError"),
        ],
    )
    def test_bad_snapshots_are_refused(self, data, message):
        with pytest.raises(SnapshotError, match=message):
            from_dict(data)

    def test_tampered_snapshots_are_refused(self):
        data = to_dict(full_graph("jaffle-1.12"))
        broken_edge = json.loads(json.dumps(data))
        broken_edge["edges"][0][1] = "model.p.ghost"
        no_evidence = json.loads(json.dumps(data))
        no_evidence["edges"][0][4] = []
        bad_status = json.loads(json.dumps(data))
        bad_status["column_lineage"]["model.p.ghost"] = ["resolved", None]
        not_a_list = json.loads(json.dumps(data))
        not_a_list["columns"] = {}
        for tampered in (broken_edge, no_evidence, bad_status, not_a_list):
            with pytest.raises(SnapshotError):
                from_dict(tampered)

    def test_from_dict_leaves_the_caller_s_data_alone_unless_consuming(self):
        data = to_dict(full_graph("jaffle-1.12"))
        from_dict(data)
        assert data["columns"] and data["edges"]
        from_dict(data, consume=True)
        assert "columns" not in data and "edges" not in data

    def test_unreadable_file(self, tmp_path):
        bad = tmp_path / "x.json"
        bad.write_text("{not json")
        with pytest.raises(SnapshotError, match="cannot read"):
            load_snapshot(bad)
        with pytest.raises(SnapshotError, match="cannot read"):
            load_snapshot(tmp_path / "missing.json")

    def test_keys_change_with_inputs(self):
        base = snapshot_key("m", "c", True)
        assert base != snapshot_key("m2", "c", True)
        assert base != snapshot_key("m", None, True)
        assert base != snapshot_key("m", "c", False)
        assert base == snapshot_key("m", "c", True) and base.isalnum()

    def test_file_hash(self, tmp_path):
        path = tmp_path / "x"
        path.write_bytes(b"abc" * 1_000_000)
        import hashlib

        assert file_sha256(path) == hashlib.sha256(b"abc" * 1_000_000).hexdigest()


class TestCache:
    def test_get_put_and_prune(self, tmp_path):
        graph = full_graph("jaffle-1.12")
        cache = SnapshotCache(tmp_path, keep=2)
        assert cache.get("a1") is None
        for key in ("a1", "b2", "c3"):
            cache.put(key, graph)
        assert sorted(p.name for p in tmp_path.iterdir()) == ["b2.json", "c3.json"]
        assert_same(graph, cache.get("c3"))

    def test_a_corrupt_entry_is_dropped(self, tmp_path):
        cache = SnapshotCache(tmp_path)
        cache.path("bad").write_text("{")
        assert cache.get("bad") is None
        assert not cache.path("bad").exists()

    def test_keys_cannot_name_other_files(self, tmp_path):
        with pytest.raises(ValueError):
            SnapshotCache(tmp_path).path("../escape")


@pytest.fixture
def project(tmp_path):
    """A dbt project directory with target/manifest.json and catalog.json."""
    target = tmp_path / "project" / "target"
    target.mkdir(parents=True)
    for name in ("manifest.json", "catalog.json"):
        shutil.copy(DBT_FIXTURES / "jaffle-1.12" / name, target / name)
    return tmp_path / "project"


class TestIndexer:
    def test_locate_artifacts(self, project, tmp_path):
        assert locate_artifacts(project) == (
            project / "target" / "manifest.json",
            project / "target" / "catalog.json",
        )
        bare = tmp_path / "bare"
        assert locate_artifacts(bare) == (bare / "target" / "manifest.json", None)

    def test_cold_then_warm(self, project, tmp_path):
        cache = SnapshotCache(tmp_path / "cache")
        cold = index_project(project, cache=cache)
        assert (cold.status, cold.from_cache) == (IndexStatus.OK, False)
        assert cold.project_name == "jaffle_shop" and cold.schema_version == 12
        assert set(cold.seconds) == {"parse", "graph", "column_lineage", "snapshot", "total"}
        assert cold.snapshot.is_file()

        warm = index_project(project, cache=cache)
        assert (warm.status, warm.from_cache, warm.snapshot) == (
            IndexStatus.OK,
            True,
            cold.snapshot,
        )
        assert (warm.project_name, warm.dbt_version, warm.schema_version) == (
            "jaffle_shop",
            "1.12.6",
            12,
        )
        assert_same(cold.graph, warm.graph)

    def test_a_changed_manifest_is_not_served_from_cache(self, project, tmp_path):
        cache = SnapshotCache(tmp_path / "cache")
        first = index_project(project, cache=cache)
        manifest = project / "target" / "manifest.json"
        manifest.write_bytes(manifest.read_bytes() + b"\n")
        second = index_project(project, cache=cache)
        assert not second.from_cache and second.snapshot != first.snapshot

    def test_without_cache_or_column_lineage(self, project):
        result = index_project(project, column_lineage=False)
        assert result.status is IndexStatus.OK and result.snapshot is None
        assert result.graph.column_lineage == {}
        assert "column_lineage" not in result.seconds

    def test_missing_manifest_is_unknown(self, tmp_path):
        result = index_project(tmp_path)
        assert result.status is IndexStatus.UNKNOWN and result.graph is None
        assert "run dbt parse" in result.errors[0].message

    def test_untrusted_artifacts_are_unknown_and_not_cached(self, project, tmp_path):
        (project / "target" / "manifest.json").write_text('{"nodes": []}')
        cache = SnapshotCache(tmp_path / "cache")
        result = index_project(project, cache=cache)
        assert result.status is IndexStatus.UNKNOWN and result.graph is None
        assert any("structure check failed" in e.message for e in result.errors)
        assert not (tmp_path / "cache").exists()

    def test_compiled_sql_from_target_compiled(self, project):
        """Without compiled_code, lineage reads target/compiled under the project."""
        manifest_path = project / "target" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        node = manifest["nodes"]["model.jaffle_shop.stg_orders"]
        compiled = project / node["compiled_path"]
        compiled.parent.mkdir(parents=True, exist_ok=True)
        compiled.write_text(node.pop("compiled_code"))
        manifest_path.write_text(json.dumps(manifest))
        result = index_project(project)
        assert result.graph.column_lineage["model.jaffle_shop.stg_orders"].state.value == "resolved"


def run(*argv) -> tuple[int, str]:
    out = io.StringIO()
    return main(list(argv), out=out), out.getvalue()


class TestCli:
    def test_index_text_and_cache(self, project):
        code, out = run("index", "--project-dir", str(project))
        assert code == 0
        assert out.splitlines()[0] == "Indexed jaffle_shop (dbt 1.12.6, manifest schema v12)"
        assert "Column lineage: 13 resolved" in out
        assert (project / "target" / ".lineagepilot").is_dir()
        code, out = run("index", "--project-dir", str(project))
        assert "(from the snapshot cache)" in out.splitlines()[0]

    def test_index_json(self, project):
        code, out = run("index", "--project-dir", str(project), "--json", "--no-cache")
        data = json.loads(out)
        assert (code, data["status"], data["project"], data["from_cache"]) == (
            0,
            "ok",
            "jaffle_shop",
            False,
        )
        assert data["stats"]["node_kinds"]["model"] == 13
        assert data["column_lineage"] == {"resolved": 13}
        assert not (project / "target" / ".lineagepilot").exists()

    def test_index_unknown(self, tmp_path):
        code, out = run("index", "--project-dir", str(tmp_path), "--no-cache")
        assert code == 1 and out.startswith("Index unknown")
        code, out = run("index", "--project-dir", str(tmp_path), "--no-cache", "--json")
        assert code == 1 and json.loads(out)["status"] == "unknown"

    def test_index_warnings_are_shown(self, project):
        manifest = project / "target" / "manifest.json"
        data = json.loads(manifest.read_text())
        data["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v13.json"
        manifest.write_text(json.dumps(data))
        code, out = run("index", "--project-dir", str(project), "--no-cache")
        assert code == 0 and "warning: Manifest schema v13 is newer" in out

    def test_impact_of_a_column(self, project):
        code, out = run(
            "impact", "stg_orders#order_total", "--project-dir", str(project), "--no-cache"
        )
        assert code == 0
        lines = out.splitlines()
        assert lines[0] == "Impact of model.jaffle_shop.stg_orders#order_total"
        assert lines[1] == "  2 impacted: 2 model"
        assert "model.jaffle_shop.orders" in lines[2] and "[order_total]" in lines[2]

    def test_impact_json_by_file(self, project):
        code, out = run(
            "impact",
            "models/staging/stg_orders.sql",
            "--project-dir",
            str(project),
            "--json",
            "--max-depth",
            "1",
            "--no-cache",
        )
        data = json.loads(out)
        assert code == 0 and data["targets"] == ["model.jaffle_shop.stg_orders"]
        assert {n["id"] for n in data["nodes"]} == {
            "model.jaffle_shop.orders",
            "model.jaffle_shop.order_items",
        }
        assert data["truncated"] is True

    def test_impact_with_nothing_downstream_and_unknowns(self, project):
        code, out = run(
            "impact",
            "customers",
            "--project-dir",
            str(project),
            "--no-cache",
            "--no-column-lineage",
        )
        assert code == 0 and "Nothing downstream." in out

    def test_impact_reports_unknowns(self, project):
        manifest = project / "target" / "manifest.json"
        data = json.loads(manifest.read_text())
        data["nodes"]["model.jaffle_shop.orders"]["depends_on"]["nodes"].append("metric.x.y")
        manifest.write_text(json.dumps(data))
        code, out = run("impact", "stg_orders", "--project-dir", str(project), "--no-cache")
        assert "Unknowns (1):" in out
        assert "model.jaffle_shop.orders: metric.x.y: metric nodes are not indexed" in out

    def test_impact_problems(self, project, tmp_path):
        code, out = run(
            "impact", "nope", "stg_orders#nope", "--project-dir", str(project), "--no-cache"
        )
        assert code == 1
        assert out.splitlines() == [
            "nope: no node, model or file by that name",
            "stg_orders#nope: model.jaffle_shop.stg_orders has no column 'nope'",
        ]
        code, out = run("impact", "x", "--project-dir", str(tmp_path), "--no-cache", "--json")
        assert code == 1 and json.loads(out)["status"] == "unknown"
        code, out = run("impact", "nope", "--project-dir", str(project), "--no-cache", "--json")
        assert json.loads(out) == {"errors": ["nope: no node, model or file by that name"]}

    def test_usage_errors_and_version(self, capsys):
        assert run()[0] == 2
        assert run("impact")[0] == 2
        assert run("--version")[0] == 0
        assert f"lineagepilot {__version__}" in capsys.readouterr().out


def test_resolve_targets_by_absolute_path_and_ambiguous_names(project, monkeypatch):
    graph = index_project(project).graph
    absolute = str((project / "models" / "staging" / "stg_orders.sql").resolve())
    monkeypatch.chdir(project)
    assert resolve_targets(graph, [absolute], project)[0] == ["model.jaffle_shop.stg_orders"]
    assert resolve_targets(graph, ["models/staging/stg_orders.sql"], project)[0] == [
        "model.jaffle_shop.stg_orders"
    ]
    # raw_customers names a seed and a source
    ids, problems = resolve_targets(graph, ["raw_customers", "raw_customers#id"], project)
    assert ids == ["seed.jaffle_shop.raw_customers", "source.jaffle_shop.ecom.raw_customers"]
    assert problems == [
        "raw_customers#id: raw_customers names 2 nodes: "
        "seed.jaffle_shop.raw_customers, source.jaffle_shop.ecom.raw_customers"
    ]


def test_untrusted_parse_result_has_no_graph():
    result = ParseResult()
    result.nodes.append(RawNode("model.p.a", "model", "a"))
    result.add_error("bad", severity=ParseErrorSeverity.ERROR)
    with pytest.raises(UntrustedParseResult, match="could not be trusted"):
        build_dbt_graph(result)
