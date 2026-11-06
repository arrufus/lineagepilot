"""The Change Impact Review on real dbt artifacts: fixtures/impact-shop, before and after a
column rename, both written by dbt (tasks 4.2 and 4.3, design sections 8 and 14)."""

from __future__ import annotations

import json
import shutil
import sys

import pytest
from support import DBT_FIXTURES, IMPACT_SHOP, RENAME, apply_rename, copy_impact_shop

from lineagepilot_core.analysis.baseline import ManifestBaseline, load_baseline_manifest
from lineagepilot_core.analysis.review import (
    CATALOG_NOTE,
    COLUMNS_OFF_NOTE,
    COMPUTING_NOTE,
    BaseRef,
    ChangedFile,
    FileStatus,
    baseline_inputs,
    review_changes,
)
from lineagepilot_core.graph.model import (
    AirflowSummary,
    ColumnLineageStatus,
    Confidence,
    EdgeKind,
    Evidence,
    LineageState,
    Node,
    NodeKind,
    Origin,
)
from lineagepilot_core.indexer import index_project
from lineagepilot_core.project import read_project

S = "model.impact_shop"
ORDERS_SQL = "models/marts/orders.sql"
ORDERS_YML = "models/marts/orders.yml"
MODIFIED = FileStatus.MODIFIED


@pytest.fixture
def root(tmp_path):
    return copy_impact_shop(tmp_path / "impact_shop")


def graph_of(root, **kwargs):
    result = index_project(root, **kwargs)
    assert result.graph is not None, result.errors
    return result.graph


def changed(*paths, status=MODIFIED):
    return [ChangedFile(path, status) for path in paths]


def review(root, files, graph=None, **kwargs):
    return review_changes(graph or graph_of(root), files, read_project(root), **kwargs)


def by_id(entries):
    return {entry["id"]: entry for entry in entries}


def unknowns_of(result, subject):
    return [u for u in result["unknowns"] if u["subject"] == subject]


@pytest.fixture
def renamed(root):
    apply_rename(root)
    return review(root, changed(ORDERS_SQL, ORDERS_YML), base=BaseRef("origin/main", "a" * 40))


class TestAcceptanceScenario:
    """Design section 14, the dbt half: rename a mart column."""

    def test_the_changed_column_is_found(self, renamed):
        assert [n["id"] for n in renamed["changedNodes"]] == [f"{S}.orders"]
        node = renamed["changedNodes"][0]
        assert node["status"] == MODIFIED.value
        assert node["owner"] == "finance-data"
        assert [(f["path"], f["role"]) for f in node["files"]] == [
            (ORDERS_SQL, "definition"),
            (ORDERS_YML, "properties"),
        ]
        columns = node["columns"]
        assert columns["state"] == "known" and columns["baseline"] == "catalog"
        assert columns["removed"] == ["order_total"] and columns["added"] == ["total_usd"]
        assert renamed["summary"]["changedColumns"] == 2

    def test_downstream_models_are_reached_through_the_changed_column(self, renamed):
        hits = by_id(renamed["impacted"])
        revenue = hits[f"{S}.customer_revenue"]
        assert revenue["reach"] == "column" and revenue["columns"] == ["lifetime_value"]
        assert revenue["confidence"] == "inferred" and revenue["origins"] == ["compiled_sql"]
        assert revenue["via"] == [
            {
                "source": f"{S}.orders#order_total",
                "target": f"{S}.customer_revenue#lifetime_value",
                "confidence": "inferred",
                "origins": ["compiled_sql"],
            }
        ]
        tiers = hits[f"{S}.revenue_tiers"]
        assert tiers["reach"] == "column" and tiers["depth"] == 2

    def test_a_model_that_reads_other_columns_is_still_listed_at_model_level(self, renamed):
        counts = by_id(renamed["impacted"])[f"{S}.order_counts"]
        assert counts["reach"] == "model" and counts["confidence"] == "inferred"
        assert "none of the columns known to have changed" in counts["reason"]

    def test_the_exposure_and_its_owner_are_listed(self, renamed):
        exposure = by_id(renamed["impacted"])["exposure.impact_shop.revenue_dashboard"]
        assert exposure["kind"] == "exposure" and exposure["owner"] == "Revenue Analytics"
        assert exposure["reason"] == "uses the whole node"

    def test_owners_come_from_meta_owner_or_group_and_nobody_is_lost(self, renamed):
        owners = {o["owner"]: o["nodes"] for o in renamed["owners"]}
        assert owners["growth-analytics"] == [f"{S}.customer_revenue"]
        assert owners["operations"] == [f"{S}.order_counts"]  # dbt group, no meta.owner
        assert owners[None] == [f"{S}.broken_report", f"{S}.revenue_tiers"]
        assert [o["owner"] for o in renamed["owners"]][-1] is None

    def test_the_unparseable_model_is_under_unknowns(self, renamed):
        hit = by_id(renamed["impacted"])[f"{S}.broken_report"]
        assert hit["reach"] == "model"
        assert hit["reason"].startswith("column lineage unknown: SQL could not be parsed")
        found = unknowns_of(renamed, f"{S}.broken_report")
        assert [u["reference"] for u in found] == ["column lineage"]
        assert "could not be parsed" in found[0]["reason"]
        assert "\x1b" not in found[0]["reason"], "sqlglot's terminal escapes must not get through"

    def test_airflow_is_said_not_to_be_analysed(self, renamed):
        assert renamed["airflow"]["analysed"] is False
        assert renamed["airflow"]["reason"] == "the DAG files were not analysed"

    def test_the_summary_adds_up(self, renamed):
        summary = renamed["summary"]
        assert summary["impacted"] == {"exposure": 1, "model": 4}
        assert summary["impactedByColumn"] == 2
        assert summary["unknowns"] == len(renamed["unknowns"]) == 1
        assert summary["changedFiles"] == 2 and summary["changedNodes"] == 1
        assert renamed["base"] == {"ref": "origin/main", "commit": "a" * 40}
        assert renamed["notes"] == [CATALOG_NOTE]


class TestFilesToNodes:
    def test_a_properties_file_alone_is_listed_but_not_followed(self, root):
        result = review(root, changed(ORDERS_YML))
        node = result["changedNodes"][0]
        assert node["status"] == "properties" and node["columns"]["state"] == "not_compared"
        assert result["impacted"] == []
        said = unknowns_of(result, f"{S}.orders")
        assert said[0]["reference"] == "properties" and ORDERS_YML in said[0]["reason"]

    def test_one_properties_file_touches_every_node_it_documents(self, root):
        result = review(root, changed("models/marts/marts.yml"))
        ids = [n["id"] for n in result["changedNodes"]]
        assert ids == [
            f"{S}.customer_revenue",
            f"{S}.order_counts",
            f"{S}.revenue_tiers",
            "exposure.impact_shop.revenue_dashboard",
        ]
        assert result["impacted"] == []

    def test_a_sources_file_is_a_properties_change_too(self, root):
        result = review(root, changed("models/staging/__sources.yml"))
        assert {n["kind"] for n in result["changedNodes"]} == {"source"}
        assert result["impacted"] == []

    @pytest.mark.parametrize(
        ("path", "reason"),
        [
            ("macros/cents_to_dollars.sql", "a macro"),
            ("dbt_project.yml", "project configuration"),
            ("packages.yml", "project configuration"),
        ],
    )
    def test_a_file_that_can_change_anything_is_an_unknown(self, root, path, reason):
        result = review(root, changed(path))
        assert reason in unknowns_of(result, path)[0]["reason"]
        assert result["changedNodes"] == []

    def test_a_new_model_the_manifest_has_not_seen_is_an_unknown(self, root):
        path = "models/marts/brand_new.sql"
        result = review(root, changed(path, status=FileStatus.UNTRACKED))
        assert "not in the manifest yet" in unknowns_of(result, path)[0]["reason"]

    def test_a_deleted_file_no_artifact_knows_is_an_unknown(self, root):
        path = "models/marts/gone.sql"
        result = review(root, changed(path, status=FileStatus.DELETED))
        assert "neither the manifest nor a baseline" in unknowns_of(result, path)[0]["reason"]

    @pytest.mark.parametrize(
        "path", ["README.md", "models/marts/docs.md", "tests/check_orders.sql", ".github/ci.yml"]
    )
    def test_other_files_are_listed_and_nothing_more(self, root, path):
        result = review(root, changed(path))
        assert result["otherFiles"] == [{"path": path, "status": "modified"}]
        assert result["unknowns"] == [] and result["changedNodes"] == []

    def test_dbt_s_own_output_is_left_out(self, root):
        result = review(root, changed("target/manifest.json", "target/compiled/x.sql"))
        assert result["otherFiles"] == [] and result["summary"]["changedFiles"] == 0

    def test_files_outside_the_project_are_counted(self, root):
        assert review(root, [], outside_project=3)["outsideProject"] == 3

    def test_a_renamed_file_maps_through_its_old_path_on_an_old_manifest(self, root):
        moved = "models/marts/orders_mart.sql"
        shutil.move(root / ORDERS_SQL, root / moved)
        result = review(root, [ChangedFile(moved, FileStatus.RENAMED, ORDERS_SQL)])
        node = result["changedNodes"][0]
        assert node["id"] == f"{S}.orders" and node["status"] == "renamed"
        # The text has not changed, so the compiled SQL is still current
        assert node["columns"]["state"] == "known" and node["columns"]["removed"] == []

    def test_a_package_s_file_maps_by_its_path_inside_the_package(self, root):
        graph = graph_of(root)
        graph.nodes[f"{S}.orders"].package = "impact_shop"
        path = f"dbt_packages/impact_shop/{ORDERS_SQL}"
        result = review(root, changed(path), graph=graph)
        assert [n["id"] for n in result["changedNodes"]] == [f"{S}.orders"]

    def test_a_deleted_model_still_in_the_manifest_has_its_dependents_listed(self, root):
        (root / ORDERS_SQL).unlink()
        result = review(root, changed(ORDERS_SQL, status=FileStatus.DELETED))
        node = result["changedNodes"][0]
        assert node["status"] == "deleted" and node["columns"]["state"] == "not_compared"
        assert f"{S}.customer_revenue" in by_id(result["impacted"])


class TestColumnComparison:
    def test_a_file_changed_since_the_manifest_is_never_compared(self, root):
        # The change is not in the compiled SQL yet, so comparing would find nothing changed
        apply_rename(root, recompile=False)
        result = review(root, changed(ORDERS_SQL))
        columns = result["changedNodes"][0]["columns"]
        assert columns["state"] == "unknown" and columns["stale"] is True
        assert "run dbt compile" in columns["reason"]
        assert result["summary"]["staleNodes"] == 1
        assert unknowns_of(result, f"{S}.orders")[0]["reference"] == "column changes"
        # Nothing is lost: everything below is reached at model level
        hit = by_id(result["impacted"])[f"{S}.customer_revenue"]
        assert hit["reach"] == "model" and hit["confidence"] == "inferred"
        assert "changed columns are not known" in hit["reason"]

    def test_trailing_whitespace_does_not_make_a_file_stale(self, root):
        path = root / ORDERS_SQL
        path.write_text(path.read_text() + "\n\n  \n")
        assert review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]["state"] == "known"

    def test_a_windows_checkout_of_the_same_text_is_not_stale(self, root):
        # git on Windows checks files out with CRLF; this manifest was written where they are LF
        apply_rename(root)
        path = root / ORDERS_SQL
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
        assert b"\r\n" in path.read_bytes()
        columns = review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]
        assert columns["state"] == "known" and columns["stale"] is False
        assert columns["removed"] == ["order_total"] and columns["added"] == ["total_usd"]

    def test_a_manifest_written_on_windows_is_not_stale_for_a_unix_checkout(self, root):
        apply_rename(root)
        manifest = root / "target" / "manifest.json"
        data = json.loads(manifest.read_bytes())
        node = data["nodes"][f"{S}.orders"]
        node["raw_code"] = node["raw_code"].replace("\r\n", "\n").replace("\n", "\r\n")
        manifest.write_text(json.dumps(data))
        path = root / ORDERS_SQL
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n"))
        columns = review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]
        assert columns["state"] == "known" and columns["stale"] is False

    def test_a_real_edit_is_stale_whatever_its_line_endings(self, root):
        apply_rename(root)
        path = root / ORDERS_SQL
        text = path.read_bytes().replace(b"\r\n", b"\n").replace(b"total_usd", b"total_eur")
        path.write_bytes(text.replace(b"\n", b"\r\n"))
        columns = review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]
        assert columns["state"] == "unknown" and columns["stale"] is True

    def test_no_column_change_is_said_and_impact_is_still_listed(self, root):
        result = review(root, changed(ORDERS_SQL))
        columns = result["changedNodes"][0]["columns"]
        assert (columns["state"], columns["removed"], columns["added"]) == ("known", [], [])
        hit = by_id(result["impacted"])[f"{S}.customer_revenue"]
        assert hit["reach"] == "model"

    def test_the_baseline_manifest_is_preferred_to_the_catalog(self, root, tmp_path):
        apply_rename(root)
        path = tmp_path / "state" / "manifest.json"
        path.parent.mkdir()
        shutil.copy(IMPACT_SHOP / "target" / "manifest.json", path)
        graph = graph_of(root)
        files = changed(ORDERS_SQL)
        nodes, paths = baseline_inputs(graph, files, read_project(root))
        assert nodes == [f"{S}.orders"]
        baseline = load_baseline_manifest(path, nodes, paths)
        assert baseline.error is None
        result = review(root, files, graph=graph, baseline=baseline)
        columns = result["changedNodes"][0]["columns"]
        assert columns["baseline"] == "baseline_manifest"
        assert columns["removed"] == ["order_total"] and columns["added"] == ["total_usd"]
        assert CATALOG_NOTE not in result["notes"]
        recorded = result["artifacts"]["baselineManifest"]
        assert recorded["name"] == "manifest.json" and recorded["sha256"]

    def test_the_baseline_manifest_works_with_no_catalog_at_all(self, root, tmp_path):
        # A developer who ran dbt compile but never dbt docs generate
        apply_rename(root)
        (root / "target" / "catalog.json").unlink()
        path = tmp_path / "manifest.json"
        shutil.copy(IMPACT_SHOP / "target" / "manifest.json", path)
        baseline = load_baseline_manifest(path, [f"{S}.orders"])
        result = review(root, changed(ORDERS_SQL), baseline=baseline)
        columns = result["changedNodes"][0]["columns"]
        assert columns["removed"] == ["order_total"] and columns["added"] == ["total_usd"]
        # No column lineage without a catalog, so downstream is at model level
        assert by_id(result["impacted"])[f"{S}.customer_revenue"]["reach"] == "model"

    def test_a_node_the_baseline_does_not_have_is_all_new(self, root, tmp_path):
        apply_rename(root)
        baseline = ManifestBaseline(name="manifest.json")  # read fine, and has no such node
        columns = review(root, changed(ORDERS_SQL), baseline=baseline)["changedNodes"][0]["columns"]
        assert columns["baseline"] == "baseline_manifest"
        assert columns["removed"] == [] and "total_usd" in columns["added"]

    def test_a_baseline_with_no_compiled_sql_falls_back_to_the_catalog_and_says_so(
        self, root, tmp_path
    ):
        apply_rename(root)
        data = json.loads((IMPACT_SHOP / "target" / "manifest.json").read_bytes())
        for node in data["nodes"].values():
            node.pop("compiled_code", None)
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(data))
        baseline = load_baseline_manifest(path, [f"{S}.orders"])
        columns = review(root, changed(ORDERS_SQL), baseline=baseline)["changedNodes"][0]["columns"]
        assert columns["baseline"] == "catalog" and columns["removed"] == ["order_total"]
        assert "no compiled SQL" in columns["reason"]

    def test_a_baseline_that_cannot_be_read_falls_back_and_is_reported(self, root, tmp_path):
        apply_rename(root)
        baseline = load_baseline_manifest(tmp_path / "missing.json", [f"{S}.orders"])
        result = review(root, changed(ORDERS_SQL), baseline=baseline)
        assert result["changedNodes"][0]["columns"]["baseline"] == "catalog"
        said = unknowns_of(result, "missing.json")
        assert said[0]["reference"] == "baseline manifest" and "no file at" in said[0]["reason"]

    def test_no_baseline_at_all_marks_columns_unknown(self, root):
        apply_rename(root)
        (root / "target" / "catalog.json").unlink()
        data = json.loads((root / "target" / "manifest.json").read_bytes())
        columns = review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]
        assert data["nodes"][f"{S}.orders"]["compiled_code"]
        assert columns["state"] == "unknown" and columns["baseline"] == "none"
        assert "lineagePilot.baseline.manifestPath" in columns["reason"]

    def test_a_new_file_needs_no_baseline(self, root):
        apply_rename(root)
        (root / "target" / "catalog.json").unlink()
        result = review(root, changed(ORDERS_SQL, status=FileStatus.UNTRACKED))
        columns = result["changedNodes"][0]["columns"]
        assert columns["state"] == "known" and "total_usd" in columns["added"]
        assert columns["reason"] == "a new file"

    def test_a_removed_column_that_has_left_the_graph_falls_back_to_model_level(self, root):
        # The catalog was regenerated after the rename, so the old name is gone from the graph
        apply_rename(root)
        graph = graph_of(root)
        files = changed(ORDERS_SQL)
        baseline = ManifestBaseline(name="prod.json")
        baseline.nodes[f"{S}.orders"] = _baseline_node(columns=("order_id", "no_longer_anywhere"))
        result = review(root, files, graph=graph, baseline=baseline)
        assert result["changedNodes"][0]["columns"]["removed"] == ["no_longer_anywhere"]
        assert by_id(result["impacted"])[f"{S}.customer_revenue"]["reach"] == "model"

    def test_a_changed_type_is_a_changed_column(self, root):
        graph = graph_of(root)
        current = graph.columns[f"{S}.orders#order_total"].data_type
        assert current
        baseline = ManifestBaseline(name="prod.json")
        names = tuple(graph.output_columns[f"{S}.orders"])
        baseline.nodes[f"{S}.orders"] = _baseline_node(
            columns=names, types={"order_total": "VARCHAR", "order_id": "NOT-CHECKED-WITH-CASE"}
        )
        graph.columns[f"{S}.orders#order_id"].data_type = "not-checked-with-case"
        result = review(root, changed(ORDERS_SQL), graph=graph, baseline=baseline)
        columns = result["changedNodes"][0]["columns"]
        assert columns["typeChanged"] == [
            {"name": "order_total", "before": "VARCHAR", "after": current}
        ]
        assert by_id(result["impacted"])[f"{S}.customer_revenue"]["reach"] == "column"

    @pytest.mark.parametrize(
        ("mutate", "reason"),
        [
            (lambda n: setattr(n, "kind", NodeKind.SEED), "a seed has no compiled SQL"),
            (lambda n: setattr(n, "source_sha256", None), "no source text to check"),
        ],
    )
    def test_what_cannot_be_compared_is_unknown(self, root, mutate, reason):
        graph = graph_of(root)
        mutate(graph.nodes[f"{S}.orders"])
        columns = review(root, changed(ORDERS_SQL), graph=graph)["changedNodes"][0]["columns"]
        assert columns["state"] == "unknown" and reason in columns["reason"]

    def test_an_unparseable_changed_model_says_why_its_columns_are_unknown(self, root):
        path = "models/marts/broken_report.sql"
        columns = review(root, changed(path))["changedNodes"][0]["columns"]
        assert columns["state"] == "unknown" and "could not be parsed" in columns["reason"]

    def test_a_file_that_cannot_be_read_is_unknown(self, root):
        (root / ORDERS_SQL).write_bytes(b"\xff\xfe not utf-8")
        columns = review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]
        assert columns["state"] == "unknown" and "cannot be read" in columns["reason"]

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX devices and symlinks")
    @pytest.mark.parametrize("target", ["/dev/zero", "/etc/hosts"])
    def test_a_symlink_out_of_the_project_is_never_read(self, root, target):
        # /dev/zero reports size 0 and never ends: reading it would hang the language server
        path = root / ORDERS_SQL
        path.unlink()
        path.symlink_to(target)
        columns = review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]
        assert columns["state"] == "unknown"
        assert "not a regular file inside the project" in columns["reason"]

    def test_a_huge_file_is_not_read(self, root, monkeypatch):
        monkeypatch.setattr("lineagepilot_core.analysis.review.MAX_SOURCE_BYTES", 10)
        columns = review(root, changed(ORDERS_SQL))["changedNodes"][0]["columns"]
        assert "too large" in columns["reason"]


class TestBaselineFromDeletedFiles:
    def test_a_model_gone_from_the_manifest_is_named_by_the_baseline(self, root, tmp_path):
        # Deleted and recompiled: only the baseline still knows the file defined a model
        data = json.loads((root / "target" / "manifest.json").read_bytes())
        gone = "models/marts/order_counts.sql"
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(data))
        del data["nodes"][f"{S}.order_counts"]
        data["parent_map"].pop(f"{S}.order_counts")
        data["child_map"].pop(f"{S}.order_counts")
        data["child_map"][f"{S}.orders"].remove(f"{S}.order_counts")
        (root / "target" / "manifest.json").write_text(json.dumps(data))
        (root / gone).unlink()

        graph = graph_of(root)
        files = changed(gone, status=FileStatus.DELETED)
        nodes, paths = baseline_inputs(graph, files, read_project(root))
        assert nodes == [] and paths == [gone]
        baseline = load_baseline_manifest(path, nodes, paths)
        assert baseline.files == {gone: [f"{S}.order_counts"]}
        result = review(root, files, graph=graph, baseline=baseline)
        node = result["changedNodes"][0]
        assert node["id"] == f"{S}.order_counts" and node["status"] == "deleted"
        assert node["owner"] == "operations" and result["impacted"] == []
        assert unknowns_of(result, gone) == []


class TestManifestBaselineFile:
    def test_it_survives_the_trip_from_the_indexer_process(self):
        baseline = load_baseline_manifest(
            IMPACT_SHOP / "target" / "manifest.json", [f"{S}.orders", f"{S}.not_there"]
        )
        assert set(baseline.nodes) == {f"{S}.orders"}
        node = baseline.nodes[f"{S}.orders"]
        assert node.columns == ("order_id", "customer_id", "order_total", "ordered_at")
        assert node.owner == "finance-data" and node.types  # catalog.json beside it
        through = ManifestBaseline.from_json(json.loads(json.dumps(baseline.to_json())))
        assert through == baseline

    def test_it_uses_no_catalog_when_none_is_beside_it(self, tmp_path):
        path = tmp_path / "manifest.json"
        shutil.copy(RENAME / "target" / "manifest.json", path)
        baseline = load_baseline_manifest(path, [f"{S}.orders"])
        assert baseline.nodes[f"{S}.orders"].columns[2] == "total_usd"

    def test_a_star_it_cannot_expand_says_so(self, tmp_path):
        data = json.loads((IMPACT_SHOP / "target" / "manifest.json").read_bytes())
        data["nodes"][f"{S}.orders"]["compiled_code"] = 'select * from "impact_shop"."main"."x"'
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(data))
        node = load_baseline_manifest(path, [f"{S}.orders"]).nodes[f"{S}.orders"]
        assert node.columns is None and "could not be expanded" in node.reason

    def test_a_manifest_that_is_too_large_is_refused(self):
        path = IMPACT_SHOP / "target" / "manifest.json"
        baseline = load_baseline_manifest(path, [], max_artifact_bytes=10)
        assert "over the" in baseline.error and baseline.nodes == {}

    def test_a_manifest_that_cannot_be_trusted_is_reported(self, tmp_path):
        path = tmp_path / "manifest.json"
        path.write_text("{ not json")
        baseline = load_baseline_manifest(path, [])
        assert "cannot be trusted" in baseline.error

    def test_a_path_that_cannot_be_read_is_reported(self, tmp_path):
        baseline = load_baseline_manifest(tmp_path / ("x" * 5000), [])
        assert baseline.error


class TestStages:
    def test_the_stage_is_recorded_with_a_note(self, root):
        assert COMPUTING_NOTE in review(root, [], column_stage="computing")["notes"]
        assert COLUMNS_OFF_NOTE in review(root, [], column_stage="off")["notes"]

    def test_a_status_callable_computes_on_demand(self, root):
        graph = graph_of(root)
        names = graph.output_columns.pop(f"{S}.orders")
        asked = []

        def on_demand(node_id):
            asked.append(node_id)
            graph.output_columns[f"{S}.orders"] = names
            return ColumnLineageStatus(LineageState.RESOLVED)

        result = review(root, changed(ORDERS_SQL), graph=graph, column_lineage=on_demand)
        assert asked[0] == f"{S}.orders"
        assert result["changedNodes"][0]["columns"]["state"] == "known"

    def test_with_no_output_columns_the_reason_is_given(self, root):
        graph = graph_of(root, column_lineage=False)
        columns = review(root, changed(ORDERS_SQL), graph=graph)["changedNodes"][0]["columns"]
        assert columns["state"] == "unknown"
        assert "column lineage has not reached it" in columns["reason"]

    def test_airflow_counts_as_analysed_once_the_graph_has_it(self, root):
        graph = graph_of(root)
        graph.airflow = AirflowSummary(analysed=True, dag_files=1)
        task = graph.add_node(Node("airflow:d.t", NodeKind.AIRFLOW_TASK, "t", owner="etl"))
        graph.add_edge(
            EdgeKind.ORCHESTRATES,
            task.id,
            f"{S}.customer_revenue",
            Evidence(Origin.DAG_STATIC, Confidence.HEURISTIC),
        )
        result = review(root, changed(ORDERS_SQL), graph=graph)
        assert result["airflow"]["analysed"] is True
        hit = by_id(result["impacted"])["airflow:d.t"]
        assert hit["kind"] == "airflow_task" and hit["confidence"] == "heuristic"
        assert {"owner": "etl", "nodes": ["airflow:d.t"]} in result["owners"]


def test_the_source_hash_means_the_same_on_dbt_core_and_fusion():
    # The check rests on raw_code. Fusion's checksum differs from the file's as Core's does, but
    # the two engines read the same customers.sql to the same raw_code
    hashes = {
        index_project(DBT_FIXTURES / name, manifest=DBT_FIXTURES / name / "manifest.json")
        .graph.nodes["model.jaffle_shop.customers"]
        .source_sha256
        for name in ("jaffle-1.12", "jaffle-fusion-2.0")
    }
    assert len(hashes) == 1 and None not in hashes


def test_snapshots_have_no_source_text_to_check():
    graph = index_project(
        DBT_FIXTURES / "synth-24", manifest=DBT_FIXTURES / "synth-24" / "manifest.json"
    ).graph
    assert all(n.source_sha256 for n in graph.nodes.values() if n.kind is NodeKind.MODEL)
    assert not any(n.source_sha256 for n in graph.nodes.values() if n.kind is NodeKind.SEED)


def _baseline_node(columns=None, types=None):
    from lineagepilot_core.analysis.baseline import BaselineNode

    return BaselineNode(f"{S}.orders", "model", "orders", columns=columns, types=types or {})


def test_project_paths_are_kept_per_key(root):
    project = read_project(root)
    assert project.is_under("macros/x.sql", "macro-paths")
    assert not project.is_under("models/x.sql", "macro-paths")
    assert project.is_under("models/x.sql", "model-paths")
    assert not project.is_under("models/x.sql", "unknown-key")
