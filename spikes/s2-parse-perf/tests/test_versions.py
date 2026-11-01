"""Version detection and per-manifest checks, on the checked-in real fixtures."""

from __future__ import annotations

import json

from spike import versions as V
from tests.conftest import REAL_FIXTURES


def test_schema_number():
    assert V.schema_number("https://schemas.getdbt.com/dbt/manifest/v12.json") == 12
    assert V.schema_number("https://schemas.getdbt.com/dbt/manifest/v20.json") == 20
    assert V.schema_number(None) is None and V.schema_number("junk") is None


def test_core_and_fusion_both_report_v12():
    for name, (m, _) in REAL_FIXTURES.items():
        meta = json.loads(m.read_bytes())["metadata"]
        assert V.schema_number(meta["dbt_schema_version"]) == 12, name
    fusion = json.loads(REAL_FIXTURES["jaffle-fusion-2.0"][0].read_bytes())["metadata"]
    assert fusion["dbt_version"].startswith("2.")


def test_parser_checks_on_real_fixtures():
    for name, (m, c) in REAL_FIXTURES.items():
        res = V.parser_checks(m, c if c and c.exists() else None)
        assert res["edges_match_depends_on"], name
        assert res["parse_error_count"] == 0, name
        assert res["models_has_tests_true"] == 0 < res["models_with_attached_tests_truth"], name


def test_fusion_columns_lack_data_type():
    """Fusion manifests carry no column data_type; Core 1.12 includes the key (null unless declared)."""
    fus = V.key_profile(json.loads(REAL_FIXTURES["jaffle-fusion-2.0"][0].read_bytes()))
    core = V.key_profile(json.loads(REAL_FIXTURES["jaffle-1.12"][0].read_bytes()))
    assert "data_type" in core["column"] and "data_type" not in fus["column"]
    assert "compiled_code" in core["model"]


def test_analyse_on_fixture_tree(tmp_path):
    for name, (m, c) in REAL_FIXTURES.items():
        d = tmp_path / name / "1.12" / "docs"
        d.mkdir(parents=True)
        (d / "manifest.json").write_bytes(m.read_bytes())
    res = V.analyse(tmp_path)
    assert len(res["manifests"]) == 3
    assert {r["schema_number"] for r in res["manifests"]} == {12}


def test_unknown_schema_version_fails_silently(mods, tmp_path):
    """Finding: DbtParser never reads dbt_schema_version. A manifest from a
    future schema that renamed a field parses 'successfully' with no edges and
    no ParseError, so Phase 1 needs an explicit version gate."""
    from tests.conftest import parse
    m, c = REAL_FIXTURES["synth-24"]
    man = json.loads(m.read_bytes())
    man["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v99.json"
    for n in man["nodes"].values():
        n["parents"] = n.pop("depends_on", {})
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(man))
    r = parse(mods, p)
    assert r.capsules and r.edges == [] and r.errors == []


def test_column_edges_never_produced_from_real_manifests(mods):
    """Finding: _build_column_lineage reads depends_on.columns, catalog_col_lineage
    and columns[].depends_on. No real Core or Fusion manifest has them."""
    from tests.conftest import parse
    for name, (m, c) in REAL_FIXTURES.items():
        assert parse(mods, m, c).column_edges == [], name
