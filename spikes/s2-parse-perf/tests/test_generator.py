"""Shape of the synthetic dbt project generator (no dbt needed)."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import gen_project  # noqa: E402

REF = re.compile(r"ref\('([^']+)'\)")
SRC = re.compile(r"source\('([^']+)', '([^']+)'\)")


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    out = tmp_path_factory.mktemp("gen") / "synth-120"
    shape = gen_project.generate(120, out, seed=7)
    return out, shape


def _models(out: Path) -> tuple[dict[str, str], dict[str, dict]]:
    sql = {p.stem: p.read_text() for p in (out / "models").rglob("*.sql")}
    props: dict[str, dict] = {}
    for y in (out / "models").rglob("*.yml"):
        doc = yaml.safe_load(y.read_text())
        for m in doc.get("models", []):
            props[m["name"]] = m
    return sql, props


def test_counts(project):
    out, shape = project
    sql, props = _models(out)
    enabled = {k: v for k, v in sql.items() if "enabled=false" not in v}
    assert len(enabled) == shape["models"] == 120
    assert shape["by_layer"]["staging"] == shape["sources"]
    assert shape["exposures"] >= 1 and shape["yaml_tests"] > 200


def test_layer_order_and_fan_in(project):
    out, _ = project
    sql, props = _models(out)
    names = set(props) | {"country_codes", "currency_rates"}
    for name, text in sql.items():
        if "enabled=false" in text:
            continue
        refs = REF.findall(text)
        srcs = SRC.findall(text)
        base = name[:-3] if re.search(r"_v\d$", name) else name
        assert 1 <= len(refs) + len(srcs) <= 4, name
        assert all(r in names for r in refs), name
        if base.startswith("stg_"):
            assert len(srcs) == 1 and not refs
        if base.startswith("int_"):
            assert not any(r.startswith(("fct_", "dim_")) for r in refs)  # marts never feed intermediate


def test_columns_documented_and_in_range(project):
    out, _ = project
    sql, props = _models(out)
    for name, m in props.items():
        cols = m["columns"]
        assert 10 <= len(cols) <= 30, name
        assert all(c.get("description") for c in cols)
        assert len({c["name"] for c in cols}) == len(cols)


def test_generic_tests_present(project):
    out, _ = project
    _, props = _models(out)
    kinds = set()
    for m in props.values():
        pk = m["columns"][0]
        assert {"unique", "not_null"} <= set(t for t in pk["data_tests"] if isinstance(t, str))
        for c in m["columns"]:
            for t in c.get("data_tests", []):
                kinds.add(t if isinstance(t, str) else next(iter(t)))
    assert {"unique", "not_null", "relationships"} <= kinds


def test_owner_exposures_versions(project):
    out, _ = project
    _, props = _models(out)
    owners = sum(1 for m in props.values() if (m.get("config", {}).get("meta") or {}).get("owner"))
    assert owners > len(props) * 0.3
    assert sum(1 for m in props.values() if m.get("versions")) == 1
    exposures = [e for y in (out / "models").rglob("*.yml") for e in (yaml.safe_load(y.read_text()).get("exposures") or [])]
    assert exposures and all(e["depends_on"] and e["owner"]["email"] for e in exposures)
    assert "+meta" in (out / "dbt_project.yml").read_text()


def test_raw_tables_cover_every_source(project):
    out, _ = project
    ddl = (out / "raw_tables.sql").read_text()
    sql, _ = _models(out)
    for text in sql.values():
        for system, table in SRC.findall(text):
            assert f"raw_{system}.{table} (" in ddl


def test_deterministic(tmp_path):
    def digest(p: Path) -> str:
        h = hashlib.sha256()
        for f in sorted(p.rglob("*")):
            if f.is_file():
                h.update(f.relative_to(p).as_posix().encode() + f.read_bytes())
        return h.hexdigest()
    gen_project.generate(40, tmp_path / "a", seed=5)
    gen_project.generate(40, tmp_path / "b", seed=5)
    assert digest(tmp_path / "a") == digest(tmp_path / "b")


def test_v2_yaml_style(tmp_path):
    gen_project.generate(30, tmp_path / "v2", seed=5, yaml_style="v2")
    text = "\n".join(p.read_text() for p in (tmp_path / "v2" / "models").rglob("*.yml"))
    assert "arguments:" in text
    srcs = [yaml.safe_load(p.read_text())["sources"][0] for p in (tmp_path / "v2" / "models").rglob("*__sources.yml")]
    assert all("freshness" in s["config"] and "freshness" not in s for s in srcs)
