"""DAG files are never imported or executed; dynamic patterns come out as unknown; parse failures are records."""
from __future__ import annotations

import builtins
import importlib
import os
import runpy
import subprocess
import sys
from pathlib import Path

import pytest

from lineagepilot_spike_dag import scanner as scanner_mod
from lineagepilot_spike_dag.scanner import scan

from .conftest import FIXTURES, find_link, short

SIDE_EFFECT = FIXTURES / "dags" / "side_effect_dag.py"
SRC = Path(__file__).resolve().parent.parent / "src"


def test_fixture_side_effect_is_real(tmp_path):
    """Control: executing the fixture does write the marker, so the next tests are not vacuous."""
    marker = tmp_path / "marker"
    env = {**os.environ, "LP_SIDE_EFFECT_MARKER": str(marker)}
    res = subprocess.run([sys.executable, "-I", str(SIDE_EFFECT)], env=env, capture_output=True, text=True, cwd=tmp_path)
    assert res.returncode != 0 and marker.exists()


def test_scan_never_imports_or_executes_dag_files(tmp_path, monkeypatch):
    marker = tmp_path / "marker"
    monkeypatch.setenv("LP_SIDE_EFFECT_MARKER", str(marker))
    imported: list[str] = []

    class Spy:
        def find_spec(self, name, path=None, target=None):
            imported.append(name)
            return None

    def forbidden(*a, **k):
        raise AssertionError("the scanner must not exec/eval/run user code")

    spy = Spy()
    sys.meta_path.insert(0, spy)
    monkeypatch.setattr(builtins, "exec", forbidden)
    monkeypatch.setattr(builtins, "eval", forbidden)
    monkeypatch.setattr(runpy, "run_path", forbidden)
    monkeypatch.setattr(importlib, "import_module", forbidden)
    try:
        result = scan([str(FIXTURES / "dags")], workspace=str(FIXTURES))
    finally:
        sys.meta_path.remove(spy)
    assert not marker.exists()
    fixture_modules = {"side_effect_dag", "cosmos_patterns", "shell_patterns", "include", "airflow", "cosmos", "dbt_wrapper"}
    assert not [m for m in imported if m.split(".")[0] in fixture_modules]
    assert not any(getattr(m, "__file__", "") and str(FIXTURES) in str(m.__file__) for m in list(sys.modules.values()))
    link = find_link(result, "side_effect", "t")
    assert short(link["resolved_models"]) == ["stg_orders"]
    assert result["scanner"]["executes_user_code"] is False


def test_cli_subprocess_never_executes(tmp_path):
    marker = tmp_path / "marker"
    env = {**os.environ, "LP_SIDE_EFFECT_MARKER": str(marker), "PYTHONPATH": str(SRC)}
    out = tmp_path / "out.json"
    res = subprocess.run([sys.executable, "-m", "lineagepilot_spike_dag", "scan", str(SIDE_EFFECT), "--workspace", str(FIXTURES),
                          "-o", str(out)], env=env, capture_output=True, text=True, cwd=tmp_path)
    assert res.returncode == 0, res.stderr
    assert not marker.exists() and '"side_effect"' in out.read_text()


def test_dynamic_loops_comprehensions_and_manifest_reads(fixture_scan):
    reasons = [u["reason"] for u in fixture_scan["unresolved"] if u["file"].endswith("dynamic_patterns.py")]
    assert any("for-loop" in r for r in reasons)
    assert any("comprehension" in r for r in reasons)
    assert any("manifest.json at DAG-parse time" in r for r in reasons)
    assert any("never called" in r for r in reasons)
    assert any("dependency operand" in r for r in reasons)
    assert not any(l["status"] == "resolved" and l["dag_id"] == "loop_dag" for l in fixture_scan["links"])
    factory = [l for l in fixture_scan["links"] if l["location"]["file"].endswith("dynamic_patterns.py") and l.get("group_id") == "g"]
    assert factory and all(l["status"] == "unknown" and l["dag_id"] is None for l in factory)


def test_helper_called_with_literals_is_inlined(fixture_scan):
    h1 = find_link(fixture_scan, "helper_dag", "h1")
    h2 = find_link(fixture_scan, "helper_dag", "h2")
    assert short(h1["resolved_models"]) == ["stg_orders"] and short(h2["resolved_models"]) == ["stg_payments"]
    assert h1["location"]["line"] == 39 and h1["constructed_at"]["line"] == 35 and h1["via_call_at"] == [[39, 8]]
    assert not any(l.get("task_id") == "never" and l["status"] == "resolved" for l in fixture_scan["links"])


def test_main_guard_is_skipped(fixture_scan):
    assert not any("hd.test" in u["reason"] for u in fixture_scan["unresolved"])


def test_parse_failures_and_discovery(fixture_scan, tmp_path, monkeypatch):
    syntax = [u for u in fixture_scan["unresolved"] if u["file"].endswith("syntax_error.py")]
    assert syntax and syntax[0]["reason"].startswith("SyntaxError") and syntax[0]["line"] == 3
    whys = {s["path"]: s["why"] for s in fixture_scan["skipped"]}
    assert "safe-mode" in whys["plain_module.py"] and whys["ignored/hidden_dag.py"] == ".airflowignore"
    monkeypatch.setattr(scanner_mod, "FILE_SIZE_CAP", 10)
    big = scan([str(FIXTURES / "dags" / "shell_patterns.py")], workspace=str(FIXTURES))
    assert big["links"] == [] and "over the 10-byte cap" in big["unresolved"][0]["reason"]
    deep = tmp_path / "deep_dag.py"
    deep.write_text("# airflow dag\nx = " + "(" * 400 + "1" + ")" * 400 + "\n")
    res = scan([str(deep)], workspace=str(tmp_path))
    assert res["links"] == []  # deeply nested input must not crash the scanner
    all_files = scan([str(FIXTURES / "dags")], workspace=str(FIXTURES), safe_mode=False)
    assert "dags/plain_module.py" in all_files["files"]


def test_ablation_switches(fixture_scan):
    no_x = scan([str(FIXTURES / "dags" / "cross")], workspace=str(FIXTURES), cross_module=False)
    assert find_link(no_x, "cross_module")["status"] == "unknown"
    no_remap = scan([str(FIXTURES / "dags" / "cosmos_patterns.py")], workspace=str(FIXTURES), path_remap=False)
    assert find_link(no_remap, "cosmos_container")["status"] == "unknown"
    no_inline = scan([str(FIXTURES / "dags" / "dynamic_patterns.py")], workspace=str(FIXTURES), inline_calls=False)
    assert not any(l.get("task_id") == "h1" and l["status"] == "resolved" for l in no_inline["links"])
    assert no_inline["scanner"]["options"]["inline_calls"] is False
    # an unfollowed call that builds operators is reported at the call site, never dropped silently
    flagged = [u for u in no_inline["unresolved"] if u.get("category") == "call_not_followed"]
    by_line = {u["line"]: u["reason"] for u in flagged}
    assert "BashOperator" in by_line[39] and "BashOperator" in by_line[40]
    assert "DAG" in by_line[26]  # the make_dag factory called in a loop


@pytest.mark.parametrize("version_ok", [sys.version_info >= (3, 10)])
def test_supported_python(version_ok):
    assert version_ok
