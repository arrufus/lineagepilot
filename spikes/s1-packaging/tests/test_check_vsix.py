"""Tests for the VSIX gate: universal, pure Python, only the expected files, within budget."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import check_vsix  # noqa: E402

MANIFEST = '<PackageManifest><Metadata><Identity Id="lineagepilot-spike" /></Metadata></PackageManifest>'


def make_vsix(path: Path, extra: dict[str, bytes] | None = None, manifest: str = MANIFEST) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("extension.vsixmanifest", manifest)
        for name in check_vsix.REQUIRED:
            archive.writestr(name, "x")
        for name, data in (extra or {}).items():
            archive.writestr(name, data)
    return path


def test_minimal_valid_vsix_passes(tmp_path):
    problems, facts = check_vsix.check(make_vsix(tmp_path / "ok.vsix"), budget_mb=10)
    assert problems == []
    assert facts["files"] == len(check_vsix.REQUIRED) + 1


@pytest.mark.parametrize("name, expected", [
    ("extension/bundled/libs/yaml/_yaml.cpython-310-darwin.so", "compiled file"),
    ("extension/bundled/libs/x/__pycache__/a.cpython-313.pyc", "bytecode cache"),
    ("extension/node_modules/left-pad/index.js", "should not be packaged"),
    ("extension/fixture-workspace/yaml.py", "should not be packaged"),
])
def test_unwanted_entries_fail(tmp_path, name, expected):
    problems, _ = check_vsix.check(make_vsix(tmp_path / "bad.vsix", {name: b"\0"}), budget_mb=10)
    assert any(p.startswith(expected) for p in problems), problems


def test_missing_required_file_fails(tmp_path):
    vsix = tmp_path / "missing.vsix"
    with zipfile.ZipFile(vsix, "w") as archive:
        archive.writestr("extension.vsixmanifest", MANIFEST)
    problems, _ = check_vsix.check(vsix, budget_mb=10)
    assert f"missing: {check_vsix.REQUIRED[0]}" in problems


def test_platform_specific_vsix_fails(tmp_path):
    manifest = MANIFEST.replace("<Identity ", '<Identity TargetPlatform="darwin-arm64" ')
    problems, _ = check_vsix.check(make_vsix(tmp_path / "p.vsix", manifest=manifest), budget_mb=10)
    assert any("TargetPlatform" in p for p in problems)


def test_over_budget_fails(tmp_path):
    problems, _ = check_vsix.check(make_vsix(tmp_path / "big.vsix"), budget_mb=0.000001)
    assert any("budget" in p for p in problems)


def test_main_exits_non_zero_on_problems(tmp_path, monkeypatch, capsys):
    vsix = make_vsix(tmp_path / "bad.vsix", {"extension/x.dll": b"\0"})
    monkeypatch.setattr(sys, "argv", ["check_vsix.py", str(vsix)])
    with pytest.raises(SystemExit, match="VSIX check failed"):
        check_vsix.main()


def test_main_passes_a_good_vsix(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["check_vsix.py", str(make_vsix(tmp_path / "ok.vsix"))])
    check_vsix.main()
    assert "VSIX check passed" in capsys.readouterr().out


@pytest.mark.skipif(not (ROOT / "dist" / "lineagepilot-spike.vsix").exists(), reason="run npm run package first")
def test_the_real_vsix_passes():
    problems, facts = check_vsix.check(ROOT / "dist" / "lineagepilot-spike.vsix", budget_mb=10)
    assert problems == [], problems
