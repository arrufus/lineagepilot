"""The build scripts (task 2.7): the bundler's lock parsing and purity check, and the VSIX gate.

Run from extension/ with the language server's environment, which has pytest:

    uv run --project ../packages/lsp --locked pytest --no-cov -p no:cacheprovider scripts/tests
"""

from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import bundle_libs  # noqa: E402
import check_vsix  # noqa: E402

LOCK = """\
attrs==26.1.0 \\
    --hash=sha256:{a} \\
    --hash=sha256:{b}
    # via cattrs
exceptiongroup==1.3.1 ; python_full_version < '3.11' \\
    --hash=sha256:{a}
""".format(a="a" * 64, b="b" * 64)


def make_dist(libs: Path, name: str, version: str, tag: str = "py3-none-any") -> None:
    info = libs / f"{name}-{version}.dist-info"
    info.mkdir(parents=True)
    (info / "WHEEL").write_text(f"Wheel-Version: 1.0\nTag: {tag}\n")
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")


# The bundler


def test_parse_lock_drops_markers_and_keeps_hashes():
    packages = bundle_libs.parse_lock(LOCK)
    assert [(p.name, p.version) for p in packages] == [("attrs", "26.1.0"), ("exceptiongroup", "1.3.1")]
    assert packages[0].hashes == ["sha256:" + "a" * 64, "sha256:" + "b" * 64]
    assert ";" not in packages[1].requirement()


@pytest.mark.parametrize(
    "text, message", [("attrs==26.1.0\n", "no hashes"), ("attrs>=26 --hash=sha256:" + "a" * 64 + "\n", "unexpected")]
)
def test_parse_lock_rejects_unhashed_or_unpinned_entries(text, message):
    with pytest.raises(ValueError, match=message):
        bundle_libs.parse_lock(text)


def test_check_pure_accepts_a_pure_tree(tmp_path):
    make_dist(tmp_path, "attrs", "26.1.0")
    assert bundle_libs.check_pure(tmp_path, bundle_libs.parse_lock(LOCK)[:1]) == []


@pytest.mark.parametrize("filename", ["_yaml.cpython-310-darwin.so", "_x.pyd", "lib.dylib", "a.DLL"])
def test_check_pure_flags_compiled_files(tmp_path, filename):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / filename).write_bytes(b"\0")
    assert any("compiled file" in p for p in bundle_libs.check_pure(tmp_path))


def test_check_pure_flags_platform_wheels_and_wrong_versions(tmp_path):
    make_dist(tmp_path, "pyyaml", "6.0.3", tag="cp310-cp310-macosx_11_0_arm64")
    make_dist(tmp_path, "attrs", "25.0.0")
    problems = bundle_libs.check_pure(tmp_path, bundle_libs.parse_lock(LOCK))
    assert any("impure wheel tag" in p for p in problems)
    assert "attrs is 25.0.0, lock says 26.1.0" in problems
    assert "missing locked package: exceptiongroup" in problems


@pytest.mark.skipif(not (ROOT / "bundled" / "libs" / "BUNDLE.json").exists(), reason="run scripts/bundle_libs.py first")
def test_the_real_bundle_is_pure_and_complete():
    libs = ROOT / "bundled" / "libs"
    recorded = json.loads((libs / "BUNDLE.json").read_text())
    expected = [bundle_libs.Locked(p["name"], p["version"], p["hashes"]) for p in recorded["packages"]]
    assert bundle_libs.check_pure(libs, expected) == []
    names = {p.name for p in expected}
    locked = {p.name for p in bundle_libs.parse_lock((ROOT / "requirements.lock").read_text())}
    assert names == locked | {"lineagepilot-core", "lineagepilot-lsp"}
    # The 3.10 backport is present whichever Python built the bundle
    assert (libs / "exceptiongroup").is_dir()


# The VSIX gate

MANIFEST = '<PackageManifest><Metadata><Identity Id="lineagepilot" /></Metadata></PackageManifest>'
PACKAGE = {
    "name": "lineagepilot",
    "publisher": "lineagepilot",
    "version": "0.1.0",
    "displayName": "LineagePilot",
    "description": "Lineage",
    "engines": {"vscode": "^1.93.0"},
    "repository": {"url": "https://example.invalid/repo"},
    "license": "MIT",
}


def make_vsix(path: Path, extra: dict[str, bytes] | None = None, manifest: str = MANIFEST, package=None) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("extension.vsixmanifest", manifest)
        for name in check_vsix.REQUIRED:
            archive.writestr(name, json.dumps(package or PACKAGE) if name.endswith("package.json") else "x")
        for name, data in (extra or {}).items():
            archive.writestr(name, data)
    return path


def test_a_minimal_valid_vsix_passes(tmp_path):
    problems, facts = check_vsix.check(make_vsix(tmp_path / "ok.vsix"), budget_mb=10, release=True)
    assert problems == [] and facts["warnings"] == []


@pytest.mark.parametrize(
    "name, expected",
    [
        ("extension/bundled/libs/yaml/_yaml.cpython-310-darwin.so", "compiled file"),
        ("extension/bundled/libs/x/__pycache__/a.cpython-313.pyc", "bytecode cache"),
        ("extension/node_modules/left-pad/index.js", "should not be packaged"),
        ("extension/dist/extension.js.map", "should not be packaged"),
        ("extension/src/extension.ts", "should not be packaged"),
        ("extension/scripts/bundle_libs.py", "should not be packaged"),
    ],
)
def test_unwanted_entries_fail(tmp_path, name, expected):
    problems, _ = check_vsix.check(make_vsix(tmp_path / "bad.vsix", {name: b"\0"}), budget_mb=10)
    assert any(p.startswith(expected) for p in problems), problems


def test_missing_files_platform_vsix_and_budget(tmp_path):
    vsix = tmp_path / "missing.vsix"
    with zipfile.ZipFile(vsix, "w") as archive:
        archive.writestr("extension.vsixmanifest", MANIFEST)
        archive.writestr("extension/package.json", json.dumps(PACKAGE))
    problems, _ = check_vsix.check(vsix, budget_mb=10)
    assert "missing: extension/readme.md" in problems
    manifest = MANIFEST.replace("<Identity ", '<Identity TargetPlatform="darwin-arm64" ')
    problems, _ = check_vsix.check(make_vsix(tmp_path / "p.vsix", manifest=manifest), budget_mb=10)
    assert any("TargetPlatform" in p for p in problems)
    problems, _ = check_vsix.check(make_vsix(tmp_path / "big.vsix"), budget_mb=0.000001)
    assert any("budget" in p for p in problems)


def test_registry_rules_on_package_json(tmp_path):
    bad = {**PACKAGE, "name": "LineagePilot", "version": "0.1", "repository": {}, "displayName": ""}
    problems, _ = check_vsix.check(make_vsix(tmp_path / "m.vsix", package=bad), budget_mb=10)
    assert {p.split(" must")[0].split(" has")[0] for p in problems} >= {"package.json name", "package.json version"}
    assert "package.json has no repository URL" in problems and "package.json has no displayName" in problems


def test_no_licence_warns_and_fails_a_release(tmp_path):
    unlicensed = make_vsix(tmp_path / "u.vsix", package={**PACKAGE, "license": "UNLICENSED"})
    problems, facts = check_vsix.check(unlicensed, budget_mb=10)
    assert problems == [] and "Open VSX" in facts["warnings"][0]
    problems, _ = check_vsix.check(unlicensed, budget_mb=10, release=True)
    assert any("Open VSX" in p for p in problems)


def test_main(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["check_vsix.py", str(make_vsix(tmp_path / "ok.vsix"))])
    check_vsix.main()
    assert "VSIX check passed" in capsys.readouterr().out
    bad = make_vsix(tmp_path / "bad.vsix", {"extension/x.dll": b"\0"})
    monkeypatch.setattr(sys, "argv", ["check_vsix.py", str(bad)])
    with pytest.raises(SystemExit, match="VSIX check failed"):
        check_vsix.main()


@pytest.mark.skipif(not (ROOT / "dist" / "lineagepilot.vsix").exists(), reason="run npm run package first")
def test_the_real_vsix_passes():
    problems, facts = check_vsix.check(ROOT / "dist" / "lineagepilot.vsix", budget_mb=check_vsix.BUDGET_MB)
    assert problems == [], problems
