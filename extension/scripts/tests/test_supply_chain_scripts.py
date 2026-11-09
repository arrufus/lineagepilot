"""The supply-chain hardening in the build scripts (security audit H3, H4 and H5): no build-machine
paths in the bundle or the VSIX, a VSIX whose bytes depend only on its files and the commit time,
and the npm audit gate. Run with the other script tests:

    uv run --project ../packages/lsp --locked pytest --no-cov -p no:cacheprovider scripts/tests
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import bundle_libs  # noqa: E402
import check_vsix  # noqa: E402
import normalise_vsix  # noqa: E402
import npm_audit_gate  # noqa: E402

from test_scripts import make_vsix  # noqa: E402

EPOCH = 1_762_000_000  # 2025-11-01T12:26:40Z


# H3: pip's direct_url.json and console scripts record the build machine's temporary paths


def make_installed(libs: Path, name: str, direct_url: bool, script: bool = False) -> Path:
    info = libs / f"{name}-1.0.dist-info"
    info.mkdir(parents=True)
    rows = [f"{name}/__init__.py,sha256=abc,10", f"{info.name}/METADATA,sha256=def,20",
            f'"{info.name}/odd, name.txt",sha256=ghi,30']
    if direct_url:
        (info / "direct_url.json").write_text('{"url": "file:///var/folders/xy/T/lp-bundle-1/wheels/x.whl"}')
        rows.insert(2, f"{info.name}/direct_url.json,sha256=jkl,62")
    if script:
        rows.insert(0, "../../bin/lineagepilot,sha256=mno,235")
    rows.append(f"{info.name}/RECORD,,")
    (info / "RECORD").write_text("\n".join(rows) + "\n", encoding="utf-8")
    return info


def test_strip_build_paths_removes_direct_url_and_script_lines_only(tmp_path):
    ours = make_installed(tmp_path, "lineagepilot_core", direct_url=True, script=True)
    pyyaml = make_installed(tmp_path, "pyyaml", direct_url=True)
    third = make_installed(tmp_path, "attrs", direct_url=False)
    before = (third / "RECORD").read_text(encoding="utf-8")
    assert bundle_libs.strip_build_paths(tmp_path) == [ours.name, pyyaml.name]
    assert not (ours / "direct_url.json").exists() and not (pyyaml / "direct_url.json").exists()
    assert (ours / "RECORD").read_text(encoding="utf-8").splitlines() == [
        "lineagepilot_core/__init__.py,sha256=abc,10",
        f"{ours.name}/METADATA,sha256=def,20",
        f'"{ours.name}/odd, name.txt",sha256=ghi,30',
        f"{ours.name}/RECORD,,",
    ]
    assert "direct_url.json" not in (pyyaml / "RECORD").read_text(encoding="utf-8")
    assert (third / "RECORD").read_text(encoding="utf-8") == before
    assert bundle_libs.strip_build_paths(tmp_path) == []


BUILT = (ROOT / "bundled" / "libs" / "BUNDLE.json").exists()


@pytest.mark.skipif(not BUILT, reason="run scripts/bundle_libs.py first")
def test_the_real_bundle_records_no_build_paths():
    libs = ROOT / "bundled" / "libs"
    assert list(libs.glob("*.dist-info/direct_url.json")) == []
    for record in libs.glob("*.dist-info/RECORD"):
        text = record.read_text(encoding="utf-8")
        assert "direct_url.json" not in text and "../../bin/" not in text, record.parent.name


def test_a_vsix_with_a_direct_url_fails(tmp_path):
    name = "extension/bundled/libs/pyyaml-6.0.3.dist-info/direct_url.json"
    problems, _ = check_vsix.check(make_vsix(tmp_path / "d.vsix", {name: b"{}"}), budget_mb=10)
    assert problems == [(f"build machine path: {name} records where pip installed it from "
                         "(bundle_libs.py removes these)")]


# H4: a VSIX whose bytes depend only on its files and SOURCE_DATE_EPOCH

FILES = {
    "extension.vsixmanifest": b"<PackageManifest/>",
    "[Content_Types].xml": b"<Types/>",
    "extension/package.json": b'{"name": "lineagepilot"}',
    "extension/dist/extension.js": b"console.log('x');\n" * 200,
    "extension/bundled/tool/lsp_server.py": b"print('server')\n",
    "extension/media/café.svg": b"<svg/>",
}


def build_zip(order: list[str], when: tuple, mode: int, executable: str | None = None,
              extras: bool = False) -> bytes:
    """A zip of FILES in `order`, as different builders might write it."""
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in order:
            info = zipfile.ZipInfo(name, date_time=when)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = ((0o100755 if name == executable else mode) << 16)
            info.extra = b"UT\x05\x00\x01" + int(time.time()).to_bytes(4, "little") if extras else b""
            archive.writestr(info, FILES[name])
    return out.getvalue()


def test_normalised_archives_of_the_same_files_are_byte_identical():
    one = build_zip(list(FILES), (2026, 11, 8, 15, 39, 30), 0o100664, extras=True)
    two = build_zip(sorted(FILES, reverse=True), (2024, 1, 2, 3, 4, 6), 0o100644)
    assert one != two
    first = normalise_vsix.normalise(one, EPOCH)
    assert first == normalise_vsix.normalise(two, EPOCH)
    # Idempotent, and two runs of the same input agree
    assert normalise_vsix.normalise(first, EPOCH) == first == normalise_vsix.normalise(one, EPOCH)
    with zipfile.ZipFile(io.BytesIO(first)) as archive:
        assert archive.testzip() is None
        infos = archive.infolist()
        assert [i.filename for i in infos] == sorted(FILES, key=lambda n: n.encode("utf-8"))
        assert infos[0].filename == "[Content_Types].xml" and infos[1].filename == "extension.vsixmanifest"
        assert {archive.read(name) == data for name, data in FILES.items()} == {True}
        assert {i.date_time for i in infos} == {(2025, 11, 1, 12, 26, 40)}
        assert {i.external_attr >> 16 for i in infos} == {0o100644}
        assert {i.extra for i in infos} == {b""}
        assert {i.flag_bits for i in infos} == {0x0800}
        assert {i.compress_type for i in infos} == {zipfile.ZIP_DEFLATED}


def test_normalise_keeps_the_executable_bit_and_clamps_old_times():
    data = build_zip(list(FILES), (2020, 1, 1, 0, 0, 0), 0o100600,
                     executable="extension/bundled/tool/lsp_server.py")
    with zipfile.ZipFile(io.BytesIO(normalise_vsix.normalise(data, 0))) as archive:
        modes = {i.filename: i.external_attr >> 16 for i in archive.infolist()}
        assert modes.pop("extension/bundled/tool/lsp_server.py") == 0o100755
        assert set(modes.values()) == {0o100644}
        assert {i.date_time for i in archive.infolist()} == {(1980, 1, 1, 0, 0, 0)}


def test_normalise_refuses_duplicate_names():
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr("extension/a.txt", b"1")
        with pytest.warns(UserWarning, match="Duplicate name"):
            archive.writestr("extension/a.txt", b"2")
    with pytest.raises(ValueError, match="duplicate"):
        normalise_vsix.normalise(out.getvalue(), EPOCH)


def test_normalise_main_rewrites_in_place_and_needs_a_time(tmp_path, monkeypatch, capsys):
    vsix = make_vsix(tmp_path / "ok.vsix")
    monkeypatch.delenv("SOURCE_DATE_EPOCH", raising=False)
    monkeypatch.setattr(sys, "argv", ["normalise_vsix.py", str(vsix)])
    with pytest.raises(SystemExit, match="SOURCE_DATE_EPOCH"):
        normalise_vsix.main()
    monkeypatch.setenv("SOURCE_DATE_EPOCH", "not-a-number")
    with pytest.raises(SystemExit, match="whole seconds"):
        normalise_vsix.main()
    monkeypatch.setenv("SOURCE_DATE_EPOCH", str(EPOCH))
    normalise_vsix.main()
    assert "timestamps 2025-11-01T12:26:40Z" in capsys.readouterr().out
    once = vsix.read_bytes()
    assert (vsix.stat().st_mode & 0o777) == 0o644 or os.name == "nt"
    problems, _ = check_vsix.check(vsix, budget_mb=10, release=True)
    assert problems == []
    normalise_vsix.main()
    assert vsix.read_bytes() == once


@pytest.mark.skipif(not (ROOT / "dist" / "lineagepilot.vsix").exists(), reason="run npm run package first")
def test_the_real_vsix_normalises_to_the_same_files():
    data = (ROOT / "dist" / "lineagepilot.vsix").read_bytes()
    first = normalise_vsix.normalise(data, EPOCH)
    assert normalise_vsix.normalise(first, EPOCH) == first
    normalise_vsix.check_same_files(data, first)


# H5: npm audit over the build, publish and test tools too

PACKAGE = {"dependencies": {"vscode-languageclient": "10.1.2"},
           "devDependencies": {"@vscode/vsce": "4.0.0", "ovsx": "1.3.0", "esbuild": "0.28.2", "mocha": "11.8.0"}}


def vuln(name, direct, effects, *advisories):
    via = [{"source": 1, "name": name, "url": f"https://github.com/advisories/{ghsa}", "severity": severity}
           for ghsa, severity in advisories]
    return {"name": name, "isDirect": direct, "effects": effects, "via": via}


def report(*vulns):
    return {"auditReportVersion": 2, "vulnerabilities": {v["name"]: v for v in vulns}}


KNOWN = report(
    vuln("minimatch", False, ["@vscode/vsce", "mocha", "vscode-languageclient"], ("GHSA-g3hv", "moderate")),
    vuln("@vscode/vsce", True, ["ovsx"]),
    vuln("ovsx", True, []),
    vuln("vscode-languageclient", True, []),
    vuln("serialize-javascript", False, ["mocha"], ("GHSA-5c6j", "high"), ("GHSA-qj8w", "moderate")),
    vuln("mocha", True, []),
)


def test_the_known_advisories_pass_and_are_all_listed():
    failures, lines = npm_audit_gate.evaluate(KNOWN, PACKAGE)
    assert failures == []
    assert len(lines) == 3
    assert "GHSA-g3hv in minimatch, reaches @vscode/vsce, mocha, ovsx, vscode-languageclient (gate: high)" in lines[0]


@pytest.mark.parametrize("vulns, culprit", [
    # A high advisory anywhere under a release tool
    ([vuln("yazl", False, ["@vscode/vsce"], ("GHSA-new1", "high")), vuln("@vscode/vsce", True, ["ovsx"]),
      vuln("ovsx", True, [])], "GHSA-new1"),
    ([vuln("esbuild", True, [], ("GHSA-new2", "high"))], "GHSA-new2"),
    # A high advisory in shipped code
    ([vuln("vscode-languageclient", True, [], ("GHSA-new3", "high"))], "GHSA-new3"),
    # A critical one in a test tool
    ([vuln("serialize-javascript", False, ["mocha"], ("GHSA-new4", "critical")), vuln("mocha", True, [])],
     "GHSA-new4"),
])
def test_new_advisories_at_their_gate_fail(vulns, culprit):
    failures, _ = npm_audit_gate.evaluate(report(*vulns), PACKAGE)
    assert len(failures) == 1 and culprit in failures[0]


def test_an_error_instead_of_a_report_fails(tmp_path, monkeypatch):
    failures, _ = npm_audit_gate.evaluate({"error": {"code": "ENOAUDIT", "summary": "network"}}, PACKAGE)
    assert failures == ["no audit report from npm: network"]
    path = tmp_path / "audit.json"
    path.write_text(json.dumps(KNOWN))
    package = tmp_path / "package.json"
    package.write_text(json.dumps(PACKAGE))
    monkeypatch.setattr(sys, "argv", ["npm_audit_gate.py", str(path), "--package", str(package)])
    npm_audit_gate.main()
    path.write_text("")
    with pytest.raises(SystemExit, match="cannot read"):
        npm_audit_gate.main()
