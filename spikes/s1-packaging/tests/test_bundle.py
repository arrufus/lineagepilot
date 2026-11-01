"""Unit tests for the bundler's lock parsing and purity check, plus a check of the real bundle."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import bundle_libs  # noqa: E402

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


def test_parse_lock_drops_markers_and_keeps_hashes():
    packages = bundle_libs.parse_lock(LOCK)
    assert [(p.name, p.version) for p in packages] == [("attrs", "26.1.0"),
                                                       ("exceptiongroup", "1.3.1")]
    assert packages[0].hashes == ["sha256:" + "a" * 64, "sha256:" + "b" * 64]
    assert ";" not in packages[1].requirement()


def test_parse_lock_rejects_unhashed_entries():
    with pytest.raises(ValueError, match="no hashes"):
        bundle_libs.parse_lock("attrs==26.1.0\n")


def test_parse_lock_rejects_unpinned_entries():
    with pytest.raises(ValueError, match="unexpected lock line"):
        bundle_libs.parse_lock("attrs>=26 --hash=sha256:" + "a" * 64 + "\n")


def test_check_pure_accepts_a_pure_tree(tmp_path):
    make_dist(tmp_path, "attrs", "26.1.0")
    (tmp_path / "attrs").mkdir()
    (tmp_path / "attrs" / "__init__.py").write_text("")
    expected = bundle_libs.parse_lock(LOCK)[:1]
    assert bundle_libs.check_pure(tmp_path, expected) == []


@pytest.mark.parametrize("filename", ["_yaml.cpython-310-darwin.so", "_x.pyd", "lib.dylib", "a.DLL"])
def test_check_pure_flags_compiled_files(tmp_path, filename):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / filename).write_bytes(b"\0")
    assert any("compiled file" in p for p in bundle_libs.check_pure(tmp_path))


def test_check_pure_flags_platform_wheel_tags(tmp_path):
    make_dist(tmp_path, "pyyaml", "6.0.3", tag="cp310-cp310-macosx_11_0_arm64")
    assert any("impure wheel tag" in p for p in bundle_libs.check_pure(tmp_path))


def test_check_pure_flags_missing_and_wrong_versions(tmp_path):
    make_dist(tmp_path, "attrs", "25.0.0")
    problems = bundle_libs.check_pure(tmp_path, bundle_libs.parse_lock(LOCK))
    assert "attrs is 25.0.0, lock says 26.1.0" in problems
    assert "missing locked package: exceptiongroup" in problems


@pytest.mark.skipif(not os.environ.get("LP_NETWORK_TESTS"), reason="downloads from PyPI; set LP_NETWORK_TESTS=1")
def test_build_produces_a_pure_bundle_from_the_lock(tmp_path):
    out = tmp_path / "libs"
    bundle_libs.build(out)
    expected = bundle_libs.parse_lock((ROOT / "requirements.lock").read_text())
    assert bundle_libs.check_pure(out, expected) == []
    assert not (out / "bin").exists()
    assert not list(out.rglob("__pycache__"))


def test_real_bundle_is_pure_and_matches_the_lock():
    libs = ROOT / "bundled" / "libs"
    expected = bundle_libs.parse_lock((ROOT / "requirements.lock").read_text())
    assert bundle_libs.check_pure(libs, expected) == []
    recorded = json.loads((libs / "BUNDLE.json").read_text())
    assert {p["name"] for p in recorded["packages"]} == {p.name for p in expected}
    # The 3.10 backport is present even when a newer Python built the bundle.
    assert (libs / "exceptiongroup").is_dir()
