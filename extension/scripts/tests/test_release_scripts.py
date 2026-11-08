"""The Phase 6 build scripts: the sample project, the third-party notices, the release check, and
the licence (tasks 6.1 and 6.2). Run with the other script tests:

    uv run --project ../packages/lsp --locked pytest --no-cov -p no:cacheprovider scripts/tests
"""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "scripts"))

import build_sample  # noqa: E402
import check_vsix  # noqa: E402
import release_check  # noqa: E402
import third_party_notices as notices  # noqa: E402

PACKAGE = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))


# The licence (decision D-5)


def test_every_copy_of_the_licence_is_the_same_mit_licence():
    text = (REPO / "LICENSE").read_text(encoding="utf-8")
    assert text.startswith("MIT License\n")
    for copy in (ROOT / "LICENSE", REPO / "packages" / "core" / "LICENSE", REPO / "packages" / "lsp" / "LICENSE"):
        assert copy.read_text(encoding="utf-8") == text, copy
    assert PACKAGE["license"] == "MIT"
    assert "--skip-license" not in PACKAGE["scripts"]["package"]
    for package in ("core", "lsp"):
        pyproject = (REPO / "packages" / package / "pyproject.toml").read_text(encoding="utf-8")
        assert 'license = "MIT"' in pyproject, package


# The sample project (task 6.1)


def test_the_sample_is_the_fixture_without_its_tooling_plus_its_own_files():
    files = build_sample.planned()
    assert build_sample.problems(files) == []
    fixture = build_sample.FIXTURE
    assert files["target/manifest.json"] == (fixture / "target" / "manifest.json").read_bytes()
    assert files["models/marts/orders.sql"] == (fixture / "models" / "marts" / "orders.sql").read_bytes()
    assert "make_fixture.py" not in files and "PROVENANCE.json" not in files
    assert {"README.md", ".gitignore", build_sample.MARKER} <= set(files)
    assert json.loads(files[build_sample.MARKER])["sample"] == "impact-shop"
    # Every file the change edits is in the project, and its manifest goes in target/
    change = [p for p in files if p.startswith(build_sample.CHANGE + "/")]
    assert sorted(change) == [
        "changes/rename-order-total/models/marts/orders.sql",
        "changes/rename-order-total/models/marts/orders.yml",
        "changes/rename-order-total/target/manifest.json",
    ]
    ignored = files[".gitignore"].decode().split()
    assert {"target/", "changes/"} <= set(ignored)


def test_sample_problems_are_reported(tmp_path):
    files = build_sample.planned()
    del files["target/catalog.json"]
    files["changes/rename-order-total/models/marts/missing.sql"] = b"select 1"
    assert build_sample.problems(files) == [
        "missing: target/catalog.json",
        "the change edits models/marts/missing.sql, which is not in the sample",
    ]
    assert build_sample.problems({})[:2] == ["missing: dbt_project.yml", "missing: target/manifest.json"]
    with pytest.raises(SystemExit, match="the fixture has no"):
        build_sample.planned(fixture=tmp_path)


def test_writing_and_checking_the_sample(tmp_path):
    files = build_sample.planned()
    out = tmp_path / "sample"
    assert not build_sample.up_to_date(files, out)
    build_sample.write(files, out)
    assert build_sample.up_to_date(files, out)
    (out / "models" / "stray.sql").write_text("select 1")
    assert not build_sample.up_to_date(files, out)
    build_sample.write(files, out)  # starts again from nothing
    assert build_sample.up_to_date(files, out)
    (out / "README.md").write_text("changed")
    assert not build_sample.up_to_date(files, out)


def _git(cwd: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}
    subprocess.run(["git", *args], cwd=cwd, env=env, check=True, capture_output=True)


@pytest.mark.skipif(shutil.which("git") is None, reason="needs git")
def test_the_samples_walkthrough_gives_the_review_its_readme_promises(tmp_path):
    """What a user of the sample does: commit it, apply the change, review. The README and the
    walkthrough say what this finds, so this checks they are right."""
    from lineagepilot_core.cli import main

    root = tmp_path / "lineagepilot-sample"
    build_sample.write(build_sample.planned(), root)
    _git(root, "init", "-q", "-b", "main")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "The sample as copied")
    # What LineagePilot: Apply the Sample Change copies
    change = root / build_sample.CHANGE
    for path in sorted(change.rglob("*")):
        if path.is_file():
            shutil.copyfile(path, root / path.relative_to(change))
    out = io.StringIO()
    main(["review", "--project-dir", str(root), "--no-cache", "--json"], out=out)
    review = json.loads(out.getvalue())
    assert review["summary"]["impacted"] == {"airflow_task": 4, "exposure": 1, "model": 4}
    assert review["summary"]["unknowns"] == 2
    [orders] = review["changedNodes"]
    assert orders["columns"]["removed"] == ["order_total"] and orders["columns"]["added"] == ["total_usd"]
    assert {u["subject"] for u in review["unknowns"]} == {"airflow:backfill", "model.impact_shop.broken_report"}
    readme = (ROOT / "sample-extras" / "README.md").read_text(encoding="utf-8")
    assert "reaches four models, the dashboard and four Airflow tasks" in readme


# The third-party notices (task 6.2)


def _npm(root: Path, name: str, licence, files: dict[str, str]) -> None:
    directory = root / "node_modules" / name
    directory.mkdir(parents=True)
    (directory / "package.json").write_text(json.dumps({"name": name, "version": "1.0.0", "license": licence}))
    for filename, text in files.items():
        (directory / filename).write_text(text)
    (directory / "index.js").write_text("")


def _dist(libs: Path, name: str, metadata: str, licences: dict[str, str]) -> None:
    info = libs / f"{name}-1.0.dist-info"
    (info / "licenses").mkdir(parents=True)
    (info / "METADATA").write_text(f"Metadata-Version: 2.4\nName: {name}\nVersion: 1.0\n{metadata}")
    for filename, text in licences.items():
        (info / "licenses" / filename).write_text(text)


def test_npm_packages_are_found_from_what_the_bundles_read(tmp_path):
    _npm(tmp_path, "left", "MIT", {"LICENSE": "MIT text"})
    _npm(tmp_path, "@scope/right", {"type": "ISC"}, {"License.txt": "ISC text", "README.md": "not a licence"})
    _npm(tmp_path, "unused", "MIT", {"LICENSE": "x"})
    inputs = ["src/extension.ts", "node_modules/left/index.js", "node_modules/left/lib/a.js",
              "node_modules\\@scope\\right\\index.js"]
    packages = notices.npm_packages(inputs, root=tmp_path)
    assert [(p.name, p.licence, p.texts) for p in packages] == [
        ("@scope/right", "ISC", [("License.txt", "ISC text")]),
        ("left", "MIT", [("LICENSE", "MIT text")]),
    ]


def test_python_licences_from_expressions_licence_fields_and_classifiers(tmp_path):
    _dist(tmp_path, "expressed", "License-Expression: Apache-2.0\n", {"LICENSE.txt": "Apache"})
    _dist(tmp_path, "fielded", "License: MIT\n", {"LICENSE": "MIT"})
    _dist(tmp_path, "classified", "Classifier: License :: OSI Approved :: MIT License\n", {"LICENSE": "MIT"})
    _dist(tmp_path, "lineagepilot_core", "License-Expression: MIT\n", {"LICENSE": "ours"})
    packages = notices.python_packages(tmp_path)
    assert [(p.name, p.licence) for p in packages] == [("classified", "MIT"), ("expressed", "Apache-2.0"), ("fielded", "MIT")]
    assert packages[1].texts == [("LICENSE.txt", "Apache")]


def test_a_licence_off_the_list_or_without_its_text_fails():
    ok = notices.Package("npm", "a", "1", "MIT", [("LICENSE", "x")])
    copyleft = notices.Package("npm", "b", "1", "GPL-3.0", [("LICENSE", "x")])
    textless = notices.Package("PyPI", "c", "1", "MIT", [])
    assert notices.problems([ok]) == []
    found = notices.problems([copyleft, textless])
    assert "licence 'GPL-3.0' is not on the allowed list" in found[0]
    assert found[1] == "PyPI c 1: no licence file to include"


def test_the_notices_fence_every_text_safely():
    package = notices.Package("npm", "tricky", "1", "MIT", [("LICENSE", "has ``` in it")])
    text = notices.render([package], [])
    assert "````text\nhas ``` in it\n````" in text
    assert "| `tricky` | 1 | MIT |" in text


@pytest.mark.skipif(not (ROOT / "dist" / "bundled-inputs.json").exists() or not (ROOT / "bundled" / "libs").exists(),
                    reason="run node esbuild.mjs and scripts/bundle_libs.py first")
def test_every_package_that_ships_has_an_allowed_licence_and_its_text():
    inputs = json.loads((ROOT / "dist" / "bundled-inputs.json").read_text(encoding="utf-8"))
    npm, python = notices.npm_packages(inputs), notices.python_packages()
    assert notices.problems([*npm, *python]) == []
    assert {"react", "zod", "vscode-languageclient", "@xyflow/react"} <= {p.name for p in npm}
    assert {"pygls", "sqlglot", "PyYAML"} <= {p.name for p in python}


# The release check (task 6.2)

CHANGELOG = """# Changelog

## 0.2.0

- Second.

## 0.1.0 (beta)

- First.
- Still first.

## 0.0.1
"""


def test_a_matching_tag_passes_and_gives_its_notes():
    problems, facts = release_check.check("v0.1.0", {"version": "0.1.0"}, CHANGELOG)
    assert problems == []
    assert facts == {"version": "0.1.0", "prerelease": "true", "notes": "- First.\n- Still first."}
    _, facts = release_check.check("v1.0.0", {"version": "1.0.0"}, "## 1.0.0\n\nDone.\n")
    assert facts["prerelease"] == "false" and facts["notes"] == "Done."


@pytest.mark.parametrize(
    "tag, version, changelog, expected",
    [
        ("v0.1.1", "0.1.0", CHANGELOG, "does not match package.json version 0.1.0"),
        ("0.1.0", "0.1.0", CHANGELOG, "is not v<major>.<minor>.<patch>"),
        ("v0.1.0-beta.1", "0.1.0", CHANGELOG, "is not v<major>.<minor>.<patch>"),
        ("v0.1", "0.1", CHANGELOG, "is not major.minor.patch"),
        ("v0.3.0", "0.3.0", CHANGELOG, "CHANGELOG.md has no section '## 0.3.0'"),
        ("v0.0.1", "0.0.1", CHANGELOG, "CHANGELOG.md has no section '## 0.0.1' with text"),
        ("v0.1.0", "0.1.0", "## 0.1.00\n\nx\n", "CHANGELOG.md has no section"),
    ],
)
def test_a_mismatch_fails(tag, version, changelog, expected):
    problems, _ = release_check.check(tag, {"version": version}, changelog)
    assert any(expected in p for p in problems), problems


def test_the_real_package_and_changelog_agree(tmp_path, monkeypatch, capsys):
    version = PACKAGE["version"]
    notes, output = tmp_path / "notes.md", tmp_path / "output"
    monkeypatch.setattr(sys, "argv", ["release_check.py", "--tag", f"v{version}", "--notes-out", str(notes),
                                      "--github-output", str(output)])
    release_check.main()
    assert "release check passed" in capsys.readouterr().out
    assert notes.read_text(encoding="utf-8").strip()
    assert output.read_text(encoding="utf-8") == f"version={version}\nprerelease={'true' if version.startswith('0.') else 'false'}\n"
    monkeypatch.setattr(sys, "argv", ["release_check.py", "--tag", "v99.0.0"])
    with pytest.raises(SystemExit, match="release check failed"):
        release_check.main()


# The VSIX gate: what the walkthrough shows must ship


def test_every_walkthrough_page_is_required_in_the_vsix():
    media = check_vsix.walkthrough_media(PACKAGE)
    assert len(media) == len(PACKAGE["contributes"]["walkthroughs"][0]["steps"])
    for path in media:
        assert (ROOT / path.removeprefix("extension/")).is_file(), path
    assert check_vsix.walkthrough_media({}) == []
    assert check_vsix.walkthrough_media({"contributes": {"walkthroughs": [{"steps": [
        {"media": {"image": "./media/a.png", "altText": "a"}}, {"media": {"markdown": "media/b.md"}}]}]}}) == [
        "extension/media/a.png", "extension/media/b.md"]
