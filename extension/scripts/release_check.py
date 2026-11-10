"""Check that a release tag, package.json and CHANGELOG.md agree, and extract the release notes
(task 6.2). The release workflow runs this before it builds anything.

Usage: python scripts/release_check.py --tag v0.1.0 [--notes-out notes.md] [--github-output FILE]

The tag must be v<version> with the version in package.json, which is major.minor.patch as both
registries require, and CHANGELOG.md must have a "## <version>" section: its text becomes the
release notes. No release is a GitHub pre-release: GitHub never makes a pre-release the latest
release, so the newest release would have no stable link (LP-fzx).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TAG = re.compile(r"^v(\d+\.\d+\.\d+)$")
VERSION = re.compile(r"^\d+\.\d+\.\d+$")


def notes_for(changelog: str, version: str) -> str | None:
    """The text of the changelog's section for `version`, or None if it has none."""
    lines = changelog.splitlines()
    heading = re.compile(rf"^## {re.escape(version)}(\s|$)")
    for start, line in enumerate(lines):
        if heading.match(line):
            end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
            return "\n".join(lines[start + 1:end]).strip() or None
    return None


def check(tag: str, package: dict, changelog: str) -> tuple[list[str], dict[str, str]]:
    """(problems, facts) for a release from `tag`."""
    problems: list[str] = []
    version = str(package.get("version", ""))
    match = TAG.match(tag)
    if not match:
        problems.append(f"tag {tag!r} is not v<major>.<minor>.<patch>")
    elif match.group(1) != version:
        problems.append(f"tag {tag} does not match package.json version {version}")
    if not VERSION.match(version):
        problems.append(f"package.json version {version!r} is not major.minor.patch")
    notes = notes_for(changelog, version) if version else None
    if notes is None:
        problems.append(f"CHANGELOG.md has no section '## {version}' with text in it")
    facts = {"version": version, "notes": notes or ""}
    return problems, facts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--tag", required=True)
    parser.add_argument("--notes-out", type=Path, help="write the release notes here")
    parser.add_argument("--github-output", type=Path, help="append version= here")
    args = parser.parse_args()
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    problems, facts = check(args.tag, package, changelog)
    if problems:
        sys.exit("release check failed:\n  " + "\n  ".join(problems))
    if args.notes_out:
        args.notes_out.write_text(facts["notes"] + "\n", encoding="utf-8")
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as out:
            out.write(f"version={facts['version']}\n")
    print(f"release check passed: {args.tag} is version {facts['version']}")


if __name__ == "__main__":
    main()
