"""Build the sample project that ships in the VSIX (task 6.1) from fixtures/impact-shop.

Usage: python scripts/build_sample.py [--check]

The fixture is a dbt project of our own, with real dbt 1.12 artifacts from before and after a
column rename; the Change Impact Review's acceptance tests run on it. The sample is that project
without what only the tests need, plus a README, a .gitignore and the marker that lets
LineagePilot: Apply the Sample Change find it. Building it from the fixture keeps one copy of the
artifacts, and the sample is always a project the acceptance tests passed on.

Writes extension/sample/, which is not committed. --check reports whether it is up to date.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT.parent / "fixtures" / "impact-shop"
EXTRAS = ROOT / "sample-extras"
OUT = ROOT / "sample"

# What the sample takes from the fixture: the project, its artifacts and the prepared change.
# Not make_fixture.py or PROVENANCE.json, which are about how the fixture is made.
INCLUDE = [
    "dbt_project.yml",
    "models",
    "macros",
    "seeds",
    "dags",
    "target/manifest.json",
    "target/catalog.json",
    "changes/rename-order-total",
]
CHANGE = "changes/rename-order-total"
# Kept out of the sample's repository: the artifacts, the change before it is applied, and what
# dbt writes if the user builds the project with a profiles.yml of their own
GITIGNORE = "target/\nchanges/\nlogs/\nprofiles.yml\n*.duckdb\n"
MARKER = ".lineagepilot/sample.json"
MARKER_CONTENT = {
    "about": "A copy of the LineagePilot sample project. LineagePilot: Apply the Sample Change makes the change "
             "in changes/rename-order-total only in a folder with this file.",
    "sample": "impact-shop",
    "version": 1,
}
# Left behind by tools, never part of the project
SKIP_NAMES = {"__pycache__", ".DS_Store"}


def planned(fixture: Path = FIXTURE, extras: Path = EXTRAS) -> dict[str, bytes]:
    """Every file of the sample, by its path in the sample, with its contents."""
    files: dict[str, bytes] = {}
    for entry in INCLUDE:
        source = fixture / entry
        if source.is_file():
            files[entry] = source.read_bytes()
        elif source.is_dir():
            for path in sorted(source.rglob("*")):
                if path.is_file() and not SKIP_NAMES.intersection(path.relative_to(fixture).parts):
                    files[path.relative_to(fixture).as_posix()] = path.read_bytes()
        else:
            raise SystemExit(f"the fixture has no {entry}: {source}")
    for path in sorted(extras.rglob("*")):
        if path.is_file() and not SKIP_NAMES.intersection(path.relative_to(extras).parts):
            files[path.relative_to(extras).as_posix()] = path.read_bytes()
    files[".gitignore"] = GITIGNORE.encode()
    files[MARKER] = (json.dumps(MARKER_CONTENT, indent=1) + "\n").encode()
    return files


def problems(files: dict[str, bytes]) -> list[str]:
    """What would make the sample fail to do what the walkthrough says."""
    found = []
    for required in ("dbt_project.yml", "target/manifest.json", "target/catalog.json", "README.md", MARKER):
        if required not in files:
            found.append(f"missing: {required}")
    change = [p for p in files if p.startswith(CHANGE + "/")]
    if f"{CHANGE}/target/manifest.json" not in files:
        found.append(f"missing: {CHANGE}/target/manifest.json")
    for path in change:
        relative = path[len(CHANGE) + 1:]
        if not relative.startswith("target/") and relative not in files:
            found.append(f"the change edits {relative}, which is not in the sample")
    if not any(p.startswith("dags/") for p in files):
        found.append("no DAG files")
    return found


def write(files: dict[str, bytes], out: Path = OUT) -> None:
    if out.exists():
        shutil.rmtree(out)
    for relative, data in files.items():
        target = out / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)


def up_to_date(files: dict[str, bytes], out: Path = OUT) -> bool:
    if not out.is_dir():
        return False
    present = {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()}
    if present != set(files):
        return False
    return all((out / relative).read_bytes() == data for relative, data in files.items())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="only report whether extension/sample is current")
    args = parser.parse_args()
    files = planned()
    found = problems(files)
    if found:
        sys.exit("sample project problems:\n  " + "\n  ".join(found))
    if args.check:
        if not up_to_date(files):
            sys.exit("extension/sample is out of date: run python scripts/build_sample.py")
        print("extension/sample is up to date")
        return
    write(files)
    print(f"Built the sample project: {len(files)} files in {OUT.relative_to(ROOT.parent)}")


if __name__ == "__main__":
    main()
