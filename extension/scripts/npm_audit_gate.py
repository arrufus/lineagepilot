"""Gate the whole npm dependency tree by what each package is for (security audit H5).

Usage: npm audit --json --audit-level=none > work/npm-audit.json
       python scripts/npm_audit_gate.py work/npm-audit.json

`npm audit --omit=dev --audit-level=high` covers only what the VSIX ships. The tools that build,
check and publish the VSIX (vsce, ovsx, esbuild) are dev dependencies, so it skips them. This
reads npm's report for the whole tree and fails on any advisory at or above the level set for the
direct dependencies it reaches:

    shipped: package.json "dependencies"                      high
    the build and publish tools: @vscode/vsce, ovsx, esbuild  high
    the other dev dependencies: the test tools                critical

The release tools get the same bar as shipped code, because they make the file users install and
run with the publishing tokens. Moderate and low advisories are tracked, not gated: the minimatch
ReDoS that reaches vsce and ovsx needs a glob pattern from the repository itself. The test tools
fail only on critical: they never reach a user or a token, and their known high advisory
(serialize-javascript under mocha) needs attacker-chosen input to serialise, which a test run
does not have. Every advisory is printed, gated or not.

An advisory above its gate with no fix can be accepted in ACCEPTED: by its ID, in the packages
named, while it reaches only dev dependencies, never what ships, and only until its date, when it
fails again so that someone looks at it again. An accepted one is still printed, marked "acc".
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEVELS = ["info", "low", "moderate", "high", "critical"]
RELEASE_TOOLS = {"@vscode/vsce", "ovsx", "esbuild"}
SHIPPED_LEVEL = "high"
RELEASE_TOOLS_LEVEL = "high"
OTHER_DEV_LEVEL = "critical"


@dataclass(frozen=True)
class Accepted:
    """An advisory accepted in the tools, with why, and until when."""

    packages: frozenset[str]
    until: date
    why: str


ACCEPTED = {
    # Added 17 November 2026. The ReDoS needs a glob pattern the attacker chooses. vsce and ovsx
    # (through tinyglobby) glob only the patterns in this repository's .vscodeignore and
    # package.json, and whoever can change those can change the build. No picomatch is patched:
    # the advisory covers every version, to 4.0.7, and the newest vsce (4.1.1) still uses it.
    "GHSA-fc5q-39xc-vjpf": Accepted(
        packages=frozenset({"picomatch"}),
        until=date(2027, 2, 1),
        why="picomatch's ReDoS needs an attacker's glob; the release tools glob only this "
        "repository's patterns, and no picomatch is patched yet",
    ),
}


def reached(vulnerabilities: dict, name: str) -> set[str]:
    """The direct dependencies that `name` is part of, following npm's `effects` upwards."""
    seen, queue, direct = {name}, [name], set()
    while queue:
        node = vulnerabilities.get(queue.pop(), {})
        if node.get("isDirect"):
            direct.add(node["name"])
        for parent in node.get("effects", []):
            if parent not in seen:
                seen.add(parent)
                queue.append(parent)
    return direct


def gate_for(direct: str, package: dict) -> str:
    if direct in package.get("dependencies", {}):
        return SHIPPED_LEVEL
    if direct in RELEASE_TOOLS:
        return RELEASE_TOOLS_LEVEL
    if direct in package.get("devDependencies", {}):
        return OTHER_DEV_LEVEL
    return SHIPPED_LEVEL  # not in package.json: treat it as shipped


def accepted(ident: str, name: str, direct: set[str], package: dict, today: date) -> bool:
    """Whether an advisory over its gate is accepted: see ACCEPTED."""
    found = ACCEPTED.get(ident)
    dev, shipped = package.get("devDependencies", {}), package.get("dependencies", {})
    return (
        found is not None
        and name in found.packages
        and today <= found.until
        and all(d in dev and d not in shipped for d in direct)
    )


def evaluate(report: dict, package: dict, today: date | None = None) -> tuple[list[str], list[str]]:
    """(failures, every advisory as a line) for npm's JSON report."""
    today = today or date.today()
    if "error" in report or not isinstance(report.get("vulnerabilities"), dict):
        detail = report.get("error", {}).get("summary") if isinstance(report.get("error"), dict) else None
        return [f"no audit report from npm{': ' + detail if detail else ''}"], []
    vulnerabilities = report["vulnerabilities"]
    failures, lines = [], []
    for name in sorted(vulnerabilities):
        for advisory in vulnerabilities[name].get("via", []):
            if not isinstance(advisory, dict):
                continue  # inherited from a dependency: that package's own entry reports it
            severity = advisory.get("severity", "critical")
            ident = str(advisory.get("url", "")).rsplit("/", 1)[-1] or str(advisory.get("source"))
            direct = reached(vulnerabilities, name) or {name}
            gate = min((gate_for(d, package) for d in direct), key=LEVELS.index)
            failed = LEVELS.index(severity) >= LEVELS.index(gate)
            mark = "FAIL" if failed else "ok  "
            if failed and accepted(ident, name, direct, package, today):
                failed, mark = False, "acc "
                gate += f", accepted until {ACCEPTED[ident].until.isoformat()}: {ACCEPTED[ident].why}"
            line = (f"{mark} {severity:8} {ident} in {name}, "
                    f"reaches {', '.join(sorted(direct))} (gate: {gate})")
            lines.append(line)
            if failed:
                failures.append(line)
    return failures, lines


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("report", type=Path, help="the output of npm audit --json --audit-level=none")
    parser.add_argument("--package", type=Path, default=ROOT / "package.json")
    args = parser.parse_args()
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        sys.exit(f"npm audit gate: cannot read {args.report}: {error}")
    package = json.loads(args.package.read_text(encoding="utf-8"))
    failures, lines = evaluate(report, package)
    for line in lines:
        print(line)
    if failures:
        sys.exit("npm audit gate failed:\n  " + "\n  ".join(failures))
    print(f"npm audit gate passed: {len(lines)} advisories, none at or above its gate")


if __name__ == "__main__":
    main()
