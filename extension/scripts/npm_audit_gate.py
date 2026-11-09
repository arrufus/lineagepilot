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
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEVELS = ["info", "low", "moderate", "high", "critical"]
RELEASE_TOOLS = {"@vscode/vsce", "ovsx", "esbuild"}
SHIPPED_LEVEL = "high"
RELEASE_TOOLS_LEVEL = "high"
OTHER_DEV_LEVEL = "critical"


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


def evaluate(report: dict, package: dict) -> tuple[list[str], list[str]]:
    """(failures, every advisory as a line) for npm's JSON report."""
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
            line = (f"{'FAIL' if failed else 'ok  '} {severity:8} {ident} in {name}, "
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
