"""Check a packaged VSIX: universal, pure Python, only the files it should carry, within budget,
and acceptable to both the VS Code Marketplace and Open VSX (task 2.7).

Usage: python scripts/check_vsix.py dist/lineagepilot.vsix [--budget-mb 5] [--release]

The same VSIX goes to both registries. Open VSX accepts only licensed extensions, and the
licence (design decision D-5) is not chosen yet: until it is, that is reported as a warning,
and as an error with --release.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bundle_libs import NATIVE_SUFFIXES  # noqa: E402

# S1's VSIX was 1.36 MB; the core and server add about 0.5 MB. The budget leaves room for
# the Phase 3 webview, and fails the build on an accidental dependency or data file.
BUDGET_MB = 5.0

REQUIRED = [
    "extension/package.json",
    "extension/readme.md",  # vsce lower-cases it
    "extension/media/lineage.svg",
    "extension/dist/extension.js",
    "extension/dist/webview/webview.js",
    "extension/dist/webview/webview.css",
    "extension/dist/webview/review.js",
    "extension/dist/webview/review.css",
    "extension/bundled/tool/lsp_server.py",
    "extension/bundled/libs/BUNDLE.json",
    "extension/bundled/libs/pygls/__init__.py",
    "extension/bundled/libs/lsprotocol/types.py",
    "extension/bundled/libs/sqlglot/__init__.py",
    "extension/bundled/libs/yaml/__init__.py",
    "extension/bundled/libs/lineagepilot_core/__init__.py",
    "extension/bundled/libs/lineagepilot_lsp/server.py",
    "extension/bundled/libs/lineagepilot_lsp/protocol.py",
]
FORBIDDEN_PREFIXES = ["extension/node_modules/", "extension/src/", "extension/tests/",
                      "extension/out/", "extension/.venv/", "extension/scripts/",
                      "extension/coverage/", "extension/.vscode-test/"]
FORBIDDEN_SUFFIXES = (".map", ".ts", ".pyc")
# What both registries require of package.json; Open VSX also requires a licence
NAME = re.compile(r"^[a-z0-9][a-z0-9-]*$")
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def manifest_problems(package: dict) -> tuple[list[str], list[str]]:
    """(errors, warnings) for the registries' rules on package.json."""
    errors, warnings = [], []
    for key in ("name", "publisher"):
        if not NAME.match(str(package.get(key, ""))):
            errors.append(f"package.json {key} must be lower case letters, digits and dashes")
    if not SEMVER.match(str(package.get("version", ""))):
        errors.append("package.json version must be major.minor.patch")
    for key in ("displayName", "description"):
        if not package.get(key):
            errors.append(f"package.json has no {key}")
    if not package.get("engines", {}).get("vscode"):
        errors.append("package.json has no engines.vscode")
    if not package.get("repository", {}).get("url"):
        errors.append("package.json has no repository URL")
    if package.get("license") in (None, "", "UNLICENSED"):
        warnings.append("no licence: Open VSX will not accept the VSIX until one is chosen (decision D-5)")
    return errors, warnings


def check(vsix: Path, budget_mb: float, release: bool = False) -> tuple[list[str], dict]:
    problems = []
    with zipfile.ZipFile(vsix) as archive:
        names = archive.namelist()
        manifest = archive.read("extension.vsixmanifest").decode("utf-8")
        package = json.loads(archive.read("extension/package.json"))
        unpacked = sum(info.file_size for info in archive.infolist())
    errors, warnings = manifest_problems(package)
    problems += errors + (warnings if release else [])
    for name in REQUIRED:
        if name not in names:
            problems.append(f"missing: {name}")
    for name in names:
        if name.lower().endswith(NATIVE_SUFFIXES):
            problems.append(f"compiled file: {name}")
        if "__pycache__/" in name:
            problems.append(f"bytecode cache: {name}")
        if any(name.startswith(prefix) for prefix in FORBIDDEN_PREFIXES) or name.endswith(FORBIDDEN_SUFFIXES):
            problems.append(f"should not be packaged: {name}")
    if re.search(r"TargetPlatform=", manifest):
        problems.append("manifest declares a TargetPlatform; the VSIX must be universal")
    size_mb = vsix.stat().st_size / 1_000_000
    if size_mb > budget_mb:
        problems.append(f"VSIX is {size_mb:.1f} MB, budget is {budget_mb} MB")
    facts = {"files": len(names), "size_mb": round(size_mb, 2), "unpacked_mb": round(unpacked / 1_000_000, 2),
             "warnings": [] if release else warnings}
    return problems, facts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("vsix", type=Path)
    parser.add_argument("--budget-mb", type=float, default=BUDGET_MB)
    parser.add_argument("--release", action="store_true", help="treat registry warnings as errors")
    args = parser.parse_args()
    problems, facts = check(args.vsix, args.budget_mb, args.release)
    print(f"{args.vsix.name}: {facts['files']} files, {facts['size_mb']} MB packed, "
          f"{facts['unpacked_mb']} MB unpacked")
    for warning in facts["warnings"]:
        print(f"warning: {warning}")
    if problems:
        sys.exit("VSIX check failed:\n  " + "\n  ".join(problems))
    print("VSIX check passed: universal, pure Python, expected contents, within budget")


if __name__ == "__main__":
    main()
