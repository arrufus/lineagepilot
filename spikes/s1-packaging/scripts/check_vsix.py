"""Check a packaged VSIX: universal, pure Python, only the files it should carry, within budget.

Usage: python scripts/check_vsix.py dist/lineagepilot-spike.vsix [--budget-mb 10]
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bundle_libs import NATIVE_SUFFIXES  # noqa: E402

REQUIRED = [
    "extension/package.json",
    "extension/dist/extension.js",
    "extension/bundled/tool/lsp_server.py",
    "extension/bundled/tool/lineagepilot_spike/server.py",
    "extension/bundled/libs/BUNDLE.json",
    "extension/bundled/libs/pygls/__init__.py",
    "extension/bundled/libs/lsprotocol/types.py",
    "extension/bundled/libs/sqlglot/__init__.py",
    "extension/bundled/libs/yaml/__init__.py",
]
FORBIDDEN_PREFIXES = ["extension/node_modules/", "extension/src/", "extension/tests/",
                      "extension/fixture-workspace/", "extension/out/", "extension/.venv/"]


def check(vsix: Path, budget_mb: float) -> tuple[list[str], dict]:
    problems = []
    with zipfile.ZipFile(vsix) as archive:
        names = archive.namelist()
        manifest = archive.read("extension.vsixmanifest").decode("utf-8")
        unpacked = sum(info.file_size for info in archive.infolist())
    for name in REQUIRED:
        if name not in names:
            problems.append(f"missing: {name}")
    for name in names:
        if name.lower().endswith(NATIVE_SUFFIXES):
            problems.append(f"compiled file: {name}")
        if "__pycache__/" in name:
            problems.append(f"bytecode cache: {name}")
        if any(name.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
            problems.append(f"should not be packaged: {name}")
    if re.search(r"TargetPlatform=", manifest):
        problems.append("manifest declares a TargetPlatform; the VSIX must be universal")
    size_mb = vsix.stat().st_size / 1_000_000
    if size_mb > budget_mb:
        problems.append(f"VSIX is {size_mb:.1f} MB, budget is {budget_mb} MB")
    facts = {"files": len(names), "size_mb": round(size_mb, 2), "unpacked_mb": round(unpacked / 1_000_000, 2)}
    return problems, facts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("vsix", type=Path)
    parser.add_argument("--budget-mb", type=float, default=10.0)
    args = parser.parse_args()
    problems, facts = check(args.vsix, args.budget_mb)
    print(f"{args.vsix.name}: {facts['files']} files, {facts['size_mb']} MB packed, "
          f"{facts['unpacked_mb']} MB unpacked")
    if problems:
        sys.exit("VSIX check failed:\n  " + "\n  ".join(problems))
    print("VSIX check passed: universal, pure Python, expected contents, within budget")


if __name__ == "__main__":
    main()
