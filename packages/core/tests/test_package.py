"""Packaging guarantees for lineagepilot_core."""

from __future__ import annotations

import json
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

import lineagepilot_core

PACKAGE_ROOT = Path(__file__).resolve().parents[1]

# Design section 4.4: dropped at runtime. The import-linter contract enforces the first
# three statically (section 13); this test checks the whole list at run time.
DROPPED = ("sqlalchemy", "fastapi", "pydantic", "httpx", "snowflake", "uvicorn", "asyncpg")

IMPORT_EVERYTHING = """
import importlib, json, pkgutil, sys
import lineagepilot_core
for info in pkgutil.walk_packages(lineagepilot_core.__path__, "lineagepilot_core."):
    importlib.import_module(info.name)
print(json.dumps(sorted({m.split(".")[0] for m in sys.modules})))
"""


def test_version_has_one_source():
    assert version("lineagepilot-core") == lineagepilot_core.__version__


def test_importing_every_module_loads_nothing_from_the_server_stack():
    # -I: a fresh, isolated interpreter, so modules pytest itself loaded do not count.
    p = subprocess.run(
        [sys.executable, "-I", "-c", IMPORT_EVERYTHING],
        capture_output=True,
        text=True,
        check=False,
    )
    assert p.returncode == 0, p.stderr
    loaded = set(json.loads(p.stdout))
    assert loaded.isdisjoint(DROPPED), sorted(loaded & set(DROPPED))


LINT_IMPORTS = "from importlinter.cli import lint_imports_command; lint_imports_command()"


def test_import_contract_is_kept():
    p = subprocess.run(
        [sys.executable, "-c", LINT_IMPORTS],
        cwd=PACKAGE_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert p.returncode == 0, p.stdout + p.stderr
    assert "Contracts: 1 kept, 0 broken." in p.stdout
