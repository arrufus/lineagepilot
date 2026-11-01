from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

SPIKE = Path(__file__).resolve().parent.parent
FIX = SPIKE / "tests" / "fixtures"
REPO = SPIKE.parents[2]
BACKEND_FIXTURE = REPO / "backend" / "tests" / "fixtures" / "dbt"


def run_python(code: str) -> subprocess.CompletedProcess:
    """Run code in a fresh interpreter (clean sys.modules), from the spike root."""
    return subprocess.run([sys.executable, "-c", code], cwd=SPIKE, capture_output=True, text=True)


@pytest.fixture(scope="session")
def mods():
    from spike.loader import load_parsers
    return load_parsers()


def parse(mods, manifest: Path, catalog: Path | None = None, cls=None):
    cfg = {"manifest_path": str(manifest)}
    if catalog and catalog.exists():
        cfg["catalog_path"] = str(catalog)
    return asyncio.run((cls or mods["dbt_parser"].DbtParser)().parse(cfg))


REAL_FIXTURES = {
    "synth-24": (FIX / "synth-24" / "manifest.json", FIX / "synth-24" / "catalog.json"),
    "jaffle-1.12": (FIX / "jaffle-1.12" / "manifest.json", FIX / "jaffle-1.12" / "catalog.json"),
    "jaffle-fusion-2.0": (FIX / "jaffle-fusion-2.0" / "manifest.json", None),
}
