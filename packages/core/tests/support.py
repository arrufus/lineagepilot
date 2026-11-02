"""Test helpers. The dbt artifacts are described in fixtures/dbt/PROVENANCE.json."""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

DBT_FIXTURES = Path(__file__).parent / "fixtures" / "dbt"

# Real artifacts written by dbt (spike S2), as opposed to legacy-v11, which is hand-written
REAL_PROJECTS = ("jaffle-1.12", "jaffle-fusion-2.0", "synth-24")


@cache
def raw_manifest(name: str) -> dict[str, Any]:
    """A fixture project's manifest. Shared: copy it before changing it."""
    return json.loads((DBT_FIXTURES / name / "manifest.json").read_bytes())


def project_config(name: str, catalog: bool = True) -> dict[str, str]:
    """A DbtParser config for a fixture project, with its catalog when it has one."""
    config = {"manifest_path": str(DBT_FIXTURES / name / "manifest.json")}
    catalog_path = DBT_FIXTURES / name / "catalog.json"
    if catalog and catalog_path.exists():
        config["catalog_path"] = str(catalog_path)
    return config
