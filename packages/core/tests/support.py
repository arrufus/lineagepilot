"""Test helpers. The dbt artifacts are described in fixtures/dbt/PROVENANCE.json."""

from __future__ import annotations

import json
import shutil
from functools import cache
from pathlib import Path
from typing import Any

DBT_FIXTURES = Path(__file__).parent / "fixtures" / "dbt"
# A project with real artifacts from before and after a column rename; see its PROVENANCE.json
IMPACT_SHOP = Path(__file__).parents[3] / "fixtures" / "impact-shop"
RENAME = IMPACT_SHOP / "changes" / "rename-order-total"

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


def copy_impact_shop(destination: Path) -> Path:
    """A working copy of the project as deployed, with target/ ignored as in a real repository."""
    ignore = shutil.ignore_patterns("changes", "make_fixture.py", "PROVENANCE.json")
    shutil.copytree(IMPACT_SHOP, destination, ignore=ignore)
    (destination / ".gitignore").write_text("target/\n", encoding="utf-8")
    return destination


def apply_rename(root: Path, recompile: bool = True) -> None:
    """The developer renames orders.order_total and, unless told otherwise, runs dbt compile:
    the files and the manifest as real dbt wrote them. catalog.json stays as it was."""
    for path in RENAME.rglob("*"):
        if path.is_file() and (recompile or "target" not in path.parts):
            target = root / path.relative_to(RENAME)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(path, target)
