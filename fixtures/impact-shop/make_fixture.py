"""Regenerate this fixture's artifacts with real dbt, on DuckDB.

    python -m venv venv && venv/bin/pip install dbt-core==1.12.6 dbt-duckdb==1.11.1
    venv/bin/python fixtures/impact-shop/make_fixture.py

Writes two states of the same project:

- ``target/``: the project as it is, after ``dbt seed``, ``dbt run`` and ``dbt docs generate``.
  This is what is deployed: ``catalog.json`` describes the tables dbt built.
- ``changes/rename-order-total/target/``: after renaming ``orders.order_total`` to ``total_usd``
  (the other files in that folder) and running only ``dbt compile``, as a developer would.
  ``customer_revenue`` still reads the old name, which is the breakage the review must find, so
  building it would fail. The catalog stays as it was, and is not repeated there.

``broken_report`` is deliberately not valid SQL, so ``dbt run`` leaves it out. The ``macros`` key
is emptied to keep the manifests small: the parser never reads it.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path

HERE = Path(__file__).resolve().parent
CHANGE = HERE / "changes" / "rename-order-total"
PROFILE = """impact_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{path}"
      threads: 1
"""


def dbt(project: Path, *args: str) -> None:
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(project),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    binary = Path(sys.executable).parent / "dbt"
    subprocess.run([str(binary), *args], cwd=project, env=env, check=True)


def keep(source: Path, destination: Path, project: Path) -> str:
    data = json.loads(source.read_bytes())
    if "macros" in data:
        data["macros"] = {}
    destination.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, indent=1, sort_keys=True) + "\n"
    # This machine's temporary folder is no part of the fixture
    text = text.replace(str(project.parent.resolve()), "/project").replace(str(project.parent), "/project")
    destination.write_text(text, encoding="utf-8", newline="\n")
    return hashlib.sha256(text.encode()).hexdigest()


def main() -> None:
    with tempfile.TemporaryDirectory() as work:
        project = Path(work) / "impact_shop"
        ignore = shutil.ignore_patterns("target", "changes", "make_fixture.py", "PROVENANCE.json")
        shutil.copytree(HERE, project, ignore=ignore)
        database = Path(work) / "impact_shop.duckdb"
        (project / "profiles.yml").write_text(PROFILE.format(path=database), encoding="utf-8")

        dbt(project, "seed")
        dbt(project, "run", "--exclude", "broken_report")
        dbt(project, "docs", "generate")
        hashes = {
            "target/manifest.json": keep(
                project / "target" / "manifest.json", HERE / "target" / "manifest.json", project
            ),
            "target/catalog.json": keep(
                project / "target" / "catalog.json", HERE / "target" / "catalog.json", project
            ),
        }

        for path in (CHANGE / "models").rglob("*"):
            if path.is_file():
                shutil.copy(path, project / path.relative_to(CHANGE))
        dbt(project, "compile")
        manifest = CHANGE / "target" / "manifest.json"
        hashes["changes/rename-order-total/target/manifest.json"] = keep(
            project / "target" / "manifest.json", manifest, project
        )

        provenance = {
            "about": "A small dbt project of our own for the Change Impact Review: a source, a "
            "staging model, a mart, its downstream models, an exposure, owners and a deliberately "
            "unparseable model (design section 14). Regenerate it with make_fixture.py. Tests that "
            "change files work on a copy of this folder, never on it.",
            "licence": "Written for this repository; not derived from jaffle-shop.",
            "dbt": f"{version('dbt-core')} + dbt-duckdb {version('dbt-duckdb')}",
            "steps": {
                "target/": "seed, run --exclude broken_report, docs generate; macros emptied",
                "changes/rename-order-total/target/": "the rename applied, then compile; macros "
                "emptied. catalog.json stays the one in target/",
            },
            "sha256": hashes,
        }
        (HERE / "PROVENANCE.json").write_text(
            json.dumps(provenance, indent=1) + "\n", encoding="utf-8", newline="\n"
        )


if __name__ == "__main__":
    main()
