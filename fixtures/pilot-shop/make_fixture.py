"""Regenerate pilot-shop's artifacts with real dbt, on DuckDB.

    python -m venv venv && venv/bin/pip install dbt-core==1.12.6 dbt-duckdb==1.11.1
    venv/bin/python fixtures/pilot-shop/make_fixture.py            # dbt Core only
    venv/bin/python fixtures/pilot-shop/make_fixture.py --fusion   # and dbt Fusion 2.0.9, in Docker

pilot-shop is a small dbt project written for this repository: a shop with customers, orders,
the products on each order, a catalogue and branches. It replaced the tests' copy of
dbt-labs/jaffle-shop, which has no licence (beads LP-0tf). Its shape is what the tests need:
staging views that rename their source's columns, marts built as chains of CTEs, a column traced
through a macro, an aggregate and a CASE over an aggregate, data tests of several kinds, one
name-like column, semantic models, metrics, a saved query and a unit test. It has no exposures,
owners or groups, and every model's column lineage resolves.

Writes:

- ``target/``: after ``dbt seed``, ``dbt run`` and ``dbt docs generate`` with dbt Core. The seed
  data is written into the temporary copy only, so the workspace has no CSV files, as a project
  whose seeds were loaded elsewhere would not.
- ``packages/core/tests/fixtures/dbt/pilot-1.12/``: the same two files, byte for byte.
- ``packages/core/tests/fixtures/dbt/pilot-fusion-2.0/manifest.json`` (``--fusion``): after
  ``dbt seed`` and ``dbt compile`` with dbt Fusion 2.0.9, run only inside a container built from
  dbt Labs' install script. Fusion writes no catalog.

The ``macros`` key is emptied to keep the manifests small: the parser never reads it. The build
folder's path is replaced by ``/project``, so nothing about this machine is in the fixture.
"""

from __future__ import annotations

import argparse
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
REPO = HERE.parents[1]
CORE_FIXTURES = REPO / "packages" / "core" / "tests" / "fixtures" / "dbt"
FUSION_VERSION = "2.0.9"
FUSION_IMAGE = f"lineagepilot-fusion:{FUSION_VERSION}"
FUSION_DOCKERFILE = f"""FROM debian:bookworm-slim
RUN apt-get update \\
 && apt-get install -y --no-install-recommends ca-certificates curl git \\
 && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --shell /bin/bash dbt
USER dbt
WORKDIR /home/dbt
ENV SHELL=/bin/bash
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh -o /tmp/install.sh \\
 && sh /tmp/install.sh --update --version {FUSION_VERSION} \\
 && rm /tmp/install.sh
ENV PATH=/home/dbt/.local/bin:$PATH
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false DO_NOT_TRACK=1
"""
PROFILE = """pilot_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{path}"
      threads: 1
"""

# Made up for this fixture. Customers 1, 2 and 4 order more than once; 6 never orders.
SEEDS = {
    "raw_customers": """id,name,signed_up_on
1,Amara Osei,2024-01-14
2,Tomasz Wrobel,2024-02-02
3,Lucia Ferraro,2024-02-19
4,Kenji Mori,2024-03-08
5,Siobhan Kelly,2024-04-21
6,Rafael Duarte,2024-05-30
""",
    "raw_orders": """id,customer_id,location_id,ordered_at,subtotal_cents,tax_cents
101,1,1,2024-06-01 08:12:00,850,68
102,2,2,2024-06-01 09:40:00,1200,90
103,1,1,2024-06-03 12:05:00,400,32
104,3,1,2024-06-04 16:30:00,1650,132
105,4,2,2024-06-05 07:55:00,300,23
106,2,2,2024-06-07 13:20:00,950,71
107,5,1,2024-06-08 10:10:00,700,56
108,4,2,2024-06-09 18:45:00,1400,105
""",
    "raw_order_lines": """id,order_id,product_sku,quantity
1,101,DRK-001,1
2,101,FOD-002,1
3,102,FOD-001,2
4,103,DRK-002,1
5,104,FOD-002,2
6,104,DRK-001,2
7,105,DRK-002,1
8,106,FOD-001,1
9,106,DRK-001,1
10,107,MRC-001,1
11,108,FOD-002,2
""",
    "raw_products": """sku,title,category,unit_price_cents
DRK-001,Flat white,drink,350
DRK-002,Mint tea,drink,300
FOD-001,Cheese toastie,food,600
FOD-002,Lentil soup,food,500
MRC-001,Enamel mug,merch,700
""",
    "raw_locations": """id,branch_name,tax_rate,opened_on
1,Canal Street,0.08,2023-11-01
2,Market Square,0.075,2024-03-15
""",
}


def dbt(project: Path, *args: str) -> None:
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(project),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    binary = Path(sys.executable).parent / "dbt"
    subprocess.run([str(binary), *args], cwd=project, env=env, check=True)


def keep(source: Path, destinations: list[Path], build: str) -> str:
    """Write an artifact without its macros or the build folder's path, and return its hash."""
    data = json.loads(source.read_bytes())
    if "macros" in data:
        data["macros"] = {}
    text = json.dumps(data, indent=1, sort_keys=True) + "\n"
    text = text.replace(build, "/project")
    for destination in destinations:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8", newline="\n")
    return hashlib.sha256(text.encode()).hexdigest()


def prepare(work: Path) -> Path:
    """A copy of the project to build in, with its seed data and a profile."""
    project = work / "pilot_shop"
    ignore = shutil.ignore_patterns("target", "make_fixture.py", "PROVENANCE.json")
    shutil.copytree(HERE, project, ignore=ignore)
    (project / "seeds").mkdir()
    for name, rows in SEEDS.items():
        (project / "seeds" / f"{name}.csv").write_text(rows, encoding="utf-8", newline="\n")
    return project


def build_core() -> dict[str, str]:
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp).resolve()
        project = prepare(work)
        (project / "profiles.yml").write_text(
            PROFILE.format(path=work / "pilot.duckdb"), encoding="utf-8"
        )
        dbt(project, "seed")
        dbt(project, "run")
        dbt(project, "test")
        dbt(project, "docs", "generate")
        hashes = {}
        for name in ("manifest.json", "catalog.json"):
            hashes[f"target/{name}"] = keep(
                project / "target" / name,
                [HERE / "target" / name, CORE_FIXTURES / "pilot-1.12" / name],
                str(work),
            )
        return hashes


def build_fusion() -> str:
    subprocess.run(
        ["docker", "build", "-q", "-t", FUSION_IMAGE, "-"],
        input=FUSION_DOCKERFILE.encode(),
        check=True,
    )
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp).resolve()
        project = prepare(work)
        (project / "profiles.yml").write_text(
            PROFILE.format(path="/work/pilot.duckdb"), encoding="utf-8"
        )
        for path in [work, *work.rglob("*")]:
            path.chmod(0o777 if path.is_dir() else 0o666)  # the container's user writes here
        for command in ("dbt seed --profiles-dir .", "dbt compile --profiles-dir ."):
            subprocess.run(
                ["docker", "run", "--rm", "-v", f"{work}:/work", "-w", "/work/pilot_shop",
                 FUSION_IMAGE, "bash", "-lc", command],
                check=True,
            )
        return keep(
            project / "target" / "manifest.json",
            [CORE_FIXTURES / "pilot-fusion-2.0" / "manifest.json"],
            "/work",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--fusion", action="store_true", help="also build with dbt Fusion")
    fusion = parser.parse_args().fusion

    hashes = build_core()
    core = f"{version('dbt-core')} + dbt-duckdb {version('dbt-duckdb')}"
    licence = "Written for this repository; not derived from jaffle-shop."
    (HERE / "PROVENANCE.json").write_text(
        json.dumps(
            {
                "about": "A small dbt project of our own for the language server and extension "
                "tests, and the source of the core's pilot-1.12 and pilot-fusion-2.0 artifacts. "
                "Regenerate it with make_fixture.py. Tests that change files work on a copy of "
                "this folder, never on it.",
                "licence": licence,
                "dbt": core,
                "steps": "seed, run, test, docs generate; macros emptied. The seed data is in "
                "make_fixture.py and is not kept here.",
                "sha256": hashes,
            },
            indent=1,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )

    provenance_path = CORE_FIXTURES / "PROVENANCE.json"
    provenance = json.loads(provenance_path.read_bytes())
    for old in ("jaffle-1.12", "jaffle-fusion-2.0"):
        provenance.pop(old, None)
    provenance["pilot-1.12"] = {
        "source": "fixtures/pilot-shop (make_fixture.py)",
        "licence": licence,
        "dbt": core,
        "steps": "seed, run, test, docs generate; macros emptied",
        "sha256": {
            "catalog.json": hashes["target/catalog.json"],
            "manifest.json": hashes["target/manifest.json"],
        },
    }
    if fusion:
        provenance["pilot-fusion-2.0"] = {
            "source": "fixtures/pilot-shop (make_fixture.py --fusion)",
            "licence": licence,
            "dbt": f"{FUSION_VERSION} (Fusion), in Docker",
            "steps": "seed, compile; macros emptied",
            "sha256": {"manifest.json": build_fusion()},
        }
    provenance_path.write_text(
        json.dumps(provenance, indent=1) + "\n", encoding="utf-8", newline="\n"
    )


if __name__ == "__main__":
    main()
