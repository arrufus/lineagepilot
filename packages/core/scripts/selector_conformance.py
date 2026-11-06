"""Check the selector engine against real ``dbt ls`` (task 5.1, design section 6.2).

dbt changed wildcard matching in 1.10, so the engine's semantics follow the manifest's dbt version,
and CI runs this with dbt on both sides of that change:

    uv run --with dbt-core==1.12.6 --with dbt-duckdb==1.11.1 python scripts/selector_conformance.py
    uv run --with dbt-core==1.9.10 --with dbt-duckdb==1.9.6 python scripts/selector_conformance.py

dbt runs only on tests/fixtures/airflow/dbt/shop, a project written for spike S3, in a temporary
copy, against an in-memory database. With ``--record FILE``, the answers are written to that file:
tests/fixtures/airflow/dbt/shop_dbt_ls*.json are what the offline tests read.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from lineagepilot_core.airflow.manifest import manifest_from_dict
from lineagepilot_core.airflow.selector import select

FIXTURES = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "airflow" / "dbt"
FIXTURE = FIXTURES / "shop"

# (label, select, exclude, YAML selector). A label starting UNSUPPORTED must answer unknown.
CASES: list[tuple[str, list[str], list[str], str | None]] = [
    ("bare name", ["stg_customers"], [], None),
    ("all parents", ["+customer_ltv"], [], None),
    ("all children", ["fct_orders+"], [], None),
    ("1 parent level", ["1+fct_orders"], [], None),
    ("1 child level", ["fct_orders+1"], [], None),
    ("n+ both sides", ["2+customer_ltv+1"], [], None),
    ("@ childrens parents", ["@stg_payments"], [], None),
    ("@ through relationships test", ["@fct_orders"], [], None),
    ("tag", ["tag:finance"], [], None),
    ("intersection", ["tag:staging,config.materialized:view"], [], None),
    ("union in one arg", ["tag:daily tag:core"], [], None),
    ("union across args", ["tag:daily", "tag:core"], [], None),
    ("path folder", ["path:models/staging"], [], None),
    ("path file", ["path:models/marts/finance/fct_orders.sql"], [], None),
    ("path glob", ["path:models/marts/*/dim_*.sql"], [], None),
    ("default path method", ["models/reporting"], [], None),
    ("default file method", ["fct_orders.sql"], [], None),
    ("file stem", ["file:py_scores"], [], None),
    ("fqn package path", ["fqn:shop.marts.finance"], [], None),
    ("unscoped folder fqn", ["marts"], [], None),
    ("full fqn", ["shop.staging.stg_orders"], [], None),
    ("wildcard contains", ["*orders*"], [], None),
    # Where dbt 1.10 changed: a bare wildcard also matches the leaf name, so this finds nothing
    # on dbt 1.9 and four models on 1.12
    ("wildcard prefix", ["stg_*"], [], None),
    ("config.materialized", ["config.materialized:incremental"], [], None),
    ("config + children", ["config.materialized:ephemeral+"], [], None),
    ("resource_type", ["resource_type:model"], [], None),
    ("source children", ["source:raw+"], [], None),
    ("source table children", ["source:raw.payments+"], [], None),
    ("source tag + depth", ["tag:raw_data+1"], [], None),
    ("versioned all", ["fct_revenue"], [], None),
    ("versioned dot", ["fct_revenue.v1"], [], None),
    ("versioned underscore", ["fct_revenue_v2"], [], None),
    ("package intersect tag", ["package:shop,tag:pii"], [], None),
    ("python model config tag", ["tag:ml"], [], None),
    ("tag wildcard", ["tag:fin*"], [], None),
    ("select + exclude", ["+rpt_weekly"], ["tag:staging"], None),
    (
        "path + two excludes",
        ["path:models"],
        ["tag:deprecated", "config.materialized:ephemeral"],
        None,
    ),
    ("everything", [], [], None),
    ("yaml selector union+exclude", [], [], "nightly"),
    ("yaml selector children", [], [], "finance_and_children"),
    ("yaml selector intersection", [], [], "core_not_python"),
    ("yaml selector parents depth", [], [], "upstream_of_ltv"),
    ("UNSUPPORTED exposure method", ["+exposure:weekly_report"], [], None),
    ("UNSUPPORTED test_name method", ["1+test_name:relationships"], [], None),
]


def dbt(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "False",
        "DO_NOT_TRACK": "1",
        "DBT_USE_COLORS": "False",
    }
    executable = shutil.which("dbt", path=str(Path(sys.executable).parent))
    command = [executable or "dbt", "--quiet", "--no-use-colors", *args]
    command += ["--project-dir", str(project), "--profiles-dir", str(project)]
    return subprocess.run(
        command, capture_output=True, text=True, env=env, cwd=project, timeout=300
    )


def answers(project: Path) -> list[dict]:
    found = []
    for label, chosen, excluded, selector in CASES:
        args = ["ls", "--resource-type", "model", "--output", "json", "--output-keys", "unique_id"]
        args += [*(["--select", *chosen] if chosen else [])]
        args += [*(["--exclude", *excluded] if excluded else [])]
        args += [*(["--selector", selector] if selector else [])]
        done = dbt(project, *args)
        lines = (line for line in done.stdout.splitlines() if line.strip().startswith("{"))
        found.append(
            {
                "label": label,
                "select": chosen,
                "exclude": excluded,
                "selector": selector,
                "dbt_returncode": done.returncode,
                "dbt_ids": sorted(json.loads(line)["unique_id"] for line in lines),
            }
        )
    return found


def compare(manifest, recorded: list[dict]) -> list[str]:
    problems = []
    for case in recorded:
        got = select(manifest, case["select"], case["exclude"], case["selector"], ("model",))
        if case["label"].startswith("UNSUPPORTED"):
            if not got.unknown:
                problems.append(f"{case['label']}: the engine should say unknown")
        elif got.unknown:
            problems.append(f"{case['label']}: the engine said unknown: {got.reason}")
        elif sorted(got.ids) != case["dbt_ids"]:
            only_dbt = sorted(set(case["dbt_ids"]) - got.ids)
            only_engine = sorted(got.ids - set(case["dbt_ids"]))
            problems.append(f"{case['label']}: only dbt {only_dbt}; only the engine {only_engine}")
    return problems


def main() -> int:
    with tempfile.TemporaryDirectory() as work:
        project = Path(work) / "shop"
        shutil.copytree(FIXTURE, project, ignore=shutil.ignore_patterns("target", "logs"))
        parsed = dbt(project, "parse")
        if parsed.returncode != 0:
            sys.stdout.write(parsed.stdout + parsed.stderr)
            return 1
        raw = json.loads((project / "target" / "manifest.json").read_text(encoding="utf-8"))
        manifest = manifest_from_dict(raw)
        recorded = answers(project)
    version = raw["metadata"]["dbt_version"]
    problems = compare(manifest, recorded)
    agreed = len(recorded) - len(problems)
    sys.stdout.write(f"dbt {version}: {agreed} of {len(recorded)} expressions agree\n")
    for problem in problems:
        sys.stdout.write(f"  {problem}\n")
    if "--record" in sys.argv:
        record = Path(sys.argv[sys.argv.index("--record") + 1])
        payload = {
            "dbt_version": version,
            "adapter_type": raw["metadata"].get("adapter_type"),
            "generated_at": raw["metadata"].get("generated_at"),
            "cases": recorded,
        }
        record.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8", newline="\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
