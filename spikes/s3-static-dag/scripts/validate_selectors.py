"""Validate the selector engine against real ``dbt ls`` on the fixture project written for this spike.

dbt runs ONLY on ``tests/fixtures/dbt/shop`` (our own project), copied to ``work/dbt/shop`` so dbt's
``target/`` and ``logs/`` stay out of the fixtures. No third-party dbt project is ever run.

Run with:  uv run --group dbt python scripts/validate_selectors.py

Outputs:
- tests/fixtures/dbt/shop/target/manifest.json  (trimmed: macros and docs removed)
- tests/fixtures/dbt/shop_dbt_ls.json           (dbt ls answers, so tests run without dbt)
- results/selector_agreement.json
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE / "src"))

from lineagepilot_spike_dag.manifest import manifest_from_dict  # noqa: E402
from lineagepilot_spike_dag.selector import select  # noqa: E402

FIXTURE = HERE / "tests" / "fixtures" / "dbt" / "shop"
WORK = HERE / "work" / "dbt" / "shop"

# (label, select args, exclude args, selector)
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
    ("path + two excludes", ["path:models"], ["tag:deprecated", "config.materialized:ephemeral"], None),
    ("everything", [], [], None),
    ("yaml selector union+exclude", [], [], "nightly"),
    ("yaml selector children", [], [], "finance_and_children"),
    ("yaml selector intersection", [], [], "core_not_python"),
    ("yaml selector parents depth", [], [], "upstream_of_ltv"),
    # Unsupported by design: the engine must answer "unknown", never an empty set.
    ("UNSUPPORTED exposure method", ["+exposure:weekly_report"], [], None),
    ("UNSUPPORTED test_name method", ["1+test_name:relationships"], [], None),
]


def dbt_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update({
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "False",
        "DO_NOT_TRACK": "1",
        "DBT_PROFILES_DIR": str(WORK),
        "DBT_PROJECT_DIR": str(WORK),
        "DBT_USE_COLORS": "False",
        "DBT_PARTIAL_PARSE": "True",
    })
    return env


def dbt(*args: str) -> subprocess.CompletedProcess[str]:
    exe = Path(sys.executable).with_name("dbt")
    cmd = [str(exe), "--quiet", "--no-use-colors", *args, "--project-dir", str(WORK), "--profiles-dir", str(WORK)]
    return subprocess.run(cmd, capture_output=True, text=True, env=dbt_env(), cwd=str(WORK), timeout=300)


def main() -> int:
    if WORK.exists():
        shutil.rmtree(WORK)
    shutil.copytree(FIXTURE, WORK, ignore=shutil.ignore_patterns("target", "logs"))
    res = dbt("parse")
    if res.returncode != 0:
        print(res.stdout, res.stderr)
        return 1
    manifest_path = WORK / "target" / "manifest.json"
    full = json.loads(manifest_path.read_text(encoding="utf-8"))
    trimmed = {k: v for k, v in full.items() if k not in ("macros", "docs")}
    (FIXTURE / "target").mkdir(exist_ok=True)
    (FIXTURE / "target" / "manifest.json").write_text(json.dumps(trimmed, indent=1, sort_keys=True), encoding="utf-8")
    manifest = manifest_from_dict(trimmed)

    answers = []
    for label, sel, exc, selector in CASES:
        args = ["ls", "--resource-type", "model", "--output", "json", "--output-keys", "unique_id"]
        if sel:
            args += ["--select", *sel]
        if exc:
            args += ["--exclude", *exc]
        if selector:
            args += ["--selector", selector]
        r = dbt(*args)
        ids = sorted(
            json.loads(line)["unique_id"] for line in r.stdout.splitlines() if line.strip().startswith("{")
        )
        answers.append({
            "label": label, "select": sel, "exclude": exc, "selector": selector,
            "dbt_returncode": r.returncode, "dbt_ids": ids,
            "dbt_stderr_tail": r.stderr.strip().splitlines()[-1:] if r.returncode else [],
        })
    meta = full.get("metadata", {})
    (FIXTURE.parent / "shop_dbt_ls.json").write_text(json.dumps({
        "dbt_version": meta.get("dbt_version"),
        "adapter_type": meta.get("adapter_type"),
        "generated_at": meta.get("generated_at"),
        "cases": answers,
    }, indent=1), encoding="utf-8")

    rows = compare(manifest, answers)
    summary = summarise(rows, meta)
    out = HERE / "results" / "selector_agreement.json"
    out.write_text(json.dumps({"summary": summary, "cases": rows}, indent=1), encoding="utf-8")
    print(json.dumps(summary, indent=1))
    return 0


def compare(manifest, answers: list[dict]) -> list[dict]:
    rows = []
    for a in answers:
        got = select(manifest, a["select"], a["exclude"], a["selector"], resource_types=("model",))
        engine_ids = None if got.unknown else sorted(got.ids)  # type: ignore[arg-type]
        if got.unknown:
            verdict = "engine_unknown"
        elif engine_ids == a["dbt_ids"]:
            verdict = "agree"
        else:
            verdict = "disagree"
        rows.append({
            "label": a["label"], "select": a["select"], "exclude": a["exclude"], "selector": a["selector"],
            "verdict": verdict, "dbt_count": len(a["dbt_ids"]),
            "engine_count": None if engine_ids is None else len(engine_ids),
            "engine_reason": got.reason,
            "only_dbt": sorted(set(a["dbt_ids"]) - set(engine_ids or [])) if engine_ids is not None else None,
            "only_engine": sorted(set(engine_ids) - set(a["dbt_ids"])) if engine_ids is not None else None,
        })
    return rows


def summarise(rows: list[dict], meta: dict) -> dict:
    supported = [r for r in rows if not r["label"].startswith("UNSUPPORTED")]
    unsupported = [r for r in rows if r["label"].startswith("UNSUPPORTED")]
    return {
        "dbt_version": meta.get("dbt_version"),
        "expressions": len(rows),
        "supported_expressions": len(supported),
        "agree": sum(r["verdict"] == "agree" for r in supported),
        "disagree": sum(r["verdict"] == "disagree" for r in supported),
        "engine_unknown_on_supported": sum(r["verdict"] == "engine_unknown" for r in supported),
        "unsupported_expressions": len(unsupported),
        "unsupported_answered_unknown": sum(r["verdict"] == "engine_unknown" for r in unsupported),
    }


if __name__ == "__main__":
    raise SystemExit(main())
