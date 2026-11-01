"""Spike S3 end to end: fetch the three repositories (data only), scan, score against ground truth.

    uv run python scripts/run_s3.py fetch     # shallow clones at the pinned SHAs into work/repos/
    uv run python scripts/run_s3.py run       # scan + evaluate, writes results/

Third-party repositories are read as text. Nothing in them is imported, executed or installed;
``git clone`` runs with hooks disabled, no submodules and no LFS smudge.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE / "src"))

from lineagepilot_spike_dag.evaluate import evaluate, review_unresolved  # noqa: E402
from lineagepilot_spike_dag.scanner import scan  # noqa: E402

REPOS = [
    {"name": "astronomer-cosmos", "url": "https://github.com/astronomer/astronomer-cosmos.git",
     "sha": "c92d0b23ff8cfc4c96f8b0bf2ec13f2f496d4fc2", "paths": ["dev/dags"]},
    {"name": "cosmos-demo", "url": "https://github.com/astronomer/cosmos-demo.git",
     "sha": "169707b8b725fb3b852f7eb58dc6edff818c9a57", "paths": ["dags"]},
    {"name": "airflow-dbt-demo", "url": "https://github.com/astronomer/airflow-dbt-demo.git",
     "sha": "8da816d0c7db1a9bd012c051bbde21a5d0a612f5", "paths": ["dags"]},
]
# Held out: read and labelled only after the scanner was frozen (see results/heldout_frozen_run.json).
HELDOUT = [
    {"name": "konosp_dbt-airflow-docker-compose", "url": "https://github.com/konosp/dbt-airflow-docker-compose.git",
     "sha": "25f03315cd35228b3211fb46e28d763a997ca952", "paths": ["airflow/dags"]},
    {"name": "danielvdende_data-testing-with-airflow", "url": "https://github.com/danielvdende/data-testing-with-airflow.git",
     "sha": "86600ee5dcffe54adb2e4b620f404ff2c13cd887", "paths": ["dags"]},
    {"name": "GokuMohandas_data-engineering", "url": "https://github.com/GokuMohandas/data-engineering.git",
     "sha": "1d346983ad79575f1c9729ae6ec8574a6de37de7", "paths": ["airflow/dags"]},
    {"name": "saastoolset_dbt-core-airflow-template", "url": "https://github.com/saastoolset/dbt-core-airflow-template.git",
     "sha": "d4cd1d2414c3313fd94456fc63f6c84cff32bc1e", "paths": ["dags/airflow-dbt-python", "dags/bashOperator"]},
]
WORK = HERE / "work" / "repos"
WORK_HELDOUT = HERE / "work" / "heldout"
RESULTS = HERE / "results"
ABLATIONS = {
    "full": {},
    "no_cross_module_imports": {"cross_module": False},
    "no_container_path_remap": {"path_remap": False},
    "no_helper_call_inlining": {"inline_calls": False},
    "single_file_literals_only": {"cross_module": False, "path_remap": False, "inline_calls": False},
}
GIT_ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_LFS_SKIP_SMUDGE": "1"}


def git(*args: str, cwd: Path | None = None) -> str:
    return subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=never", *args],
                          cwd=cwd, env=GIT_ENV, check=True, capture_output=True, text=True).stdout.strip()


def fetch() -> None:
    for base, repos in ((WORK, REPOS), (WORK_HELDOUT, HELDOUT)):
        base.mkdir(parents=True, exist_ok=True)
        for r in repos:
            _fetch_one(base / r["name"], r)


def _fetch_one(dest: Path, r: dict) -> None:
    """Shallow fetch of exactly the pinned commit (data only: hooks off, no submodules, no LFS)."""
    if not dest.exists():
        git("init", "-q", str(dest))
        git("remote", "add", "origin", r["url"], cwd=dest)
    git("fetch", "-q", "--depth", "1", "--no-tags", "origin", r["sha"], cwd=dest)
    git("checkout", "-q", "--detach", "FETCH_HEAD", cwd=dest)
    print(r["name"], git("rev-parse", "HEAD", cwd=dest))


def _portable(scan_out: dict, root: Path | None = None) -> dict:
    """Drop machine-specific absolute paths so results/ is reproducible across machines."""
    text = json.dumps(dict(scan_out, workspace="<repo root>"))
    for prefix in filter(None, [str(root.resolve()) if root else None, str(HERE)]):
        text = text.replace(prefix, "<repo root>" if root and prefix == str(root.resolve()) else "<spike>")
    return json.loads(text)


def run() -> int:
    (RESULTS / "scans").mkdir(parents=True, exist_ok=True)
    summary: dict = {"spike": "S3 static DAG analysis", "question": (
        "Does AST-only scanning give correct task-to-dbt-model links on 3 public example DAG repositories?"),
        "repos": {}, "totals": {}, "ablations": {}}
    totals: dict[str, int] = {}
    for r in REPOS:
        root = WORK / r["name"]
        if not root.exists():
            print(f"missing {root}; run: uv run python scripts/run_s3.py fetch", file=sys.stderr)
            return 1
        head = git("rev-parse", "HEAD", cwd=root)
        gt = json.loads((RESULTS / "ground_truth" / f"{r['name']}.json").read_text(encoding="utf-8"))
        per_ablation = {}
        for name, opts in ABLATIONS.items():
            out = scan([str(root / p) for p in r["paths"]], workspace=str(root), **opts)
            ev = evaluate(gt, out)
            per_ablation[name] = ev["metrics"]
            if name == "full":
                full_scan, full_eval = out, ev
        (RESULTS / "scans" / f"{r['name']}.json").write_text(json.dumps(_portable(full_scan, root), indent=1), encoding="utf-8")
        summary["repos"][r["name"]] = {
            "url": r["url"].removesuffix(".git"), "sha": head, "sha_pinned": r["sha"], "sha_ok": head == r["sha"],
            "scan_paths": r["paths"],
            "metrics": full_eval["metrics"],
            "misses": full_eval["misses"],
            "false_positives": full_eval["false_positives"],
            "unexpected_unknown_links": full_eval["unexpected_unknown_links"],
            "out_of_scope_dbt_links": full_eval["out_of_scope_dbt_links"],
            "unresolved_review": _portable({"x": review_unresolved(gt, full_scan)}, root)["x"],
            "rows": full_eval["rows"],
        }
        summary["ablations"][r["name"]] = per_ablation
        for k in ("dag_files_scanned", "expected_links", "expected_resolvable", "expected_unknown", "found", "correct",
                  "misses", "legit_unknowns", "false_positives", "unexpected_unknown_links", "task_id_checks",
                  "task_id_checks_passed"):
            totals[k] = totals.get(k, 0) + full_eval["metrics"][k]
    totals["miss_rate"] = round(totals["misses"] / totals["expected_links"], 4)
    totals["correct_rate_resolvable"] = round(totals["correct"] / totals["expected_resolvable"], 4)
    summary["totals"] = totals
    agg: dict[str, dict[str, int]] = {}
    for per in summary["ablations"].values():
        for name, m in per.items():
            a = agg.setdefault(name, {"correct": 0, "misses": 0, "expected_links": 0})
            a["correct"] += m["correct"]
            a["misses"] += m["misses"]
            a["expected_links"] += m["expected_links"]
    summary["ablation_totals"] = agg
    summary["heldout"] = {"note": "current scanner on the held-out repos; the frozen-scanner result is in results/heldout_frozen_run.json", "repos": {}}
    ht: dict[str, int] = {}
    for r in HELDOUT:
        root = WORK_HELDOUT / r["name"]
        if not root.exists():
            print(f"missing {root}; run: uv run python scripts/run_s3.py fetch", file=sys.stderr)
            return 1
        gt = json.loads((RESULTS / "ground_truth" / "heldout" / f"{r['name']}.json").read_text(encoding="utf-8"))
        out = scan([str(root / p) for p in r["paths"]], workspace=str(root))
        ev = evaluate(gt, out)
        (RESULTS / "scans" / f"heldout_{r['name']}.json").write_text(json.dumps(_portable(out, root), indent=1), encoding="utf-8")
        summary["heldout"]["repos"][r["name"]] = {
            "url": r["url"].removesuffix(".git"), "sha": git("rev-parse", "HEAD", cwd=root), "sha_pinned": r["sha"],
            "metrics": ev["metrics"], "misses": ev["misses"], "false_positives": ev["false_positives"],
            "unresolved_review": review_unresolved(gt, out), "rows": ev["rows"],
        }
        for k in ("dag_files_scanned", "expected_links", "expected_resolvable", "correct", "misses", "legit_unknowns", "false_positives"):
            ht[k] = ht.get(k, 0) + ev["metrics"][k]
    summary["heldout"]["totals"] = ht
    frozen = RESULTS / "heldout_frozen_run.json"
    if frozen.exists():
        summary["heldout"]["frozen_run"] = json.loads(frozen.read_text(encoding="utf-8"))["totals"]
    sel = RESULTS / "selector_agreement.json"
    if sel.exists():
        summary["selector_agreement"] = json.loads(sel.read_text(encoding="utf-8"))["summary"]
    (RESULTS / "s3_results.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    print(json.dumps({"totals": totals, "heldout_totals": ht, "ablation_totals": agg,
                      "per_repo": {k: v["metrics"] for k, v in summary["repos"].items()}}, indent=1))
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "fetch":
        fetch()
        raise SystemExit(0)
    raise SystemExit(run())
