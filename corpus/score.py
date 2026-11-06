"""Score the frozen scanner on the held-out corpus (task 5.5, the Phase 5 exit).

    python corpus/score.py fetch             fetch the pinned commits into corpus/work, as data only
    python corpus/score.py run NAME          scan, score, and write corpus/results/NAME.json
    python corpus/score.py hash              the scanner's hash, to compare with a recorded run

Run with lineagepilot_core importable:

    UV_PYTHON_PREFERENCE=only-managed uv run --project packages/core python corpus/score.py run first

The repositories are data. They are fetched at a pinned commit with hooks off, without submodules
or LFS, and read only as text and syntax. Nothing in them is imported, executed or installed.

A link is **correct** when the scanner found it with the right DAG, the right task or group, the
right dbt project, status ``resolved`` and a model set equal to the labelled one (spike S3's
definition). Everything else that was expected is a **miss**. Seeds and snapshots are scored
separately, so a wrong one is reported without changing what S3's number means.

Each repository is scanned twice: in the folders its label names, which measures the scanner, and
with the default ``**/dags``, which measures what a user gets without touching a setting.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
LABELS = HERE / "labels"
WORK = HERE / "work"
RESULTS = HERE / "results"
CORE = REPO / "packages" / "core" / "src" / "lineagepilot_core"
# What decides the answer: the scanner, and the two modules it leans on
SCANNER_FILES = (*sorted((CORE / "airflow").glob("*.py")), CORE / "parsers" / "sql_parser.py", CORE / "rules" / "safe_regex.py")


def scanner_hash() -> str:
    digest = hashlib.sha256()
    for path in SCANNER_FILES:
        # As git stores them, so a CRLF checkout gives the same hash
        digest.update(path.relative_to(CORE).as_posix().encode())
        digest.update(path.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


def commit() -> str:
    done = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True)
    return done.stdout.strip()


def labels() -> list[dict[str, Any]]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(LABELS.glob("*.json"))]


def checkout(label: dict[str, Any]) -> Path:
    return WORK / label["repo"].replace("/", "_")


def fetch() -> int:
    for label in labels():
        target = checkout(label)
        if target.exists():
            continue
        target.mkdir(parents=True)
        for args in (
            ["init", "-q"],
            ["remote", "add", "origin", label["url"]],
            ["-c", "core.hooksPath=/dev/null", "fetch", "-q", "--depth", "1", "origin", label["sha"]],
            ["-c", "core.hooksPath=/dev/null", "-c", "filter.lfs.smudge=", "-c", "filter.lfs.required=false", "checkout", "-q", "FETCH_HEAD"],
        ):
            subprocess.run(["git", *args], cwd=target, check=True)
    return 0


# Scoring


def expected_models(entry: dict[str, Any]) -> list[str]:
    package = entry.get("package")
    return sorted(m if m.startswith("model.") else f"model.{package}.{m}" for m in entry.get("models", []))


def _identity(entry: dict[str, Any], link: dict[str, Any]) -> bool:
    if link["location"]["file"] != entry["file"] or link.get("dag_id") != entry.get("dag_id"):
        return False
    unit = entry.get("unit")
    if unit == "cosmos_dag":
        return link["kind"] == "cosmos_dag"
    if unit == "cosmos_task_group":
        return link["kind"] == "cosmos_task_group" and link.get("group_id") == entry.get("group_id")
    if unit == "task":
        if entry.get("command_index") and link.get("command_index", 0) != entry["command_index"]:
            return False
        return link.get("task_id") == entry.get("task_id") and link.get("dbt_command") == entry.get("dbt_command")
    return False


def _lines(link: dict[str, Any]) -> set[int]:
    found = {link["location"]["line"]}
    if link.get("constructed_at"):
        found.add(link["constructed_at"]["line"])
    for site in link.get("via_call_at") or []:
        found.add(site[0])
    return found


def score(label: dict[str, Any], scan: dict[str, Any]) -> dict[str, Any]:
    links = scan["links"]
    unresolved = scan["unresolved"]
    used: set[int] = set()
    rows: list[dict[str, Any]] = []
    for entry in label["links"]:
        lines = set(entry.get("lines") or []) | {entry.get("line")}
        row: dict[str, Any] = {
            "id": entry["id"],
            "file": entry["file"],
            "operator": entry.get("operator"),
            "expect": entry["expect"],
            "unsure": str(entry.get("reason", "")).startswith("UNSURE:"),
        }
        match = next((i for i, x in enumerate(links) if i not in used and _identity(entry, x)), None)
        here = [i for i, x in enumerate(links) if x["location"]["file"] == entry["file"] and _lines(x) & lines]
        flagged = [u for u in unresolved if u["file"] == entry["file"] and u["line"] in lines]
        if entry["expect"] == "unknown":
            candidates = ([match] if match is not None else []) + [i for i in here if i != match]
            guesses = [i for i in candidates if links[i]["status"] == "resolved" and links[i]["in_scope"]]
            unknown_links = [i for i in candidates if links[i]["status"] == "unknown"]
            used.update(candidates)
            if guesses and not unknown_links:
                row.update(outcome="miss", reason="resolved to a guess where only unknown is correct", got=links[guesses[0]]["resolved_models"])
            elif unknown_links or flagged:
                row.update(outcome="unknown_legit" if entry.get("legit") else "unknown_not_legit")
                row["category"] = next((u.get("category") for u in flagged), None)
            else:
                row.update(outcome="miss", reason="silently dropped: no link and no unresolved record")
            rows.append(row)
            continue
        expected = expected_models(entry)
        row["expected_models"] = len(expected)
        if match is None:
            same_place = [i for i in here if i not in used]
            if same_place:
                link = links[same_place[0]]
                used.add(same_place[0])
                row.update(
                    outcome="miss",
                    reason="found with the wrong identity",
                    got={k: link.get(k) for k in ("dag_id", "task_id", "group_id", "dbt_command", "command_index")},
                )
            elif flagged:
                row.update(outcome="miss", reason="only an unresolved record", category=flagged[0].get("category"), detail=flagged[0]["reason"][:300])
            else:
                row.update(outcome="miss", reason="not found: no link and no unresolved record")
            rows.append(row)
            continue
        used.add(match)
        link = links[match]
        row["confidence"] = link["confidence"]
        if link["status"] != "resolved":
            row.update(outcome="miss", reason="unknown", category=link.get("category"), detail="; ".join(link["unknown_reasons"])[:300])
        elif (link.get("project_path") or None) != entry.get("project_path"):
            row.update(outcome="miss", reason="wrong project", got=link.get("project_path"), expected=entry.get("project_path"))
        elif sorted(link["resolved_models"]) != expected:
            got = set(link["resolved_models"])
            row.update(outcome="miss", reason="wrong model set", missing=sorted(set(expected) - got), extra=sorted(got - set(expected)))
        else:
            row["outcome"] = "correct"
        if link["status"] == "resolved" and "other_nodes" in entry:
            models = set(link["resolved_models"])
            others = {n for n in link["resolved_nodes"] if n not in models}
            row["other_nodes_match"] = others == set(entry["other_nodes"] or [])
            if not row["other_nodes_match"]:
                row["other_nodes_diff"] = {"missing": sorted(set(entry["other_nodes"] or []) - others), "extra": sorted(others - set(entry["other_nodes"] or []))}
        rows.append(row)
    unmatched = [links[i] for i, x in enumerate(links) if i not in used and x["in_scope"]]
    false_positives = [_brief(x) for x in unmatched if x["status"] == "resolved"]
    extra_unknown = [_brief(x) for x in unmatched if x["status"] == "unknown"]
    return {"rows": rows, "false_positives": false_positives, "unexpected_unknown_links": extra_unknown, "metrics": metrics(rows, false_positives, extra_unknown, scan)}


def _brief(link: dict[str, Any]) -> dict[str, Any]:
    return {
        "file": link["location"]["file"],
        "line": link["location"]["line"],
        "dag_id": link.get("dag_id"),
        "task_or_group": link.get("group_id") or link.get("task_id"),
        "dbt_command": link.get("dbt_command"),
        "status": link["status"],
        "models": len(link.get("resolved_models") or []),
        "reasons": link.get("unknown_reasons"),
    }


def metrics(rows: list[dict[str, Any]], false_positives: list, extra_unknown: list, scan: dict[str, Any] | None = None) -> dict[str, Any]:
    resolvable = [r for r in rows if r["expect"] == "resolved"]
    misses = [r for r in rows if r["outcome"] == "miss"]
    confidence: dict[str, int] = {}
    for row in rows:
        if row["outcome"] == "correct":
            confidence[row["confidence"]] = confidence.get(row["confidence"], 0) + 1
    found: dict[str, Any] = {
        "expected_links": len(rows),
        "expected_resolvable": len(resolvable),
        "expected_unknown": len(rows) - len(resolvable),
        "correct": sum(r["outcome"] == "correct" for r in rows),
        "misses": len(misses),
        "legit_unknowns": sum(r["outcome"] == "unknown_legit" for r in rows),
        "false_positives": len(false_positives),
        "unexpected_unknown_links": len(extra_unknown),
        "miss_rate": round(len(misses) / len(rows), 4) if rows else None,
        "other_nodes_wrong": sum(r.get("other_nodes_match") is False for r in rows),
        "confidence_of_correct": confidence,
    }
    if scan is not None:
        found["dag_files"] = scan["stats"]["files_scanned"]
        found["unresolved_records"] = scan["stats"]["unresolved"]
    return found


def pooled(results: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [r for result in results for r in result["rows"]]
    false_positives = [f for result in results for f in result["false_positives"]]
    extra = [u for result in results for u in result["unexpected_unknown_links"]]
    summary = metrics(rows, false_positives, extra)
    certain = [r for r in rows if not r["unsure"]]
    summary["without_unsure_labels"] = metrics(certain, false_positives, extra)
    by_operator: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_operator.setdefault(row["operator"] or "?", []).append(row)
    summary["by_operator"] = {
        operator: {k: v for k, v in metrics(found, [], []).items() if k in ("expected_links", "correct", "misses", "miss_rate")}
        for operator, found in sorted(by_operator.items())
    }
    reasons: dict[str, int] = {}
    for row in rows:
        if row["outcome"] == "miss":
            reasons[row["reason"]] = reasons.get(row["reason"], 0) + 1
    summary["miss_reasons"] = dict(sorted(reasons.items(), key=lambda kv: -kv[1]))
    return summary


def run(name: str) -> int:
    from lineagepilot_core.airflow.dag_static import DEFAULT_DAG_FOLDERS
    from lineagepilot_core.airflow.workspace import scan_workspace

    sets: dict[str, list[dict[str, Any]]] = {"labelled_folders": [], "default_folders": []}
    repos = []
    for label in labels():
        root = checkout(label)
        labelled = score(label, scan_workspace(root, label["scan"]["paths"]))
        default = score(label, scan_workspace(root, DEFAULT_DAG_FOLDERS))
        sets["labelled_folders"].append(labelled)
        sets["default_folders"].append(default)
        repos.append({"repo": label["repo"], "sha": label["sha"], "labelled_folders": labelled, "default_folders": {"metrics": default["metrics"]}})
    output = {
        "scanner_sha256": scanner_hash(),
        "commit": commit(),
        "summary": {key: pooled(found) for key, found in sets.items()},
        "repos": repos,
    }
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / f"{name}.json").write_text(json.dumps(output, indent=1) + "\n", encoding="utf-8", newline="\n")
    sys.stdout.write(json.dumps({"scanner_sha256": output["scanner_sha256"], **output["summary"]}, indent=1) + "\n")
    return 0


def main() -> int:
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "fetch":
        return fetch()
    if command == "hash":
        sys.stdout.write(scanner_hash() + "\n")
        return 0
    if command == "run" and len(sys.argv) == 3:
        return run(sys.argv[2])
    sys.stderr.write(__doc__ or "")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
