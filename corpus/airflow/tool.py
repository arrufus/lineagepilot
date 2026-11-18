"""What is particular to scoring the Airflow reader (task 5.5; ../score.py runs it, task 7.9).

A link is **correct** when the scanner found it with the right DAG, the right task or group, the
right dbt project, status ``resolved`` and a model set equal to the labelled one (spike S3's
definition). Everything else that was expected is a **miss**. Seeds and snapshots are scored
separately, so a wrong one is reported without changing what S3's number means.

Each repository is scanned twice: in the folders its label names, which measures the scanner, and
with the default ``**/dags``, which measures what a user gets without touching a setting.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# What decides the answer, under lineagepilot_core: the scanner, the shared reading code, and the
# two modules it leans on. Paths are part of the hash, so the move to readers/ in task 7.2 gave a
# new hash for the same code.
HASH_FILES = (
    "readers/airflow/*.py",
    "readers/common/*.py",
    "parsers/sql_parser.py",
    "rules/safe_regex.py",
)
# What the results are broken down by: a label's operator
GROUP = "operator"


def _labelled_folders(root: Path, label: dict[str, Any]) -> dict[str, Any]:
    from lineagepilot_core.readers.airflow.scan import scan_workspace

    return scan_workspace(root, label["scan"]["paths"])


def _default_folders(root: Path, label: dict[str, Any]) -> dict[str, Any]:
    from lineagepilot_core.readers.airflow.dag_static import DEFAULT_DAG_FOLDERS
    from lineagepilot_core.readers.airflow.scan import scan_workspace

    return scan_workspace(root, DEFAULT_DAG_FOLDERS)


# Each repository's scans, the first in full in the results, the others by their metrics
SCANS = {"labelled_folders": _labelled_folders, "default_folders": _default_folders}


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
    """Each labelled link's outcome, what was found that no label expects, and the scan's counts."""
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
    return {
        "rows": rows,
        "false_positives": [_brief(x) for x in unmatched if x["status"] == "resolved"],
        "unexpected_unknown_links": [_brief(x) for x in unmatched if x["status"] == "unknown"],
        "stats": {"dag_files": scan["stats"]["files_scanned"], "unresolved_records": scan["stats"]["unresolved"]},
    }


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
