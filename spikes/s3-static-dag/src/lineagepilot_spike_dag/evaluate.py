"""Score a scan against hand-labelled ground truth (``results/ground_truth/<repo>.json``).

A link is **correct** when the scanner found it with the right DAG, the right task or group,
the right dbt project, status ``resolved`` and a model set equal to the hand-labelled set.
Everything else that was expected is a **miss**, with a reason.
"""

from __future__ import annotations

from typing import Any


def expected_models(e: dict[str, Any]) -> list[str]:
    pkg = e.get("package")
    out = []
    for m in e.get("models", []):
        out.append(m if m.startswith("model.") else f"model.{pkg}.{m}")
    return sorted(out)


def _identity(e: dict[str, Any], link: dict[str, Any]) -> bool:
    if link["location"]["file"] != e["file"] or link.get("dag_id") != e.get("dag_id"):
        return False
    unit = e.get("unit")
    if unit == "cosmos_dag":
        return link["kind"] == "cosmos_dag"
    if unit == "cosmos_task_group":
        return link["kind"] == "cosmos_task_group" and link.get("group_id") == e.get("group_id")
    if unit == "task":
        if "command_index" in e and link.get("command_index", 0) != e["command_index"]:
            return False
        return link.get("task_id") == e.get("task_id") and link.get("dbt_command") == e.get("dbt_command")
    return False


def _lines(link: dict[str, Any]) -> set[int]:
    out = {link["location"]["line"]}
    if link.get("constructed_at"):
        out.add(link["constructed_at"]["line"])
    for cs in link.get("via_call_at") or []:
        out.add(cs[0])
    return out


def _predicted_task_ids(link: dict[str, Any]) -> list[str] | None:
    if link.get("tasks") is None:
        return None
    ids = [t for row in link["tasks"] for t in row["task_ids"]]
    ids += [x["task_id"] for x in link.get("extra_tasks") or [] if "producer" not in x.get("runs", "")]
    return sorted(ids)


def evaluate(gt: dict[str, Any], scan: dict[str, Any]) -> dict[str, Any]:
    links = scan["links"]
    unresolved = scan["unresolved"]
    used: set[int] = set()
    rows: list[dict[str, Any]] = []
    for e in gt["links"]:
        exp_lines = set(e.get("lines") or [e.get("line")])
        row: dict[str, Any] = {"id": e["id"], "file": e["file"], "dag_id": e.get("dag_id"), "expect": e["expect"],
                               "task_or_group": e.get("group_id") or e.get("task_id") or e.get("unit")}
        match = next((i for i, l in enumerate(links) if i not in used and _identity(e, l)), None)
        if e["expect"] == "unknown":
            at_line = [i for i, l in enumerate(links) if l["location"]["file"] == e["file"] and _lines(l) & exp_lines]
            if match is not None:
                at_line = [match] + [i for i in at_line if i != match]
            resolved_guess = [i for i in at_line if links[i]["status"] == "resolved" and links[i]["in_scope"]]
            flagged_links = [i for i in at_line if links[i]["status"] == "unknown"]
            flagged_unres = [u for u in unresolved if u["file"] == e["file"] and u["line"] in exp_lines]
            for i in at_line:
                used.add(i)
            if resolved_guess and not flagged_links:
                row.update(outcome="miss", reason="resolved to a guess where only unknown is correct",
                           got_models=links[resolved_guess[0]]["resolved_models"])
            elif flagged_links or flagged_unres:
                reasons = [r for i in flagged_links for r in links[i]["unknown_reasons"]] + [u["reason"] for u in flagged_unres]
                row.update(outcome="unknown_legit" if e.get("legit") else "unknown_not_legit",
                           scanner_reasons=list(dict.fromkeys(reasons))[:4])
            else:
                row.update(outcome="miss", reason="silently dropped: no link and no unresolved record")
            rows.append(row)
            continue
        exp = expected_models(e)
        row["expected_models"] = exp
        if match is None:
            same_line = [i for i, l in enumerate(links) if i not in used and l["location"]["file"] == e["file"] and _lines(l) & exp_lines]
            flagged = [u for u in unresolved if u["file"] == e["file"] and u["line"] in exp_lines]
            if same_line:
                l = links[same_line[0]]
                used.add(same_line[0])
                row.update(outcome="miss", reason="found with the wrong identity",
                           got={"dag_id": l.get("dag_id"), "task_id": l.get("task_id"), "group_id": l.get("group_id"),
                                "dbt_command": l.get("dbt_command")})
            elif flagged:
                row.update(outcome="miss", reason="only an unresolved record: " + flagged[0]["reason"][:200])
            else:
                row.update(outcome="miss", reason="not found (no link and no unresolved record)")
            rows.append(row)
            continue
        used.add(match)
        l = links[match]
        row["confidence"] = l["confidence"]
        if l["status"] != "resolved":
            row.update(outcome="miss", reason="unknown: " + "; ".join(l["unknown_reasons"])[:300])
        elif (l.get("project_path") or None) != e.get("project_path"):
            row.update(outcome="miss", reason=f"wrong project: got {l.get('project_path')}, expected {e.get('project_path')}")
        elif sorted(l["resolved_models"]) != exp:
            got = sorted(l["resolved_models"])
            row.update(outcome="miss", reason="wrong model set", missing=sorted(set(exp) - set(got)), extra=sorted(set(got) - set(exp)))
        else:
            row["outcome"] = "correct"
        if "task_ids" in e:
            pred = _predicted_task_ids(l)
            row["task_ids_match"] = pred is not None and pred == sorted(e["task_ids"])
            if not row["task_ids_match"]:
                row["task_ids_diff"] = {"missing": sorted(set(e["task_ids"]) - set(pred or [])), "extra": sorted(set(pred or []) - set(e["task_ids"]))}
        rows.append(row)

    in_scope_unmatched = [i for i, l in enumerate(links) if i not in used and l["in_scope"]]
    false_pos = [links[i] for i in in_scope_unmatched if links[i]["status"] == "resolved"]
    extra_unknown = [links[i] for i in in_scope_unmatched if links[i]["status"] == "unknown"]
    out_of_scope = [l for l in links if not l["in_scope"]]

    expected = len(rows)
    resolvable = [r for r in rows if r["expect"] == "resolved"]
    correct = sum(r["outcome"] == "correct" for r in rows)
    misses = [r for r in rows if r["outcome"] == "miss"]
    legit = [r for r in rows if r["outcome"] == "unknown_legit"]
    found = sum(1 for r in rows if r["outcome"] in ("correct", "unknown_legit", "unknown_not_legit")
                or r.get("reason", "").startswith(("unknown:", "wrong", "found with")))
    task_checks = [r for r in rows if "task_ids_match" in r]
    metrics = {
        "dag_files_scanned": scan["stats"]["files_scanned"],
        "expected_links": expected,
        "expected_resolvable": len(resolvable),
        "expected_unknown": expected - len(resolvable),
        "found": found,
        "correct": correct,
        "misses": len(misses),
        "legit_unknowns": len(legit),
        "false_positives": len(false_pos),
        "unexpected_unknown_links": len(extra_unknown),
        "out_of_scope_dbt_links": len(out_of_scope),
        "miss_rate": round(len(misses) / expected, 4) if expected else None,
        "miss_rate_resolvable": round(sum(1 for r in misses if r["expect"] == "resolved") / len(resolvable), 4) if resolvable else None,
        "correct_rate_resolvable": round(sum(1 for r in resolvable if r["outcome"] == "correct") / len(resolvable), 4) if resolvable else None,
        "task_id_checks": len(task_checks),
        "task_id_checks_passed": sum(1 for r in task_checks if r["task_ids_match"]),
        "confidence_of_correct": _count(r.get("confidence") for r in rows if r["outcome"] == "correct"),
        "dependency_edges": len(scan["dependencies"]),
        "unresolved_records": len(unresolved),
    }
    return {
        "metrics": metrics,
        "rows": rows,
        "misses": misses,
        "false_positives": [_brief(l) for l in false_pos],
        "unexpected_unknown_links": [_brief(l) for l in extra_unknown],
        "out_of_scope_dbt_links": [_brief(l) for l in out_of_scope],
    }


def _count(it) -> dict[str, int]:
    out: dict[str, int] = {}
    for x in it:
        out[str(x)] = out.get(str(x), 0) + 1
    return out


def _brief(l: dict[str, Any]) -> dict[str, Any]:
    return {"file": l["location"]["file"], "line": l["location"]["line"], "dag_id": l.get("dag_id"),
            "task_or_group": l.get("group_id") or l.get("task_id"), "dbt_command": l.get("dbt_command"),
            "status": l["status"], "models": l.get("resolved_models"), "reasons": l.get("unknown_reasons")}


def review_unresolved(gt: dict[str, Any], scan: dict[str, Any]) -> list[dict[str, Any]]:
    """Join every unresolved record with the hand review in the ground truth (``unresolved_review``)."""
    reviews = gt.get("unresolved_review") or []
    out = []
    for u in scan["unresolved"]:
        rv = next((r for r in reviews if r["file"] == u["file"] and r["line"] == u["line"]), None)
        out.append({"file": u["file"], "line": u["line"], "category": u.get("category"), "reason": u["reason"],
                    "legit": rv.get("legit") if rv else None, "review": rv.get("why") if rv else "not reviewed"})
    return out
