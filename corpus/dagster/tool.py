"""What is particular to scoring the Dagster reader (task 8.9; ../score.py runs it).

A link is **correct** when the reader found the unit, with the right name in the right file, for
the right dbt project, resolved, and with a model set equal to the labelled one: spike S3's
definition, as for Airflow. Seeds and snapshots are scored separately, as ``other_nodes``, so a
wrong one is reported without changing what the miss rate means.

The units are the labels' (README.md): ``dbt_assets`` is one of the reader's dbt asset
definitions, ``job`` and ``schedule`` its jobs and schedules, ``asset`` a non-dbt asset downstream
of dbt, and ``dbt_cli`` the unknown it gives for Python that runs dbt itself
(``python_dbt_call``). A unit is the same one when its file and name agree, the name as the
labels give it: a component's folder, and an asset's key joined with ``/`` where it has a prefix.

Lines are not part of a unit's identity. A label gives a decorated function's decorator, and a
call's first line; the reader may give the ``def`` line, or a line inside the call. So a record
is at a label's place when its line is in the statement that starts at the label's line,
decorators included, read as syntax. That decides only a miss's reason, and whether what the
reader found there is also a false positive. An unknown at a place answers every label there:
the units a loop makes share one line.

Each repository is scanned once for each of its dbt projects, as the server reads one project at
a time, and the records are pooled. It is scanned twice: in the folders its label names, which
measures the reader, and with the default, the whole workspace, which is what a user gets without
touching a setting.

Frozen on 22 November 2026 at be8453e (0.4.0's reader, after task 8.9's fixtures):
reader_sha256 ecb8dfdaebd77dd79e6a8c21e914b08614e5a466bd91240abaaa9961882d3136
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

# What decides the answer, under lineagepilot_core: the reader, the shared reading code, and the
# parsers they read YAML, JSON and the annotation file with. The linker is not scored: the scan's
# records are what the labels describe.
HASH_FILES = (
    "readers/dagster/*.py",
    "readers/common/*.py",
    "parsers/safe_yaml.py",
    "parsers/_json.py",
    "parsers/data_flow_annotations.py",
)
# What the results are broken down by: a label's unit
GROUP = "unit"

# Which of the scan's lists each unit is in
_LISTS = {
    "dbt_assets": ("definitions", "unknowns"),
    "job": ("jobs",),
    "schedule": ("schedules",),
    "asset": ("assets",),
    "dbt_cli": ("located",),
}
# A Python file larger than this is not parsed for places: its labels' places are their lines
_MAX_PARSE = 2 * 1024 * 1024
_INDEX = re.compile(r"\[\d+\]$")


def _scan(root: Path, label: dict[str, Any], paths: Any) -> dict[str, Any]:
    """The reader's records for every dbt project in the repository, each tagged with its
    project. A record two projects' scans both give, such as an unknown, is kept once."""
    from lineagepilot_core.readers.dagster.scan import scan_dagster

    projects = label.get("dbt_projects") or ["."]
    records: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    files: set[str] = set()
    problems: list[str] = []
    for project in projects:
        found = scan_dagster(root / project, root, paths)
        files.update(_file_names(found["files"]))
        problems += [p for p in found["problems"] if p not in problems]
        for name in _LISTS_IN_ORDER:
            for record in found[name]:
                key = json.dumps({"list": name, **record}, sort_keys=True)
                if key in seen:
                    seen[key]["projects"].append(_norm(project))
                    continue
                kept = {
                    **record,
                    "list": name,
                    "project": _norm(project),
                    "projects": [_norm(project)],
                }
                seen[key] = kept
                records.append(kept)
    return {"root": root, "records": records, "files": sorted(files), "problems": problems}


_LISTS_IN_ORDER = ("definitions", "unknowns", "jobs", "schedules", "assets", "located")


def _file_names(files: dict[str, list[str]]) -> list[str]:
    """The files discovery found: components, pyproject.toml and workspace files, and Python."""
    return [f for group in files.values() for f in group]


def _labelled_paths(root: Path, label: dict[str, Any]) -> dict[str, Any]:
    return _scan(root, label, label["scan"]["paths"])


def _default_paths(root: Path, label: dict[str, Any]) -> dict[str, Any]:
    from lineagepilot_core.readers.dagster.discovery import DEFAULT_PATHS

    return _scan(root, label, DEFAULT_PATHS)


# Each repository's scans, the first in full in the results, the others by their metrics
SCANS = {"labelled_paths": _labelled_paths, "default_paths": _default_paths}


def _norm(path: str | None) -> str:
    text = (path or ".").replace("\\", "/").strip("/")
    while text.startswith("./"):
        text = text[2:]
    return text or "."


def _node_id(kind: str, name: str, package: str | None) -> str:
    return name if name.count(".") >= 2 else f"{kind}.{package}.{name.split('.', 1)[-1]}"


def expected_models(entry: dict[str, Any]) -> list[str]:
    package = entry.get("package")
    return sorted(
        m if m.startswith("model.") else f"model.{package}.{m}" for m in entry.get("models") or []
    )


def expected_others(entry: dict[str, Any]) -> list[str]:
    package = entry.get("package")
    return sorted(_node_id(n.split(".", 1)[0], n, package) for n in entry.get("other_nodes") or [])


def _names(record: dict[str, Any]) -> set[str]:
    """The names a record answers to: its own, without a component's ``[i]``, a component's last
    folder, and an asset's key joined with ``/``."""
    name = _INDEX.sub("", str(record.get("name") or ""))
    found = {name}
    if record.get("api") == "DbtProjectComponent":
        found.add(re.split(r"[/.]", name)[-1])
    if record.get("asset_key"):
        found.add("/".join(record["asset_key"]))
    return found


def _identity(entry: dict[str, Any], record: dict[str, Any]) -> bool:
    if record["list"] not in _LISTS.get(entry.get("unit"), ()) or record["file"] != entry["file"]:
        return False
    if entry["unit"] == "dbt_cli" and record.get("category") != "python_dbt_call":
        return False
    wanted = (
        {entry.get("name"), re.split(r"/", str(entry.get("name")))[-1]}
        if entry.get("unit") == "dbt_assets"
        else {entry.get("name")}
    )
    return bool(_names(record) & wanted)


class _Places:
    """Each label's place: the lines of the statement that starts at its line."""

    def __init__(self, root: Path):
        self.root = root
        self.trees: dict[str, ast.Module | None] = {}

    def lines(self, file: str, line: int) -> set[int]:
        tree = self._tree(file)
        if tree is None:
            return {line}
        spans = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.stmt, ast.Call)):
                continue
            first = min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])])
            if first == line:
                spans.append(
                    (isinstance(node, ast.Call), node.end_lineno - first, first, node.end_lineno)
                )
        if not spans:
            return {line}
        # A statement before a call; the smallest of either
        _, _, first, last = min(spans)
        return set(range(first, (last or first) + 1))

    def _tree(self, file: str) -> ast.Module | None:
        if file not in self.trees:
            self.trees[file] = None
            path = self.root / file
            if file.endswith(".py") and path.is_file() and path.stat().st_size <= _MAX_PARSE:
                try:
                    self.trees[file] = ast.parse(path.read_bytes())
                except (SyntaxError, ValueError):
                    pass
        return self.trees[file]


def score(label: dict[str, Any], scan: dict[str, Any]) -> dict[str, Any]:
    """Each labelled link's outcome, what was found that no label expects, and the scan's counts.

    An unknown the reader puts on a whole code location, where it found no asset to put it on,
    is still at a line: it answers a label at that place. One that answers none is no unit's,
    so it is counted, not listed with the unexpected unknowns."""
    records = scan["records"]
    places = _Places(scan["root"])
    used: set[int] = set()
    unknown_cited: set[int] = set()
    rows: list[dict[str, Any]] = []
    for entry in label["links"]:
        lines = places.lines(entry["file"], entry["line"]) | {entry["line"]}
        row: dict[str, Any] = {
            "id": entry["id"],
            "file": entry["file"],
            "line": entry["line"],
            "unit": entry.get("unit"),
            "api": entry.get("api"),
            "expect": entry["expect"],
            "unsure": str(entry.get("reason", "")).startswith("UNSURE:"),
        }
        matches = [i for i, r in enumerate(records) if _identity(entry, r)]
        free = [i for i in matches if i not in used or records[i]["status"] == "unknown"]
        # The match for this label's project, or else any
        match = next(
            (i for i in free if records[i]["project"] == _norm(entry.get("project_path"))),
            free[0] if free else None,
        )
        here = [
            i
            for i, r in enumerate(records)
            if r["file"] == entry["file"] and r["line"] in lines and i != match
        ]
        if entry["expect"] == "unknown":
            candidates = ([match] if match is not None else []) + here
            guesses = [
                i for i in candidates if records[i]["status"] == "resolved" and i not in used
            ]
            unknowns = [i for i in candidates if records[i]["status"] == "unknown"]
            used.update(guesses)
            unknown_cited.update(unknowns)
            if guesses and not unknowns:
                row.update(
                    outcome="miss",
                    reason="resolved to a guess where only unknown is correct",
                    got=records[guesses[0]].get("nodes"),
                )
            elif unknowns:
                row.update(outcome="unknown_legit", category=records[unknowns[0]].get("category"))
            else:
                row.update(outcome="miss", reason="silently dropped: no record and no unknown")
            rows.append(row)
            continue
        expected = expected_models(entry)
        row["expected_models"] = len(expected)
        if match is None:
            resolved_here = [
                i for i in here if records[i]["status"] == "resolved" and i not in used
            ]
            unknown_here = [i for i in here if records[i]["status"] == "unknown"]
            if resolved_here:
                found = records[resolved_here[0]]
                used.add(resolved_here[0])
                row.update(
                    outcome="miss",
                    reason="found with the wrong identity",
                    got={k: found.get(k) for k in ("list", "name", "api", "line")},
                )
            elif unknown_here:
                found = records[unknown_here[0]]
                unknown_cited.add(unknown_here[0])
                row.update(
                    outcome="miss",
                    reason="only an unknown at its place",
                    category=found.get("category"),
                    detail=str(found.get("reason"))[:300],
                )
            else:
                row.update(outcome="miss", reason="not found: no record and no unknown")
            rows.append(row)
            continue
        found = records[match]
        row["confidence"] = found.get("confidence")
        if found["status"] != "resolved":
            unknown_cited.add(match)
            row.update(
                outcome="miss",
                reason="unknown",
                category=found.get("category"),
                detail=str(found.get("reason"))[:300],
            )
            rows.append(row)
            continue
        used.add(match)
        nodes = found.get("nodes") or []
        models = sorted(n for n in nodes if n.startswith("model."))
        if found["project"] != _norm(entry.get("project_path")):
            row.update(
                outcome="miss",
                reason="wrong project",
                got=found["project"],
                expected=_norm(entry.get("project_path")),
            )
        elif models != expected:
            row.update(
                outcome="miss",
                reason="wrong model set",
                missing=sorted(set(expected) - set(models)),
                extra=sorted(set(models) - set(expected)),
            )
        else:
            row["outcome"] = "correct"
        others = sorted(n for n in nodes if not n.startswith("model."))
        row["other_nodes_match"] = others == expected_others(entry)
        if not row["other_nodes_match"]:
            row["other_nodes_diff"] = {
                "missing": sorted(set(expected_others(entry)) - set(others)),
                "extra": sorted(set(others) - set(expected_others(entry))),
            }
        rows.append(row)
    out_of_scope = {o.get("file") for o in label.get("out_of_scope") or []}
    left = [r for i, r in enumerate(records) if i not in used and i not in unknown_cited]
    unit_left = [r for r in left if r.get("of") != "location"]
    return {
        "rows": rows,
        "false_positives": [
            _brief(r, out_of_scope) for r in unit_left if r["status"] == "resolved"
        ],
        "unexpected_unknown_links": [
            _brief(r, out_of_scope) for r in unit_left if r["status"] != "resolved"
        ],
        "stats": {
            "dagster_files": len(scan["files"]),
            "unknown_records": sum(r["status"] != "resolved" for r in records),
            "location_unknowns_answering_no_label": len(left) - len(unit_left),
            "problems": len(scan["problems"]),
        },
    }


def _brief(record: dict[str, Any], out_of_scope: set[str]) -> dict[str, Any]:
    found = {
        "file": record["file"],
        "line": record["line"],
        "list": record["list"],
        "api": record.get("api"),
        "name": record.get("name"),
        "location": record.get("location"),
        "projects": record["projects"],
        "status": record["status"],
        "models": sum(n.startswith("model.") for n in record.get("nodes") or []),
        "category": record.get("category"),
        "reason": record.get("reason"),
    }
    if record["file"] in out_of_scope:
        found["in_out_of_scope_file"] = True
    return found
