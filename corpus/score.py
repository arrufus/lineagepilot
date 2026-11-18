"""Score a reader on its held-out corpus (task 7.9; the Airflow corpus is task 5.5's).

    python corpus/score.py fetch TOOL        fetch the pinned commits, as data only
    python corpus/score.py hash TOOL         the reader's hash, to compare with a recorded run
    python corpus/score.py run TOOL NAME     scan, score, and write corpus/TOOL/results/NAME.json

    --work DIR       where the repositories are, by default the corpus's work/ (which git ignores)
    --corpus NAME    another corpus of the tool's, in corpus/TOOL/NAME/ with its own labels/,
                     work/ and results/: the Airflow reader's second is corpus/airflow/fresh/

Run with lineagepilot_core importable:

    UV_PYTHON_PREFERENCE=only-managed uv run --project packages/core python corpus/score.py run airflow first

Each tool has a folder, ``corpus/<tool>/``:

- ``repos.json``: how the repositories were chosen;
- ``labels/``: one file per repository, with its URL, the commit it is pinned to, and the links a
  careful person expects, labelled blind;
- ``results/``: each run, with the reader's hash and the commit it ran on;
- ``tool.py``: what is particular to the tool. ``HASH_FILES``, the files under
  ``lineagepilot_core`` that decide the answer, as globs; ``SCANS``, how to scan a repository,
  first the scan that measures the reader and then any others; ``GROUP``, the label field results
  are broken down by; and ``score(label, scan)``, each labelled link's outcome.

What is shared is here. A labelled link is ``correct``, a ``miss`` (with its reason),
``unknown_legit`` or ``unknown_not_legit``. A link the reader found that no label expects is a
false positive if it resolved, or an unexpected unknown if not. The miss rate is the misses over
all the labelled links, and is also given without the labels marked ``UNSURE:``.

The repositories are data. They are fetched at a pinned commit with hooks off, without submodules
or LFS, and read only as text and syntax. Nothing in them is imported, executed or installed.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
CORE = REPO / "packages" / "core" / "src" / "lineagepilot_core"
_TOOL_NAME = re.compile(r"[a-z][a-z0-9_]*")


def tool(name: str) -> ModuleType:
    """A tool's ``tool.py``, from its folder."""
    path = HERE / name / "tool.py"
    if not _TOOL_NAME.fullmatch(name) or not path.is_file():
        tools = ", ".join(sorted(p.parent.name for p in HERE.glob("*/tool.py")))
        raise SystemExit(f"no corpus for {name!r}; there is one for: {tools}")
    spec = importlib.util.spec_from_file_location(f"corpus_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def hash_files(found: ModuleType) -> list[Path]:
    """The files that decide the reader's answer: each glob of ``HASH_FILES``, in order, sorted."""
    files: list[Path] = []
    for pattern in found.HASH_FILES:
        matched = sorted(CORE.glob(pattern))
        if not matched:
            raise SystemExit(f"{pattern} matches nothing under {CORE}")
        files += [path for path in matched if path not in files]
    return files


def reader_hash(found: ModuleType) -> str:
    digest = hashlib.sha256()
    for path in hash_files(found):
        # As git stores them, so a CRLF checkout gives the same hash
        digest.update(path.relative_to(CORE).as_posix().encode())
        digest.update(path.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


def commit() -> str:
    done = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True)
    return done.stdout.strip()


def folder(name: str, corpus: str | None = None) -> Path:
    """Where a tool's corpus is: ``corpus/<tool>/``, or another of its corpora inside it."""
    if corpus is None:
        return HERE / name
    if not _TOOL_NAME.fullmatch(corpus) or not (HERE / name / corpus / "labels").is_dir():
        raise SystemExit(f"no corpus {corpus!r} in corpus/{name}/")
    return HERE / name / corpus


def labels(name: str, corpus: str | None = None) -> list[dict[str, Any]]:
    found = sorted((folder(name, corpus) / "labels").glob("*.json"))
    return [json.loads(p.read_text(encoding="utf-8")) for p in found]


def checkout(work: Path, label: dict[str, Any]) -> Path:
    return work / label["repo"].replace("/", "_")


def fetch(name: str, work: Path, corpus: str | None = None) -> int:
    for label in labels(name, corpus):
        target = checkout(work, label)
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


# Scoring, the same for every tool


def scored(found: ModuleType, label: dict[str, Any], scan: dict[str, Any]) -> dict[str, Any]:
    """One repository's scan, scored by the tool's rules, with its metrics."""
    result = found.score(label, scan)
    stats = result.pop("stats", None)
    result["metrics"] = metrics(result["rows"], result["false_positives"], result["unexpected_unknown_links"], stats)
    return result


def metrics(rows: list[dict[str, Any]], false_positives: list, extra_unknown: list, stats: dict[str, Any] | None = None) -> dict[str, Any]:
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
    if stats is not None:
        found.update(stats)
    return found


def pooled(results: list[dict[str, Any]], group: str) -> dict[str, Any]:
    rows = [r for result in results for r in result["rows"]]
    false_positives = [f for result in results for f in result["false_positives"]]
    extra = [u for result in results for u in result["unexpected_unknown_links"]]
    summary = metrics(rows, false_positives, extra)
    certain = [r for r in rows if not r["unsure"]]
    summary["without_unsure_labels"] = metrics(certain, false_positives, extra)
    by_group: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_group.setdefault(row[group] or "?", []).append(row)
    summary[f"by_{group}"] = {
        value: {k: v for k, v in metrics(found, [], []).items() if k in ("expected_links", "correct", "misses", "miss_rate")}
        for value, found in sorted(by_group.items())
    }
    reasons: dict[str, int] = {}
    for row in rows:
        if row["outcome"] == "miss":
            reasons[row["reason"]] = reasons.get(row["reason"], 0) + 1
    summary["miss_reasons"] = dict(sorted(reasons.items(), key=lambda kv: -kv[1]))
    return summary


def evaluate(found: ModuleType, name: str, labelled: list[dict[str, Any]], work: Path) -> dict[str, Any]:
    """Every scan of every labelled repository, scored and pooled."""
    sets: dict[str, list[dict[str, Any]]] = {scan: [] for scan in found.SCANS}
    first = next(iter(found.SCANS))
    repos = []
    for label in labelled:
        root = checkout(work, label)
        if not root.is_dir():
            raise SystemExit(f"{label['repo']} is not in {work}: run fetch first")
        entry: dict[str, Any] = {"repo": label["repo"], "sha": label["sha"]}
        for scan, run_scan in found.SCANS.items():
            result = scored(found, label, run_scan(root, label))
            sets[scan].append(result)
            entry[scan] = result if scan == first else {"metrics": result["metrics"]}
        repos.append(entry)
    return {
        "tool": name,
        "reader_sha256": reader_hash(found),
        "commit": commit(),
        "summary": {scan: pooled(results, found.GROUP) for scan, results in sets.items()},
        "repos": repos,
    }


def run(name: str, run_name: str, work: Path, corpus: str | None = None) -> int:
    output = evaluate(tool(name), name, labels(name, corpus), work)
    if corpus is not None:
        output["corpus"] = corpus
    results = folder(name, corpus) / "results"
    results.mkdir(exist_ok=True)
    (results / f"{run_name}.json").write_text(json.dumps(output, indent=1) + "\n", encoding="utf-8", newline="\n")
    sys.stdout.write(json.dumps({"reader_sha256": output["reader_sha256"], **output["summary"]}, indent=1) + "\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--work", type=Path, help="where the repositories are (default: the corpus's work/)")
    parser.add_argument("--corpus", help="another of the tool's corpora, in corpus/TOOL/NAME/")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("fetch", "hash"):
        commands.add_parser(command).add_argument("tool")
    run_parser = commands.add_parser("run")
    run_parser.add_argument("tool")
    run_parser.add_argument("name", help="the run's name: results/NAME.json")
    args = parser.parse_args(argv)
    found = tool(args.tool)
    work = args.work or folder(args.tool, args.corpus) / "work"
    if args.command == "fetch":
        return fetch(args.tool, work, args.corpus)
    if args.command == "hash":
        sys.stdout.write(reader_hash(found) + "\n")
        return 0
    if not re.fullmatch(r"[\w.-]+", args.name):
        parser.error("a run's name is letters, digits, '.', '-' and '_'")
    return run(args.tool, args.name, work, args.corpus)


if __name__ == "__main__":
    raise SystemExit(main())
