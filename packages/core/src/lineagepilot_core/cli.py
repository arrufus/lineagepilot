"""The headless entry point: ``lineagepilot index``, ``impact``, ``review`` and ``airflow``
(tasks 1.8, 4.5, 5.3).

Runs the same core as the language server, so CI can use it later (design section 8).
Output is deterministic, as text or ``--json``. ``review`` prints the same Markdown as the
panel's "Copy as Markdown". Exit codes: 0 on success, 1 when the index is unknown, a target
cannot be found or git cannot say what changed, 2 for usage errors.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any, TextIO

from lineagepilot_core import __version__
from lineagepilot_core.airflow.categories import explanation, label
from lineagepilot_core.airflow.dag_static import DEFAULT_DAG_FOLDERS
from lineagepilot_core.airflow.link import link_airflow, mark_not_analysed
from lineagepilot_core.airflow.workspace import default_workspace, scan_airflow, scan_workspace
from lineagepilot_core.analysis.baseline import ManifestBaseline, load_baseline_manifest
from lineagepilot_core.analysis.impact import Impact, impact
from lineagepilot_core.analysis.review import baseline_inputs, review_changes
from lineagepilot_core.gitdiff import GitError, git_changes
from lineagepilot_core.graph.ids import column_id
from lineagepilot_core.graph.model import DBT_KINDS, MetadataGraph
from lineagepilot_core.indexer import IndexResult, IndexStatus, index_project
from lineagepilot_core.project import DbtProject, read_project
from lineagepilot_core.snapshot import SnapshotCache

EXIT_OK, EXIT_UNKNOWN, EXIT_USAGE = 0, 1, 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lineagepilot",
        description="Lineage and change impact for dbt projects, from the dbt artifacts.",
    )
    parser.add_argument("--version", action="version", version=f"lineagepilot {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    def artifact_options(command: argparse.ArgumentParser) -> None:
        command.add_argument("--project-dir", type=Path, default=Path("."), help="dbt project")
        command.add_argument(
            "--manifest", type=Path, help="default: <project>/target/manifest.json"
        )
        command.add_argument(
            "--catalog", type=Path, help="default: catalog.json beside the manifest"
        )
        cache = command.add_mutually_exclusive_group()
        cache.add_argument(
            "--cache-dir", type=Path, help="snapshot cache; default: <project>/target/.lineagepilot"
        )
        cache.add_argument("--no-cache", action="store_true", help="do not read or write snapshots")
        command.add_argument(
            "--no-column-lineage", action="store_true", help="model-level lineage only"
        )
        command.add_argument("--json", action="store_true", help="machine-readable output")
        command.add_argument("-v", "--verbose", action="store_true", help="log progress to stderr")

    def airflow_options(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--workspace",
            type=Path,
            help="the folder DAG files are read from; default: the git repository, or the project",
        )
        command.add_argument(
            "--dag-folder",
            action="append",
            metavar="FOLDER",
            help=f"a folder or pattern in the workspace; default: {DEFAULT_DAG_FOLDERS[0]}",
        )

    index = commands.add_parser("index", help="index the project and report its status")
    artifact_options(index)

    impact_command = commands.add_parser(
        "impact", help="what is downstream of a changed model, column or file"
    )
    impact_command.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="a unique_id, a model name, a file path, or <model>#<column>",
    )
    impact_command.add_argument("--max-depth", type=int, help="stop after this many steps")
    artifact_options(impact_command)
    airflow_options(impact_command)
    impact_command.add_argument("--no-airflow", action="store_true", help="do not read DAG files")

    review = commands.add_parser(
        "review", help="what the changes on this branch affect, as Markdown for a pull request"
    )
    review.add_argument(
        "--base",
        metavar="REF",
        help="compare with the merge-base of HEAD and this ref; "
        "default: the first of origin/HEAD, origin/main, origin/master, main, master",
    )
    review.add_argument(
        "--baseline-manifest",
        type=Path,
        metavar="PATH",
        help="the manifest to compare columns with, typically the one for dbt's --state; "
        "default: compare with catalog.json",
    )
    artifact_options(review)
    airflow_options(review)
    review.add_argument("--no-airflow", action="store_true", help="do not read DAG files")

    airflow = commands.add_parser(
        "airflow",
        help="what the DAG files run; read as syntax only, never imported or executed",
    )
    airflow.add_argument("--project-dir", type=Path, help="default: every project in the workspace")
    airflow.add_argument("--manifest", type=Path, help="default: <project>/target/manifest.json")
    airflow.add_argument("--json", action="store_true", help="machine-readable output")
    airflow.add_argument("-v", "--verbose", action="store_true", help="log progress to stderr")
    airflow_options(airflow)
    return parser


def main(argv: list[str] | None = None, out: TextIO | None = None) -> int:
    out = out or sys.stdout
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as e:  # argparse exits 2 on usage errors, 0 for --help/--version
        return int(e.code or 0)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        stream=sys.stderr,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if args.command == "airflow":
        scan = _airflow_scan(args)
        _write(out, scan if args.json else _airflow_text(scan), args.json)
        return EXIT_OK

    project = read_project(args.project_dir) if args.command == "review" else None
    manifest = args.manifest or (project.manifest_path if project else None)
    cache = None
    if not args.no_cache:
        cache = SnapshotCache(args.cache_dir or args.project_dir / "target" / ".lineagepilot")
    result = index_project(
        args.project_dir,
        manifest=manifest,
        catalog=args.catalog,
        cache=cache,
        column_lineage=not args.no_column_lineage,
    )

    if args.command == "index":
        _write(out, _index_json(result) if args.json else _index_text(result), args.json)
        return EXIT_OK if result.status is IndexStatus.OK else EXIT_UNKNOWN

    if result.graph is None:
        _write(out, _index_json(result) if args.json else _index_text(result), args.json)
        return EXIT_UNKNOWN
    # After the index, which has cached its snapshot: a DAG scan is never part of one
    if args.no_airflow:
        mark_not_analysed(result.graph, "--no-airflow was given")
    else:
        scan = scan_airflow(
            args.project_dir,
            args.workspace or default_workspace(args.project_dir),
            args.dag_folder or DEFAULT_DAG_FOLDERS,
            manifest=result.manifest_path,
        )
        link_airflow(result.graph, scan)
    if project is not None:
        return _review(args, project, result.graph, out)
    targets, problems = resolve_targets(result.graph, args.targets, args.project_dir)
    if problems:
        _write(out, {"errors": problems} if args.json else "\n".join(problems), args.json)
        return EXIT_UNKNOWN
    found = impact(result.graph, targets, max_depth=args.max_depth)
    _write(out, _impact_json(found) if args.json else _impact_text(result.graph, found), args.json)
    return EXIT_OK


def _airflow_scan(args: argparse.Namespace) -> dict[str, Any]:
    folders = args.dag_folder or DEFAULT_DAG_FOLDERS
    if args.project_dir is not None:
        workspace = args.workspace or default_workspace(args.project_dir)
        return scan_airflow(args.project_dir, workspace, folders, args.manifest)
    return scan_workspace(args.workspace or Path("."), folders)


def _airflow_text(scan: dict[str, Any]) -> str:
    stats = scan["stats"]
    lines = [
        f"{stats['files_scanned']} DAG files in {', '.join(scan['folders']) or 'no folder found'}: "
        f"{stats['dags']} DAGs, {stats['links_resolved']} tasks linked to dbt, "
        f"{stats['links_unknown']} unknown"
    ]
    for link in scan["links"]:
        if link["status"] != "resolved":
            continue
        name = link.get("task_id") or link.get("group_id") or "(the whole DAG)"
        models = link["resolved_models"] or []
        lines.append(
            f"  {link['dag_id']}  {name}  {link['dbt_command']}  "
            f"{len(models)} models  {link['confidence']}  {link['project_path']}"
        )
    if scan["unresolved"]:
        lines.append(f"Unknowns ({len(scan['unresolved'])}):")
        for found in scan["unresolved"]:
            category = found["category"]
            lines.append(
                f"  {found['file']}:{found['line']}  {label(category)}: {found['reason']}. "
                f"{explanation(category)}"
            )
    lines += [f"Problem: {problem}" for problem in scan["problems"]]
    return "\n".join(lines)


def _review(
    args: argparse.Namespace, project: DbtProject, graph: MetadataGraph, out: TextIO
) -> int:
    try:
        changes = git_changes(project, args.base)
    except GitError as e:
        sys.stderr.write(f"lineagepilot review: {e}\n")
        return EXIT_UNKNOWN
    baseline: ManifestBaseline | None = None
    if args.baseline_manifest is not None:
        nodes, files = baseline_inputs(graph, changes.files, project)
        baseline = load_baseline_manifest(args.baseline_manifest, nodes, files)
    review = review_changes(
        graph,
        changes.files,
        project,
        baseline=baseline,
        base=changes.base,
        outside_project=changes.outside_project,
        column_stage="off" if args.no_column_lineage else "done",
    )
    _write(out, review if args.json else review["markdown"], args.json)
    return EXIT_OK


def _write(out: TextIO, content: Any, as_json: bool) -> None:
    text = json.dumps(content, indent=2, sort_keys=True) if as_json else content
    out.write(text.rstrip("\n") + "\n")


def resolve_targets(
    graph: MetadataGraph, targets: list[str], project_dir: Path
) -> tuple[list[str], list[str]]:
    """Turn user input into node or column IDs; return the IDs and any problems."""
    resolved: list[str] = []
    problems: list[str] = []
    for target in targets:
        node_part, sep, column = target.partition("#")
        nodes = _resolve_node(graph, node_part, project_dir)
        if not nodes:
            problems.append(f"{target}: no node, model or file by that name")
        elif sep:
            if len(nodes) > 1:
                problems.append(
                    f"{target}: {node_part} names {len(nodes)} nodes: {', '.join(nodes)}"
                )
                continue
            cid = column_id(nodes[0], column, graph.adapter_type)
            if cid in graph.columns:
                resolved.append(cid)
            else:
                problems.append(f"{target}: {nodes[0]} has no column {column!r}")
        else:
            resolved.extend(nodes)
    return list(dict.fromkeys(resolved)), problems


def _resolve_node(graph: MetadataGraph, name: str, project_dir: Path) -> list[str]:
    if name in graph.nodes:
        return [name]
    by_file = graph.nodes_for_file(name)
    if not by_file:
        path = Path(name)
        try:
            relative = (path if path.is_absolute() else Path.cwd() / path).resolve()
            by_file = graph.nodes_for_file(str(relative.relative_to(project_dir.resolve())))
        except (ValueError, OSError):
            by_file = []
    if by_file:
        return [n.id for n in by_file]
    return sorted(n.id for n in graph.nodes.values() if n.kind in DBT_KINDS and n.name == name)


def _index_json(result: IndexResult) -> dict[str, Any]:
    data: dict[str, Any] = {
        "status": result.status.value,
        "project": result.project_name,
        "dbt_version": result.dbt_version,
        "manifest_schema": result.schema_version,
        "from_cache": result.from_cache,
        "errors": [
            {"severity": e.severity.value, "message": e.message, "location": e.location}
            for e in result.errors
        ],
    }
    if result.graph is not None:
        stats = result.graph.stats()
        data["stats"] = stats
        data["column_lineage"] = dict(
            sorted(Counter(s.state.value for s in result.graph.column_lineage.values()).items())
        )
    return data


def _index_text(result: IndexResult) -> str:
    lines = []
    if result.status is IndexStatus.UNKNOWN:
        lines.append(
            f"Index unknown: the dbt artifacts at {result.manifest_path} cannot be trusted"
        )
    else:
        version = f"dbt {result.dbt_version}" if result.dbt_version else "dbt"
        schema = f", manifest schema v{result.schema_version}" if result.schema_version else ""
        source = " (from the snapshot cache)" if result.from_cache else ""
        lines.append(f"Indexed {result.project_name or 'project'} ({version}{schema}){source}")
        stats = result.graph.stats()
        kinds = ", ".join(f"{n} {k}" for k, n in stats["node_kinds"].items())
        edges = ", ".join(f"{n} {k}" for k, n in stats["edge_kinds"].items())
        lines.append(f"  {stats['nodes']} nodes: {kinds}")
        lines.append(f"  {stats['columns']} columns; {stats['edges']} edges: {edges}")
        lines.append(f"  {stats['unresolved']} unresolved references")
        states = Counter(s.state.value for s in result.graph.column_lineage.values())
        if states:
            lines.append(
                "  Column lineage: " + ", ".join(f"{n} {s}" for s, n in sorted(states.items()))
            )
    for error in result.errors:
        lines.append(f"  {error.severity.value}: {error.message}")
    return "\n".join(lines)


def _impact_json(found: Impact) -> dict[str, Any]:
    return {
        "targets": list(found.targets),
        "truncated": found.truncated,
        "counts": found.counts(),
        "nodes": [
            {
                "id": n.id,
                "kind": n.kind.value,
                "depth": n.depth,
                "granularity": n.granularity.value,
                "confidence": n.confidence.value,
                "columns": n.columns,
                "reason": n.reason,
            }
            for n in found.nodes.values()
        ],
        "unknowns": [
            {"subject": u.subject, "reference": u.reference, "reason": u.reason}
            for u in found.unknowns
        ],
    }


def _impact_text(graph: MetadataGraph, found: Impact) -> str:
    lines = [f"Impact of {', '.join(found.targets)}"]
    if not found.nodes:
        lines.append("  Nothing downstream.")
    else:
        counts = ", ".join(f"{n} {k}" for k, n in found.counts().items())
        lines.append(f"  {len(found.nodes)} impacted: {counts}")
        width = max(len(n.id) for n in found.nodes.values())
        for node in found.nodes.values():
            detail = f"  {node.reason}" if node.reason else ""
            columns = ""
            if node.columns:
                columns = "  [" + ", ".join(c.partition("#")[2] for c in node.columns) + "]"
            lines.append(
                f"  {node.depth:>3}  {node.kind.value:<12} {node.id:<{width}}  "
                f"{node.confidence.value:<9} {node.granularity.value}{columns}{detail}"
            )
    if found.truncated:
        lines.append("  (stopped at --max-depth; there is more downstream)")
    if found.unknowns:
        lines.append(f"Unknowns ({len(found.unknowns)}):")
        lines += [f"  {u.subject}: {u.reference}: {u.reason}" for u in found.unknowns]
    return "\n".join(lines)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
