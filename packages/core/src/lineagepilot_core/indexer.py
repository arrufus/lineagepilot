"""Index a dbt project: parse, build the graph, add column lineage, cache a snapshot.

One pipeline for the headless CLI now and the language server's child-process indexer in
Phase 2 (design section 11). A cached snapshot whose key matches the artifacts is loaded
instead of parsing; anything that makes the artifacts untrustworthy gives an ``unknown``
index with the reasons, never an empty or partial graph.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from lineagepilot_core.graph.build import build_dbt_graph
from lineagepilot_core.graph.column_lineage import add_column_lineage, compiled_sql
from lineagepilot_core.graph.model import MetadataGraph
from lineagepilot_core.parsers.base import ParseError, ParseErrorSeverity
from lineagepilot_core.parsers.dbt_parser import DbtParser
from lineagepilot_core.snapshot import SnapshotCache, file_sha256, snapshot_key


class IndexStatus(str, Enum):
    OK = "ok"
    UNKNOWN = "unknown"  # the artifacts are missing or cannot be trusted


@dataclass
class IndexResult:
    status: IndexStatus
    graph: MetadataGraph | None = None
    errors: list[ParseError] = field(default_factory=list)  # warnings too
    from_cache: bool = False
    snapshot: Path | None = None
    manifest_path: Path | None = None
    catalog_path: Path | None = None
    project_name: str | None = None
    dbt_version: str | None = None
    schema_version: int | None = None
    seconds: dict[str, float] = field(default_factory=dict)


def locate_artifacts(
    project_dir: Path, manifest: Path | None = None, catalog: Path | None = None
) -> tuple[Path, Path | None]:
    """The manifest and, if present, the catalog: given paths, or ``<project>/target/``."""
    manifest = manifest or project_dir / "target" / "manifest.json"
    if catalog is None:
        default = manifest.parent / "catalog.json"
        catalog = default if default.is_file() else None
    return manifest, catalog


def index_project(
    project_dir: str | Path = ".",
    manifest: str | Path | None = None,
    catalog: str | Path | None = None,
    cache: SnapshotCache | None = None,
    column_lineage: bool = True,
) -> IndexResult:
    started = time.perf_counter()
    project_dir = Path(project_dir)
    manifest_path, catalog_path = locate_artifacts(
        project_dir,
        Path(manifest) if manifest else None,
        Path(catalog) if catalog else None,
    )
    result = IndexResult(
        status=IndexStatus.UNKNOWN, manifest_path=manifest_path, catalog_path=catalog_path
    )
    if not manifest_path.is_file():
        result.errors.append(
            ParseError(
                f"No manifest at {manifest_path}; run dbt parse or dbt compile",
                ParseErrorSeverity.ERROR,
            )
        )
        return result

    key = None
    if cache is not None:
        key = snapshot_key(
            file_sha256(manifest_path),
            file_sha256(catalog_path) if catalog_path else None,
            column_lineage,
        )
        graph = cache.get(key)
        if graph is not None:
            result.status = IndexStatus.OK
            result.graph, result.from_cache, result.snapshot = graph, True, cache.path(key)
            result.project_name = graph.project_name
            result.dbt_version = graph.manifest.dbt_version if graph.manifest else None
            result.schema_version = graph.schema_version
            result.seconds["total"] = time.perf_counter() - started
            return result

    parsed_at = time.perf_counter()
    config = {"manifest_path": str(manifest_path)}
    if catalog_path is not None:
        config["catalog_path"] = str(catalog_path)
    parse = DbtParser().parse(config)
    result.errors = list(parse.errors)
    result.project_name = parse.project_name or None
    result.dbt_version = parse.dbt_version
    result.schema_version = parse.schema_version
    result.seconds["parse"] = time.perf_counter() - parsed_at
    if parse.has_errors:
        result.seconds["total"] = time.perf_counter() - started
        return result

    built_at = time.perf_counter()
    graph = build_dbt_graph(parse)
    result.seconds["graph"] = time.perf_counter() - built_at
    if column_lineage:
        lineage_at = time.perf_counter()
        add_column_lineage(graph, compiled_sql(parse, project_root=project_dir))
        result.seconds["column_lineage"] = time.perf_counter() - lineage_at

    result.status, result.graph = IndexStatus.OK, graph
    if cache is not None and parse.manifest_sha256:
        # Key by what was actually parsed, in case the files changed since they were hashed
        key = snapshot_key(parse.manifest_sha256, parse.catalog_sha256, column_lineage)
        saved_at = time.perf_counter()
        result.snapshot = cache.put(key, graph)
        result.seconds["snapshot"] = time.perf_counter() - saved_at
    result.seconds["total"] = time.perf_counter() - started
    return result
