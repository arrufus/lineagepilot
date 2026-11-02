"""Index a dbt project: parse, build the graph, add column lineage, cache a snapshot.

One pipeline for the headless CLI and the language server's child-process indexer
(design section 11). A cached snapshot whose key matches the artifacts is loaded
instead of parsing; anything that makes the artifacts untrustworthy gives an ``unknown``
index with the reasons, never an empty or partial graph.

Column lineage takes about ten times as long as the rest of the index, so a caller can
ask for the model-level graph first, through ``on_model_graph``, and get the full graph
when ``index_project`` returns.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from lineagepilot_core.graph.build import build_dbt_graph
from lineagepilot_core.graph.column_lineage import add_column_lineage, compiled_sql
from lineagepilot_core.graph.model import MetadataGraph
from lineagepilot_core.parsers.base import ParseError, ParseErrorSeverity
from lineagepilot_core.parsers.dbt_parser import DbtParser
from lineagepilot_core.snapshot import SnapshotCache, file_sha256, snapshot_key

# Artifacts larger than this are refused (design section 10). The synthetic 5,000-model
# manifest is about 60 MB; the cap only stops a crafted file from exhausting memory.
DEFAULT_MAX_ARTIFACT_BYTES = 1 << 30


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
    on_model_graph: Callable[[IndexResult], None] | None = None,
    max_artifact_bytes: int | None = DEFAULT_MAX_ARTIFACT_BYTES,
) -> IndexResult:
    """Index the project's dbt artifacts.

    Args:
        on_model_graph: with ``column_lineage``, called once with the model-level result
            before column lineage is computed: from the cache when a model-level snapshot
            matches, otherwise as soon as the graph is built (and cached). It is not
            called when the full graph is cached or the index is unknown. Its graph is
            the one column lineage is then added to, so the callback must not keep it.
        max_artifact_bytes: refuse a manifest or catalog larger than this
    """
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
    for path in (manifest_path, catalog_path):
        size = path.stat().st_size if path is not None else 0
        if max_artifact_bytes is not None and size > max_artifact_bytes:
            result.errors.append(
                ParseError(
                    f"{path} is {size / 2**20:.0f} MB, over the "
                    f"{max_artifact_bytes / 2**20:.0f} MB limit for dbt artifacts",
                    ParseErrorSeverity.ERROR,
                    location=str(path),
                )
            )
    if result.errors:
        return result

    staged = column_lineage and on_model_graph is not None
    model_stage_sent = False
    if cache is not None:
        hashes = (file_sha256(manifest_path), file_sha256(catalog_path) if catalog_path else None)
        key = snapshot_key(*hashes, column_lineage)
        graph = cache.get(key)
        if graph is not None:
            return _from_cache(result, cache, key, graph, started)
        if staged:
            model_key = snapshot_key(*hashes, False)
            graph = cache.get(model_key)
            if graph is not None:
                on_model_graph(_from_cache(_copy(result), cache, model_key, graph, started))
                model_stage_sent = True

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
    if staged and not model_stage_sent:
        stage = _copy(result)
        stage.status, stage.graph = IndexStatus.OK, graph
        if cache is not None and parse.manifest_sha256:
            key = snapshot_key(parse.manifest_sha256, parse.catalog_sha256, False)
            stage.snapshot = cache.put(key, graph)
        stage.seconds["total"] = time.perf_counter() - started
        on_model_graph(stage)
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


def _copy(result: IndexResult) -> IndexResult:
    return IndexResult(
        status=result.status,
        errors=list(result.errors),
        manifest_path=result.manifest_path,
        catalog_path=result.catalog_path,
        project_name=result.project_name,
        dbt_version=result.dbt_version,
        schema_version=result.schema_version,
        seconds=dict(result.seconds),
    )


def _from_cache(
    result: IndexResult, cache: SnapshotCache, key: str, graph: MetadataGraph, started: float
) -> IndexResult:
    result.status = IndexStatus.OK
    result.graph, result.from_cache, result.snapshot = graph, True, cache.path(key)
    result.project_name = graph.project_name
    result.dbt_version = graph.manifest.dbt_version if graph.manifest else None
    result.schema_version = graph.schema_version
    result.seconds["total"] = time.perf_counter() - started
    return result
