"""Dagster, the second reader (Phase 8), behind the reader contract (``readers.contract``)."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from lineagepilot_core.graph.model import DAGSTER, PIPELINE_KINDS, MetadataGraph, ReaderSummary
from lineagepilot_core.readers.common.folders import WORKSPACE, in_folders
from lineagepilot_core.readers.dagster.discovery import (
    COMPONENT_FILE,
    DEFAULT_PATHS,
    PROJECT_FILE,
    WORKSPACE_FILES,
    clean_paths,
    is_dagster_file,
)
from lineagepilot_core.readers.dagster.link import (
    DAGSTER_ORIGINS,
    link_dagster,
    mark_not_analysed,
    reader_summary_json,
    unlink_dagster,
)
from lineagepilot_core.readers.dagster.scan import scan_dagster

# The files the editor watches in each folder of the setting
_WATCHED = ("*.py", COMPONENT_FILE, PROJECT_FILE, *WORKSPACE_FILES)


class DagsterReader:
    tool = DAGSTER
    option = "dagsterPaths"
    default_paths: tuple[str, ...] = DEFAULT_PATHS
    files = "the Dagster definitions"
    kinds = PIPELINE_KINDS
    origins = DAGSTER_ORIGINS

    def clean_paths(self, paths: Iterable[Any]) -> list[str]:
        return clean_paths(paths)

    def watch_globs(self, paths: Sequence[str]) -> list[str]:
        globs: list[str] = []
        for folder in self.clean_paths(paths):
            base = "**" if folder == WORKSPACE else f"{folder}/**"
            globs.extend(f"{base}/{name}" for name in _WATCHED)
        return globs

    def owns(self, relative: str, paths: Sequence[str]) -> bool:
        return is_dagster_file(relative) and in_folders(relative, self.clean_paths(paths))

    def read(
        self,
        project_root: str | Path,
        workspace: str | Path,
        paths: Sequence[str],
        manifest: str | Path | None,
    ) -> dict[str, Any]:
        return scan_dagster(project_root, workspace, paths, manifest)

    def link(self, graph: MetadataGraph, scan: dict[str, Any]) -> ReaderSummary:
        return link_dagster(graph, scan)

    def unlink(self, graph: MetadataGraph) -> None:
        unlink_dagster(graph)

    def mark_not_analysed(self, graph: MetadataGraph, reason: str) -> None:
        mark_not_analysed(graph, reason)

    def summary_json(self, graph: MetadataGraph | None) -> dict[str, Any]:
        return reader_summary_json(graph.readers.get(DAGSTER) if graph is not None else None)
