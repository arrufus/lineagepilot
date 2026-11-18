"""Airflow, the first reader (task 7.3): the DAG scanner behind the reader contract."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from lineagepilot_core.graph.model import (
    AIRFLOW,
    PIPELINE_KINDS,
    AirflowSummary,
    MetadataGraph,
)
from lineagepilot_core.readers.airflow.dag_static import (
    DEFAULT_DAG_FOLDERS,
    clean_dag_folder,
    in_dag_folder,
)
from lineagepilot_core.readers.airflow.link import (
    AIRFLOW_ORIGINS,
    link_airflow,
    mark_not_analysed,
    unlink_airflow,
)
from lineagepilot_core.readers.airflow.scan import scan_airflow
from lineagepilot_core.readers.airflow.view import reader_summary_json


class AirflowReader:
    tool = AIRFLOW
    option = "dagFolders"
    default_paths: tuple[str, ...] = tuple(DEFAULT_DAG_FOLDERS)
    files = "the DAG files"
    kinds = PIPELINE_KINDS
    origins = AIRFLOW_ORIGINS

    def clean_paths(self, paths: Iterable[Any]) -> list[str]:
        return [f for f in (clean_dag_folder(folder) for folder in paths) if f]

    def watch_globs(self, paths: Sequence[str]) -> list[str]:
        return [f"{folder}/**/*.py" for folder in self.clean_paths(paths)]

    def owns(self, relative: str, paths: Sequence[str]) -> bool:
        return relative.endswith(".py") and in_dag_folder(relative, self.clean_paths(paths))

    def read(
        self,
        project_root: str | Path,
        workspace: str | Path,
        paths: Sequence[str],
        manifest: str | Path | None,
    ) -> dict[str, Any]:
        return scan_airflow(project_root, workspace, paths, manifest)

    def link(self, graph: MetadataGraph, scan: dict[str, Any]) -> AirflowSummary:
        return link_airflow(graph, scan)

    def unlink(self, graph: MetadataGraph) -> None:
        unlink_airflow(graph)

    def mark_not_analysed(self, graph: MetadataGraph, reason: str) -> None:
        mark_not_analysed(graph, reason)

    def summary_json(self, graph: MetadataGraph | None) -> dict[str, Any]:
        return reader_summary_json(graph.airflow if graph is not None else None)
