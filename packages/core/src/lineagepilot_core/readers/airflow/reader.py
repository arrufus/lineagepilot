"""Airflow, the first reader of the contract (``readers.contract``, task 7.3).

It wraps what Phase 5 built, unchanged: ``workspace.scan_airflow`` reads, ``link`` links. Its
setting stays ``lineagePilot.airflow.dagFolders``, sent to the server as ``dagFolders``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lineagepilot_core.graph.model import MetadataGraph
from lineagepilot_core.readers.airflow.dag_static import (
    DEFAULT_DAG_FOLDERS,
    clean_dag_folder,
    in_dag_folder,
)
from lineagepilot_core.readers.airflow.link import (
    AIRFLOW,
    link_airflow,
    mark_not_analysed,
    unlink_airflow,
)
from lineagepilot_core.readers.airflow.workspace import ANNOTATION_FILE, scan_airflow


class AirflowReader:
    tool = AIRFLOW
    files = "the DAG files"
    option = "dagFolders"
    default_paths = DEFAULT_DAG_FOLDERS
    annotation_files = (ANNOTATION_FILE,)

    def clean_paths(self, paths: Iterable[str]) -> list[str]:
        return [f for f in (clean_dag_folder(p) for p in paths) if f]

    def watch_globs(self, paths: list[str]) -> list[str]:
        # A module the DAG files import from elsewhere is not watched: Reindex picks that up
        return [f"{folder}/**/*.py" for folder in paths]

    def watches(self, relative: str, paths: list[str]) -> bool:
        return relative.endswith(".py") and in_dag_folder(relative, paths)

    def read(
        self, project_dir: str, workspace: str, paths: list[str], manifest: str | None
    ) -> dict[str, Any]:
        return scan_airflow(project_dir, workspace, paths, manifest)

    def link(self, graph: MetadataGraph, found: dict[str, Any]) -> Any:
        return link_airflow(graph, found)

    def not_analysed(self, graph: MetadataGraph, reason: str) -> None:
        mark_not_analysed(graph, reason)

    def unlink(self, graph: MetadataGraph) -> None:
        unlink_airflow(graph)


READER = AirflowReader()
