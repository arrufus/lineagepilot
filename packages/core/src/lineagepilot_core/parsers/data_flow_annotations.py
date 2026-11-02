"""Data flow annotations for Airflow tasks.

Lets users declare what a task consumes, produces, transforms or validates, for anything
the static DAG scanner cannot see (design section 6.2), in a YAML or JSON file.

Ported from data-capsule ``backend/src/parsers/data_flow_annotations.py`` (commit
5e57fcc). Changes:

- Task identity is ``airflow:<dag_id>.<task_id>`` (design section 5.1). The server's
  ``instance`` segment is ignored, because a workspace has one Airflow.
- References are kept as written: a dbt ``unique_id`` or a relation name.
  ``validate_urns`` became ``validate_references``.
- A file whose top level is not a mapping (an empty file, say) loads as "no annotations".

Phase 5 (task 5.2) extends the format, for example with container path maps.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from lineagepilot_core.graph.ids import airflow_task_id
from lineagepilot_core.parsers import _json

logger = logging.getLogger(__name__)


@dataclass
class DataFlowAnnotation:
    """Declared data flow for one task."""

    task_id: str  # airflow:<dag_id>.<task_id>
    consumes: list[str] = field(default_factory=list)
    produces: list[str] = field(default_factory=list)
    transforms: list[str] = field(default_factory=list)
    validates: list[str] = field(default_factory=list)

    # reference -> operation type, for produces
    operations: dict[str, str] = field(default_factory=dict)
    # reference -> access pattern, for consumes
    access_patterns: dict[str, str] = field(default_factory=dict)


class DataFlowAnnotationLoader:
    """Loads data flow annotation files."""

    def __init__(self, annotation_file: str | None = None):
        self.annotation_file = annotation_file
        self._annotations: dict[str, DataFlowAnnotation] = {}

    def load_annotations(self, annotation_file: str | None = None) -> bool:
        """Load annotations from a ``.yaml``, ``.yml`` or ``.json`` file.

        Returns True if the file was read, even when it holds no annotations.
        """
        file_path = annotation_file or self.annotation_file
        if not file_path:
            logger.warning("No annotation file provided")
            return False

        path = Path(file_path)
        if not path.exists():
            logger.warning("Annotation file not found: %s", file_path)
            return False

        try:
            if path.suffix in (".yaml", ".yml"):
                with open(path, encoding="utf-8") as f:
                    data = yaml.safe_load(f)
            elif path.suffix == ".json":
                data = _json.load_file(path)
            else:
                logger.error("Unsupported annotation file format: %s", path.suffix)
                return False
        except (OSError, ValueError, yaml.YAMLError) as e:
            logger.error("Cannot read annotation file %s: %s", file_path, e)
            return False

        self._parse_annotations(data if isinstance(data, dict) else {})
        logger.info("Loaded %d task annotations from %s", len(self._annotations), file_path)
        return True

    def _parse_annotations(self, data: dict[str, Any]) -> None:
        """Parse the annotation structure.

        Expected format::

            pipelines:
              - pipeline_id: finance_gl_pipeline
                tasks:
                  - task_id: load_gl_transactions
                    consumes:
                      - model.finance.stg_chart_of_accounts
                      - urn: analytics.staging.raw_transactions
                        access_pattern: incremental
                    produces:
                      - urn: analytics.facts.gl_transactions
                        operation: insert
        """
        pipelines = data.get("pipelines")
        for pipeline in pipelines if isinstance(pipelines, list) else []:
            if not isinstance(pipeline, dict):
                continue
            dag_id = pipeline.get("pipeline_id")
            if not isinstance(dag_id, str) or not dag_id:
                logger.warning("Pipeline missing pipeline_id, skipping")
                continue

            tasks = pipeline.get("tasks")
            for task in tasks if isinstance(tasks, list) else []:
                if not isinstance(task, dict):
                    continue
                task_name = task.get("task_id")
                if not isinstance(task_name, str) or not task_name:
                    logger.warning("Task in pipeline %s missing task_id, skipping", dag_id)
                    continue

                annotation = DataFlowAnnotation(task_id=airflow_task_id(dag_id, task_name))
                for ref, item in _references(task.get("consumes")):
                    annotation.consumes.append(ref)
                    if isinstance(item.get("access_pattern"), str):
                        annotation.access_patterns[ref] = item["access_pattern"]
                for ref, item in _references(task.get("produces")):
                    annotation.produces.append(ref)
                    if isinstance(item.get("operation"), str):
                        annotation.operations[ref] = item["operation"]
                annotation.transforms.extend(ref for ref, _ in _references(task.get("transforms")))
                annotation.validates.extend(ref for ref, _ in _references(task.get("validates")))

                self._annotations[annotation.task_id] = annotation

    def get_annotation(self, task_id: str) -> DataFlowAnnotation | None:
        return self._annotations.get(task_id)

    def has_annotation(self, task_id: str) -> bool:
        return task_id in self._annotations

    def get_all_annotations(self) -> dict[str, DataFlowAnnotation]:
        return self._annotations.copy()

    def validate_references(self, known: set[str]) -> list[str]:
        """Warnings for every reference that is not in ``known``."""
        warnings = []
        for task_id, annotation in self._annotations.items():
            for ref in (
                annotation.consumes
                + annotation.produces
                + annotation.transforms
                + annotation.validates
            ):
                if ref not in known:
                    warnings.append(f"Task {task_id} references unknown node: {ref}")
        return warnings


def _references(items: Any) -> list[tuple[str, dict[str, Any]]]:
    """Each entry is a reference string, or a mapping with ``urn`` and extra fields."""
    refs: list[tuple[str, dict[str, Any]]] = []
    for item in items if isinstance(items, list) else []:
        if isinstance(item, str):
            refs.append((item, {}))
        elif isinstance(item, dict) and isinstance(item.get("urn"), str):
            refs.append((item["urn"], item))
    return refs
