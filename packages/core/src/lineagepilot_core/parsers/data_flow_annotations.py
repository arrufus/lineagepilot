"""The annotation file: declared data flow, for anything a reader cannot see.

Lets users declare what a task consumes, produces, transforms or validates, for anything
the static DAG scanner cannot see (design section 6.2), in a YAML or JSON file.

Ported from data-capsule ``backend/src/parsers/data_flow_annotations.py`` (commit
5e57fcc). Changes:

- Task identity is ``airflow:<dag_id>.<task_id>`` (design section 5.1). The server's
  ``instance`` segment is ignored, because a workspace has one Airflow.
- References are kept as written: a dbt ``unique_id`` or a relation name.
  ``validate_urns`` became ``validate_references``.
- A file whose top level is not a mapping (an empty file, say) loads as "no annotations".

Phase 5 (task 5.2) extends the format:

- ``path_map``: where a path in a DAG file really is, for a project that a DAG knows by its path in
  a container. Targets are relative to the project that owns the annotation file.
- ``owner`` on a pipeline or a task, for the review.
- ``acknowledged``: unknowns from the scanner that a person has answered, by ``file``, ``line``,
  ``dag_id`` or ``task_id``. An annotated task's own unknowns are answered without this.

The file comes from the workspace, so it is size-capped, parsed with ``safe_yaml.load`` (no alias
bombs, no deep nesting), and what cannot be used is reported, cut short, rather than dropped.

Phase 7 (task 7.4, decision D-14) names the file for every tool:

- ``.lineagepilot/lineage.yml`` is the file, and ``.lineagepilot/airflow.yml`` is still read. When
  both exist, ``lineage.yml`` is read and the other is reported as ignored (``choose_file``).
- ``tool`` on a pipeline says which tool runs it, and defaults to ``airflow``. A task's ID is
  ``<tool>:<pipeline>.<task>``, which for Airflow is the ID it always had.
- ``consumers`` declares what reads a model when no file says so, such as a Tableau workbook or a
  notebook: like a dbt exposure, for any tool, and down to columns (``model.p.orders#amount``).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from lineagepilot_core.graph.ids import pipeline_task_id
from lineagepilot_core.parsers import _json, safe_yaml
from lineagepilot_core.parsers.safe_yaml import shown

logger = logging.getLogger(__name__)

MAX_ANNOTATION_BYTES = 2_000_000

LINEAGE_FILE = ".lineagepilot/lineage.yml"
AIRFLOW_FILE = ".lineagepilot/airflow.yml"
# In the order they are looked for: the first that exists is read (D-14)
ANNOTATION_FILES = (LINEAGE_FILE, AIRFLOW_FILE)

DEFAULT_TOOL = "airflow"
# The tools that run pipelines (design_next_phase section 7.1). A consumer cannot be declared for
# one: its ID would share their namespace, such as ``airflow:<dag>``.
PIPELINE_TOOLS = frozenset(
    {"airflow", "dagster", "databricks", "github_actions", "dlt", "meltano", "openlineage"}
)
# A longer name would not fit the protocol (as in ``airflow/link.py``)
MAX_NAME = 1000
_TOOL = re.compile(r"[a-z][a-z0-9_]*\Z")


def choose_file(present: Iterable[str]) -> tuple[str | None, list[str]]:
    """Which annotation file to read, of those that exist, and a problem for each one ignored."""
    exists = set(present)
    found = [name for name in ANNOTATION_FILES if name in exists]
    if not found:
        return None, []
    return found[0], [f"{name} was ignored: {found[0]} is read instead" for name in found[1:]]


@dataclass
class DataFlowAnnotation:
    """Declared data flow for one task."""

    task_id: str  # <tool>:<pipeline>.<task>, which for Airflow is airflow:<dag_id>.<task_id>
    consumes: list[str] = field(default_factory=list)
    produces: list[str] = field(default_factory=list)
    transforms: list[str] = field(default_factory=list)
    validates: list[str] = field(default_factory=list)

    # reference -> operation type, for produces
    operations: dict[str, str] = field(default_factory=dict)
    # reference -> access pattern, for consumes
    access_patterns: dict[str, str] = field(default_factory=dict)

    dag_id: str = ""  # the pipeline, whatever the tool
    task: str = ""
    owner: str | None = None
    tool: str = DEFAULT_TOOL

    def to_json(self) -> dict[str, Any]:
        return {
            "tool": self.tool,
            "dag_id": self.dag_id,
            "task": self.task,
            "owner": self.owner,
            "consumes": self.consumes,
            "produces": self.produces,
            "transforms": self.transforms,
            "validates": self.validates,
        }


@dataclass(frozen=True)
class ConsumedReference:
    """What a consumer reads: a node as the annotation names it, and optionally one column."""

    node: str  # a dbt ID, a relation name, or a model's name, resolved when linked
    column: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {"node": self.node, "column": self.column}


@dataclass
class ConsumerAnnotation:
    """A declared consumer: something outside dbt that reads its models."""

    id: str  # <tool>:<name>
    tool: str
    name: str
    owner: str | None = None
    consumes: list[ConsumedReference] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tool": self.tool,
            "name": self.name,
            "owner": self.owner,
            "consumes": [c.to_json() for c in self.consumes],
        }


class DataFlowAnnotationLoader:
    """Loads data flow annotation files."""

    def __init__(self, annotation_file: str | None = None):
        self.annotation_file = annotation_file
        self._annotations: dict[str, DataFlowAnnotation] = {}
        self._consumers: dict[str, ConsumerAnnotation] = {}
        self.path_map: dict[str, str] = {}
        self.acknowledged: list[dict[str, Any]] = []
        self.problems: list[str] = []

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
            if not path.is_file() or path.stat().st_size > MAX_ANNOTATION_BYTES:
                self.problems.append(
                    f"{path.name} is not a regular file under {MAX_ANNOTATION_BYTES} bytes"
                )
                return False
            if path.suffix in (".yaml", ".yml"):
                data = safe_yaml.load(path.read_text(encoding="utf-8"))
            elif path.suffix == ".json":
                data = _json.load_file(path)
            else:
                logger.error("Unsupported annotation file format: %s", path.suffix)
                return False
        except (OSError, ValueError, yaml.YAMLError, RecursionError) as e:
            logger.error("Cannot read annotation file %s: %s", file_path, e)
            self.problems.append(f"{path.name} cannot be read: {type(e).__name__}")
            return False

        self._parse_annotations(data if isinstance(data, dict) else {})
        logger.info(
            "Loaded %d task annotations and %d consumers from %s",
            len(self._annotations),
            len(self._consumers),
            file_path,
        )
        return True

    def _parse_annotations(self, data: dict[str, Any]) -> None:
        """Parse the annotation structure.

        Expected format::

            pipelines:
              - pipeline_id: finance_gl_pipeline
                tool: airflow                 # the default
                tasks:
                  - task_id: load_gl_transactions
                    owner: finance-eng
                    consumes:
                      - model.finance.stg_chart_of_accounts
                      - urn: analytics.staging.raw_transactions
                        access_pattern: incremental
                    produces:
                      - urn: analytics.facts.gl_transactions
                        operation: insert
            consumers:
              - id: tableau:finance/gl_overview
                tool: tableau                 # or taken from the ID's prefix
                owner: finance-analytics
                consumes:
                  - model.finance.fct_gl
                  - model.finance.fct_gl#amount
            path_map:
              /usr/local/airflow/dbt: .
            acknowledged:
              - file: dags/factory.py
        """
        self._parse_path_map(data.get("path_map"))
        self._parse_acknowledged(data.get("acknowledged"))
        self._parse_consumers(data.get("consumers"))
        pipelines = data.get("pipelines")
        for pipeline in pipelines if isinstance(pipelines, list) else []:
            if not isinstance(pipeline, dict):
                continue
            dag_id = pipeline.get("pipeline_id")
            if not isinstance(dag_id, str) or not dag_id:
                logger.warning("Pipeline missing pipeline_id, skipping")
                continue
            tool = self._pipeline_tool(dag_id, pipeline.get("tool", DEFAULT_TOOL))
            if tool is None:
                continue

            tasks = pipeline.get("tasks")
            for task in tasks if isinstance(tasks, list) else []:
                if not isinstance(task, dict):
                    continue
                task_name = task.get("task_id")
                if not isinstance(task_name, str) or not task_name:
                    logger.warning("Task in pipeline %s missing task_id, skipping", dag_id)
                    continue

                annotation = DataFlowAnnotation(
                    task_id=pipeline_task_id(tool, dag_id, task_name),
                    dag_id=dag_id,
                    task=task_name,
                    owner=_text(task.get("owner")) or _text(pipeline.get("owner")),
                    tool=tool,
                )
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

    def _pipeline_tool(self, pipeline_id: str, value: Any) -> str | None:
        tool = _tool(value)
        if tool is None:
            self.problems.append(
                f"pipeline {shown(pipeline_id)}: tool {shown(value)} is not a tool's name; ignored"
            )
        return tool

    def _parse_consumers(self, entries: Any) -> None:
        if entries is None:
            return
        if not isinstance(entries, list):
            self.problems.append("consumers is not a list; ignored")
            return
        for entry in entries:
            consumer = self._consumer(entry)
            if consumer is None:
                continue
            if consumer.id in self._consumers:
                self.problems.append(
                    f"consumer {shown(consumer.id)} is declared twice; the first is kept"
                )
                continue
            self._consumers[consumer.id] = consumer

    def _consumer(self, entry: Any) -> ConsumerAnnotation | None:
        """One entry of ``consumers``, or None with the reason in ``problems``."""
        if not isinstance(entry, dict):
            self.problems.append(f"consumers: {shown(entry)} is not a mapping; ignored")
            return None
        raw_id = entry.get("id")
        if not isinstance(raw_id, str) or not raw_id.strip():
            self.problems.append(f"consumers: {shown(entry)} has no id; ignored")
            return None
        identity = _consumer_identity(raw_id.strip(), entry.get("tool"))
        if isinstance(identity, str):
            self.problems.append(f"consumer {shown(raw_id)}: {identity}; ignored")
            return None
        tool, name = identity
        consumer = ConsumerAnnotation(
            id=f"{tool}:{name}", tool=tool, name=name, owner=_text(entry.get("owner"))
        )
        items = entry.get("consumes")
        # A set, not a search of the list: the file can hold tens of thousands of references
        seen: set[ConsumedReference] = set()
        for item in items if isinstance(items, list) else []:
            reference = item.get("urn") if isinstance(item, dict) else item
            consumed = _consumed(reference) if isinstance(reference, str) else None
            if consumed is None:
                self.problems.append(
                    f"consumer {shown(consumer.id)}: {shown(item)} names no node; ignored"
                )
            elif consumed not in seen:
                seen.add(consumed)
                consumer.consumes.append(consumed)
        if not consumer.consumes:
            self.problems.append(f"consumer {shown(consumer.id)} consumes nothing; ignored")
            return None
        return consumer

    def _parse_path_map(self, entries: Any) -> None:
        if entries is None:
            return
        if not isinstance(entries, dict):
            self.problems.append("path_map is not a mapping; ignored")
            return
        for prefix, target in entries.items():
            if isinstance(prefix, str) and isinstance(target, str) and prefix and target:
                self.path_map[prefix.rstrip("/") or "/"] = target
            else:
                self.problems.append(f"path_map: {shown(prefix)} is not a path to a path; ignored")

    def _parse_acknowledged(self, entries: Any) -> None:
        if entries is None:
            return
        if not isinstance(entries, list):
            self.problems.append("acknowledged is not a list; ignored")
            return
        for entry in entries:
            keys = {k: v for k, v in entry.items() if k in _ACKNOWLEDGE} if _is_map(entry) else {}
            if keys:
                self.acknowledged.append(keys)
            else:
                self.problems.append(
                    f"acknowledged: {shown(entry)} names no file, DAG or task; ignored"
                )

    def get_annotation(self, task_id: str) -> DataFlowAnnotation | None:
        return self._annotations.get(task_id)

    def has_annotation(self, task_id: str) -> bool:
        return task_id in self._annotations

    def get_all_annotations(self) -> dict[str, DataFlowAnnotation]:
        return self._annotations.copy()

    def for_tool(self, tool: str) -> list[DataFlowAnnotation]:
        """The tasks declared for one tool, in the order the file gives them."""
        return [a for a in self._annotations.values() if a.tool == tool]

    def get_consumers(self) -> dict[str, ConsumerAnnotation]:
        return self._consumers.copy()

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


_ACKNOWLEDGE = ("file", "line", "dag_id", "task_id")


def _is_map(value: Any) -> bool:
    return isinstance(value, dict)


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _tool(value: Any) -> str | None:
    """A tool's name, in lower case, such as ``airflow`` or ``github_actions``."""
    if not isinstance(value, str):
        return None
    tool = value.strip().lower()
    return tool if _TOOL.match(tool) and len(tool) <= 64 else None


def _consumer_identity(raw_id: str, tool_value: Any) -> tuple[str, str] | str:
    """``(tool, name)`` from an ID that may carry the tool as a prefix, and an optional ``tool``;
    or why they cannot be used."""
    prefix, colon, rest = raw_id.partition(":")
    from_id = _tool(prefix) if colon else None
    tool = _tool(tool_value) if tool_value is not None else from_id
    if tool_value is not None and tool is None:
        return f"tool {shown(tool_value)} is not a tool's name"
    if tool is None:
        return "names no tool: give it a tool, or start its id with one, as in tableau:<name>"
    name = rest if from_id == tool else raw_id
    if from_id is not None and from_id != tool:
        return f"its id starts with {from_id}: but its tool is {tool}"
    if tool in PIPELINE_TOOLS:
        return f"{tool} runs pipelines: declare it under pipelines"
    name = name.strip()
    if not name:
        return "its id has no name after the tool"
    if len(name) > MAX_NAME:
        return f"its id is longer than {MAX_NAME} characters"
    return tool, name


def _consumed(reference: str) -> ConsumedReference | None:
    """``model.p.orders`` or ``model.p.orders#amount``, as a column ID is written."""
    if len(reference) > MAX_NAME:
        return None
    node, hash_, column = reference.partition("#")
    node, column = node.strip(), column.strip()
    if not node or (hash_ and not column):
        return None
    return ConsumedReference(node, column or None)


def _references(items: Any) -> list[tuple[str, dict[str, Any]]]:
    """Each entry is a reference string, or a mapping with ``urn`` and extra fields."""
    refs: list[tuple[str, dict[str, Any]]] = []
    for item in items if isinstance(items, list) else []:
        if isinstance(item, str):
            refs.append((item, {}))
        elif isinstance(item, dict) and isinstance(item.get("urn"), str):
            refs.append((item["urn"], item))
    return refs
