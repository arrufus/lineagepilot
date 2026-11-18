"""What each tool is called where people read it: CodeLens, hovers and the review (tasks 7.6, 7.7).

Plain names, never logos (design_next_phase section 7.4). A tool missing here is shown by its own
name. The extension keeps the same table in ``extension/src/tools.ts``; each phase that adds a
reader adds its tool to both.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from lineagepilot_core.parsers.dbt_semantic import SEMANTIC_KEYS, SEMANTIC_TOOL

TOOL_LABELS = {
    "airflow": "Airflow",
    "dagster": "Dagster",
    "databricks": "Databricks",
    "github_actions": "GitHub Actions",
    "dlt": "dlt",
    "meltano": "Meltano",
    "openlineage": "OpenLineage",
    "dbt_semantic": "dbt semantic layer",
    "looker": "Looker",
    "powerbi": "Power BI",
    "lightdash": "Lightdash",
    "tableau": "Tableau",
}

# What a tool calls its pipelines, where that is not "pipeline": (one, many)
PIPELINE_WORDS = {"airflow": ("DAG", "DAGs")}


def tool_label(tool: str | None) -> str:
    """A tool's name as people write it; ``other`` for a node with no tool."""
    if tool is None:
        return "other"
    return TOOL_LABELS.get(tool, tool)


def pipeline_word(tool: str | None, count: int) -> str:
    """What ``tool`` calls ``count`` pipelines: ``DAG`` or ``DAGs`` for Airflow."""
    one, many = PIPELINE_WORDS.get(tool or "", ("pipeline", "pipelines"))
    return one if count == 1 else many


# What is downstream, in the order it is counted: dbt's own nodes, then what runs them, then what
# reads them (design_next_phase section 7.5). (kind, one, many). The semantic layer is dbt's own,
# so it is counted as dbt's nodes are, by what each is (``count_key``).
KIND_WORDS = (
    ("model", "model", "models"),
    ("snapshot", "snapshot", "snapshots"),
    ("seed", "seed", "seeds"),
    ("source", "source", "sources"),
    ("exposure", "exposure", "exposures"),
    ("semantic_model", "semantic model", "semantic models"),
    ("metric", "metric", "metrics"),
    ("saved_query", "saved query", "saved queries"),
    ("task", "task", "tasks"),
    ("pipeline", "pipeline", "pipelines"),
    ("consumer", "consumer", "consumers"),
)

_SEMANTIC_SUBKINDS = frozenset(SEMANTIC_KEYS)

# The tool a pipeline's, task's or consumer's ID starts with: ``airflow:daily.marts``
_TOOL_ID = re.compile(r"^([a-z][a-z0-9_]*):")


def tool_of_id(node_id: str) -> str | None:
    """The tool an ID starts with: ``airflow`` for ``airflow:daily.marts``; None for a dbt ID."""
    found = _TOOL_ID.match(node_id)
    return found.group(1) if found else None


def count_key(kind: str, tool: str | None, subkind: str | None) -> tuple[str, str | None]:
    """What a node is counted as: its kind and tool, or for the semantic layer what it is, with
    no tool (``4 metrics``, not ``4 dbt semantic layer consumers``)."""
    if tool == SEMANTIC_TOOL and subkind in _SEMANTIC_SUBKINDS:
        return subkind, None
    return kind, tool


def counted(counts: Mapping[tuple[str, str | None], int]) -> list[str]:
    """Counts by kind and tool, in words, in ``KIND_WORDS`` order: ``14 models``; one tool named
    in front, ``3 Airflow tasks``; several counted after, ``3 tasks (Airflow 2, Dagster 1)``."""
    parts = []
    for kind, one, many in KIND_WORDS:
        tools = {tool: n for (k, tool), n in counts.items() if k == kind and n}
        if tools:
            parts.append(_counted(kind, one, many, tools))
    return parts


def _counted(kind: str, one: str, many: str, tools: dict[str | None, int]) -> str:
    total = sum(tools.values())
    if kind == "pipeline":
        only = next(iter(tools)) if len(tools) == 1 else None
        one, many = pipeline_word(only, 1), pipeline_word(only, 2)
    word = one if total == 1 else many
    if list(tools) == [None]:
        return f"{total} {word}"
    if len(tools) == 1:
        return f"{total} {tool_label(next(iter(tools)))} {word}"
    # Most first, then by name whatever its case, as the extension sorts them
    by_tool = sorted(tools.items(), key=lambda item: (-item[1], tool_label(item[0]).casefold()))
    return f"{total} {word} (" + ", ".join(f"{tool_label(t)} {n}" for t, n in by_tool) + ")"
