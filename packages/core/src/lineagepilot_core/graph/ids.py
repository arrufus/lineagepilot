"""Canonical IDs (design section 5.1).

| Thing            | ID                                                    |
|------------------|-------------------------------------------------------|
| dbt node         | its ``unique_id``, for example ``model.shop.orders``  |
| Physical table   | ``relation:<database>.<schema>.<identifier>``         |
| Column           | ``<node id>#<column name>``                           |
| Airflow DAG/task | ``airflow:<dag_id>`` / ``airflow:<dag_id>.<task_id>`` |

Physical names and column names are normalised to the adapter's case rules, so the same
table or column reconciles across the manifest, the catalog, compiled SQL and DAG files
(review finding F09). Snowflake folds unquoted identifiers to upper case and keeps quoted
ones as written. Every other adapter is treated as case-insensitive and folds to lower
case: Postgres, Redshift and DuckDB fold unquoted names to lower case, and BigQuery,
Databricks and Spark compare them case-insensitively. On Postgres this merges two quoted
names that differ only by case, which dbt projects almost never have.

The ``relation:`` prefix keeps physical IDs apart from dbt IDs: a database called
``model`` would otherwise make the two indistinguishable.
"""

from __future__ import annotations

import re

RELATION_PREFIX = "relation:"
AIRFLOW_PREFIX = "airflow:"

# One part of a dotted relation name: "double quoted" (with "" as an escaped quote),
# `backticked`, [bracketed] or bare.
_PART = re.compile(r'"((?:[^"]|"")*)"|`([^`]*)`|\[([^\]]*)\]|([^."`\[\]]+)')


def normalise_identifier(name: str, adapter_type: str | None, quoted: bool = False) -> str:
    """Fold an identifier to the adapter's case rules."""
    if (adapter_type or "").lower() == "snowflake":
        return name if quoted else name.upper()
    return name.lower()


def split_relation_name(relation_name: str) -> list[tuple[str, bool]] | None:
    """Split ``"db"."schema"."table"`` into ``[(part, quoted), ...]``.

    Handles double quotes, backticks (including BigQuery's ```project.dataset.table```
    form) and brackets. Returns None for anything that is not a dotted name.
    """
    text = relation_name.strip()
    # BigQuery may quote the whole path in one pair of backticks
    if text.count("`") == 2 and text.startswith("`") and text.endswith("`"):
        parts = [(part, True) for part in text[1:-1].split(".")]
        return parts if all(p for p, _ in parts) else None

    parts = []
    pos = 0
    while True:
        match = _PART.match(text, pos)
        if not match:
            return None
        double, backtick, bracket, bare = match.groups()
        if double is not None:
            parts.append((double.replace('""', '"'), True))
        elif backtick is not None:
            parts.append((backtick, True))
        elif bracket is not None:
            parts.append((bracket, True))
        else:
            parts.append((bare.strip(), False))
        pos = match.end()
        if pos == len(text):
            return parts if all(p for p, _ in parts) else None
        if text[pos] != ".":
            return None
        pos += 1


def relation_id(relation_name: str | None, adapter_type: str | None) -> str | None:
    """The canonical ID of a physical relation, or None if the name cannot be read."""
    if not relation_name:
        return None
    parts = split_relation_name(relation_name)
    if not parts:
        return None
    return RELATION_PREFIX + ".".join(
        normalise_identifier(part, adapter_type, quoted) for part, quoted in parts
    )


def column_id(node_id: str, column_name: str, adapter_type: str | None) -> str:
    """The canonical ID of a column of a node."""
    return f"{node_id}#{normalise_identifier(column_name, adapter_type)}"


def split_column_id(column: str) -> tuple[str, str]:
    """``model.p.orders#ID`` -> ``("model.p.orders", "ID")``.

    Splits at the first ``#``: a column name may contain one, a dbt unique_id may not.
    """
    node_id, sep, name = column.partition("#")
    if not node_id or not sep:
        raise ValueError(f"not a column ID: {column!r}")
    return node_id, name


def airflow_dag_id(dag_id: str) -> str:
    return f"{AIRFLOW_PREFIX}{dag_id}"


def airflow_task_id(dag_id: str, task_id: str) -> str:
    return f"{AIRFLOW_PREFIX}{dag_id}.{task_id}"
