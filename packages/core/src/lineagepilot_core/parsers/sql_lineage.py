"""Column lineage of one compiled SQL statement, with ``sqlglot.lineage``.

Replaces data-capsule's ``sql_column_parser``, which read only the outermost SELECT and
returned a single ``*`` for 86-100% of compiled dbt models (spike S2, finding 4a). Here
sqlglot qualifies the statement against a schema built from ``catalog.json``, so
``select *`` through CTEs expands to real columns.

The result is ``unknown``, with a reason, whenever a wrong answer would be possible:
no SQL, SQL too large, SQL that does not parse, a table read that is not in the
catalog (its columns, and so ``select *`` and unqualified names, cannot be resolved),
or a ``*`` that could not be expanded. It never reports "no lineage" for a model it
could not analyse (design section 6.1).

Two measured choices (task 1.9):

- The schema is a prebuilt ``MappingSchema``. Passing a plain dict made sqlglot rebuild
  it on every call: 79 ms per model at 2,000 models instead of 31 ms.
- ``trim_selects=False``. Trimming deep-copies the SELECT for every column and node, and
  only changes the ``source`` that sqlglot shows for intermediate nodes, which is not
  used here. With it off a model takes about 3 ms (median), and 2,000 models 6.4 s.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field

from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import Dialect
from sqlglot.lineage import Node, lineage
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.schema import MappingSchema

# Compiled SQL larger than this is not analysed; sqlglot's cost grows with it, and the
# workspace is untrusted input (design section 10)
MAX_SQL_CHARS = 1_000_000

# dbt adapter_type -> sqlglot dialect, where the names differ
_DIALECTS = {
    "sqlserver": "tsql",
    "fabric": "tsql",
    "synapse": "tsql",
    "materialize": "postgres",
    "firebolt": "postgres",
}

# How much a column is changed on its way from source to output; a path takes the most
TRANSFORMATIONS = ("passthrough", "transform", "aggregate")


def sqlglot_dialect(adapter_type: str | None) -> str | None:
    """The sqlglot dialect for a dbt adapter, or None for sqlglot's generic dialect."""
    if not adapter_type:
        return None
    name = _DIALECTS.get(adapter_type.lower(), adapter_type.lower())
    try:
        Dialect.get_or_raise(name)
    except ValueError:
        return None
    return name


@dataclass(frozen=True, slots=True)
class TableRef:
    """A physical table as written after qualification: ``(name, quoted)`` parts."""

    parts: tuple[tuple[str, bool], ...]


@dataclass(frozen=True, slots=True)
class ColumnSource:
    table: TableRef
    column: str
    transformation: str  # passthrough, transform or aggregate


@dataclass
class SqlLineage:
    """Output column name -> the source columns it derives from.

    An output column with no sources is a constant or depends only on row counts
    (``count(*)``). ``unknown`` is set, with ``reason``, when the statement could not be
    analysed; ``columns`` is then empty.
    """

    columns: dict[str, list[ColumnSource]] = field(default_factory=dict)
    unknown: bool = False
    reason: str | None = None
    seconds: float = 0.0


def build_schema(
    tables: Iterable[tuple[Iterable[tuple[str, bool]], Iterable[str]]], dialect: str | None
) -> MappingSchema:
    """A sqlglot schema from ``(relation parts, column names)`` pairs.

    Names are rendered as identifiers of the dialect, quoted where the relation name
    quoted them, and column names always quoted: the catalog reports the names stored in
    the warehouse. sqlglot then normalises them exactly as it normalises the SQL.

    sqlglot needs every table at one depth. dbt writes one depth per project
    (``db.schema.table`` on most adapters), so names of any other depth are left out,
    and a model reading them comes out as unknown rather than wrong.
    """
    rendered = []
    for parts, columns in tables:
        keys = [
            exp.to_identifier(name, quoted=quoted).sql(dialect=dialect) for name, quoted in parts
        ]
        if 1 <= len(keys) <= 3:
            # Types are not needed for lineage
            cols = {
                exp.to_identifier(c, quoted=True).sql(dialect=dialect): "unknown" for c in columns
            }
            rendered.append((keys, cols))
    if not rendered:
        return MappingSchema({}, dialect=dialect)

    depth = Counter(len(keys) for keys, _ in rendered).most_common(1)[0][0]
    nested: dict[str, dict] = {}
    for keys, cols in rendered:
        if len(keys) != depth:
            continue
        level = nested
        for key in keys[:-1]:
            level = level.setdefault(key, {})
        level[keys[-1]] = cols
    return MappingSchema(nested, dialect=dialect)


def sql_column_lineage(
    sql: str | None,
    schema: MappingSchema,
    dialect: str | None,
    max_chars: int = MAX_SQL_CHARS,
) -> SqlLineage:
    """Trace every output column of a SELECT to the table columns it derives from."""
    started = time.perf_counter()
    result = _trace(sql, schema, dialect, max_chars)
    result.seconds = time.perf_counter() - started
    return result


def _unknown(reason: str) -> SqlLineage:
    return SqlLineage(unknown=True, reason=reason)


def _trace(
    sql: str | None, schema: MappingSchema, dialect: str | None, max_chars: int
) -> SqlLineage:
    if not sql or not sql.strip():
        return _unknown("no compiled SQL")
    if len(sql) > max_chars:
        return _unknown(f"compiled SQL is {len(sql):,} characters, over the {max_chars:,} limit")

    try:
        ast = parse_one(sql, dialect=dialect)
    except Exception as e:  # sqlglot raises several types; hostile SQL can recurse deeply
        return _unknown(f"SQL could not be parsed: {_short(e)}")
    if not isinstance(ast, exp.Query):
        return _unknown(f"not a query ({type(ast).__name__})")

    missing = _tables_missing_from(ast, schema, dialect)
    if missing:
        return _unknown(f"not in the catalog: {', '.join(missing)}")

    try:
        nodes = lineage(None, ast, schema, dialect=dialect, trim_selects=False, copy=False)
    except Exception as e:
        return _unknown(f"column lineage failed: {_short(e)}")
    if not isinstance(nodes, dict):  # pragma: no cover - sqlglot returns a dict for column=None
        nodes = {nodes.name: nodes}

    memo: dict[int, dict[tuple[TableRef, str], int]] = {}
    columns: dict[str, list[ColumnSource]] = {}
    for name, node in nodes.items():
        if name == "*":
            return _unknown("a * could not be expanded")
        try:
            sources = _sources(node, dialect, memo)
        except _Star:
            return _unknown(f"a * feeding column {name!r} could not be expanded")
        columns[name] = [
            ColumnSource(table, column, TRANSFORMATIONS[rank])
            for (table, column), rank in sorted(sources.items(), key=_source_order)
        ]
    return SqlLineage(columns=columns)


class _Star(Exception):
    pass


def _sources(node: Node, dialect: str | None, memo: dict) -> dict[tuple[TableRef, str], int]:
    """The table columns under a lineage node, each with the strongest transformation."""
    cached = memo.get(id(node))
    if cached is not None:
        return cached
    if not node.downstream:
        if node.name == "*" or node.name.endswith(".*"):
            raise _Star
        found = {}
        if isinstance(node.source, exp.Table):
            found[(_table_ref(node.source), _column_name(node.name, dialect))] = 0
    else:
        own = _transformation_rank(node.expression)
        found = {}
        for child in node.downstream:
            for key, rank in _sources(child, dialect, memo).items():
                found[key] = max(found.get(key, 0), rank, own)
    memo[id(node)] = found
    return found


def _transformation_rank(expression: exp.Expression) -> int:
    inner = expression.this if isinstance(expression, exp.Alias) else expression
    if inner.find(exp.AggFunc):
        return 2
    if isinstance(inner, exp.Column):
        return 0
    return 1


def _tables_missing_from(
    ast: exp.Expression, schema: MappingSchema, dialect: str | None
) -> list[str]:
    """Tables the statement reads that the schema does not know, CTEs aside."""
    cte_names = {cte.alias_or_name.lower() for cte in ast.find_all(exp.CTE)}
    missing = []
    for table in ast.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            missing.append(table.sql(dialect=dialect))  # a table function
            continue
        if not table.db and not table.catalog and table.name.lower() in cte_names:
            continue
        normalised = normalize_identifiers(table.copy(), dialect=dialect)
        if schema.find(normalised, raise_on_missing=False) is None:
            missing.append(table.sql(dialect=dialect))
    return sorted(set(missing))


def _table_ref(table: exp.Table) -> TableRef:
    parts = [
        (part.name, bool(part.args.get("quoted")))
        for part in (table.args.get("catalog"), table.args.get("db"), table.this)
        if isinstance(part, exp.Identifier) and part.name
    ]
    return TableRef(tuple(parts))


def _column_name(name: str, dialect: str | None) -> str:
    """The column of a lineage leaf name such as ``orders."Order Date"``."""
    try:
        column = parse_one(name, dialect=dialect)
    except Exception:
        column = None
    if isinstance(column, exp.Column):
        return column.name
    return name.rsplit(".", 1)[-1]


def _source_order(item: tuple[tuple[TableRef, str], int]) -> tuple:
    (table, column), _ = item
    return (table.parts, column)


def _short(error: Exception) -> str:
    text = " ".join(str(error).split())
    return f"{type(error).__name__}: {text[:200]}"
