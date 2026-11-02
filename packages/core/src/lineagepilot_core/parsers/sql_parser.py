"""Extract table references from SQL statements.

Ported from data-capsule ``backend/src/parsers/sql_parser.py`` (commit 5e57fcc). Changes:

- References to a CTE by name are no longer reported as table reads.
- DROP reads sqlglot 30's list of dropped tables; the old code failed on every DROP.
- Tokenizer errors are reported like parse errors, without a logged traceback.
- ``match_table_to_capsule_urn`` is gone with the server's URNs (design section 5.1).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError

logger = logging.getLogger(__name__)


@dataclass
class TableReference:
    """A table reference extracted from SQL."""

    schema: str | None
    table: str
    database: str | None = None
    alias: str | None = None

    @property
    def qualified_name(self) -> str:
        """``database.schema.table``, leaving out the parts that are absent."""
        return ".".join(part for part in (self.database, self.schema, self.table) if part)


@dataclass
class SqlParseResult:
    """Result of parsing one SQL statement."""

    reads: list[TableReference] = field(default_factory=list)
    writes: list[TableReference] = field(default_factory=list)
    operation_type: str | None = None  # select, insert, update, merge, delete, create, drop
    is_valid: bool = True
    errors: list[str] = field(default_factory=list)


def extract_table_reference(table_exp: exp.Table) -> TableReference:
    """Build a TableReference from a sqlglot Table expression."""
    return TableReference(
        database=table_exp.catalog or None,
        schema=table_exp.db or None,
        table=table_exp.name,
        alias=table_exp.alias or None,
    )


def _source_tables(ast: exp.Expression, exclude: exp.Expression | None = None) -> list[exp.Table]:
    """Every table in the statement except ``exclude`` and references to its own CTEs."""
    cte_names = {cte.alias_or_name.lower() for cte in ast.find_all(exp.CTE)}
    return [
        table
        for table in ast.find_all(exp.Table)
        if table is not exclude
        and not (not table.db and not table.catalog and table.name.lower() in cte_names)
    ]


def parse_sql(sql: str, dialect: str = "postgres") -> SqlParseResult:
    """Parse one SQL statement into the tables it reads and writes.

    Args:
        sql: the SQL statement
        dialect: a sqlglot dialect (postgres, snowflake, bigquery, duckdb, ...)
    """
    result = SqlParseResult()

    if not sql or not sql.strip():
        result.is_valid = False
        result.errors.append("Empty SQL statement")
        return result

    try:
        ast = parse_one(sql, dialect=dialect)

        if isinstance(ast, exp.Select):
            result.operation_type = "select"
            result.reads.extend(extract_table_reference(t) for t in _source_tables(ast))

        elif isinstance(ast, exp.Insert):
            result.operation_type = "insert"
            # The target can be a Table, or a Schema wrapping one when columns are listed
            target = ast.this.this if isinstance(ast.this, exp.Schema) else ast.this
            if isinstance(target, exp.Table):
                result.writes.append(extract_table_reference(target))
            else:
                target = None
            result.reads.extend(extract_table_reference(t) for t in _source_tables(ast, target))

        elif isinstance(ast, exp.Update):
            result.operation_type = "update"
            # UPDATE both reads and writes its target
            if isinstance(ast.this, exp.Table):
                result.writes.append(extract_table_reference(ast.this))
                result.reads.append(extract_table_reference(ast.this))
            result.reads.extend(extract_table_reference(t) for t in _source_tables(ast, ast.this))

        elif isinstance(ast, exp.Merge):
            result.operation_type = "merge"
            if isinstance(ast.this, exp.Table):
                result.writes.append(extract_table_reference(ast.this))
            using = ast.args.get("using")
            if using is not None:
                result.reads.extend(extract_table_reference(t) for t in using.find_all(exp.Table))
            else:
                written = {ref.qualified_name for ref in result.writes}
                for table in _source_tables(ast):
                    ref = extract_table_reference(table)
                    if ref.qualified_name not in written:
                        result.reads.append(ref)

        elif isinstance(ast, exp.Delete):
            result.operation_type = "delete"
            # DELETE both reads and writes its target
            if isinstance(ast.this, exp.Table):
                result.writes.append(extract_table_reference(ast.this))
                result.reads.append(extract_table_reference(ast.this))

        elif isinstance(ast, exp.Create):
            result.operation_type = "create"
            target = ast.this.this if isinstance(ast.this, exp.Schema) else ast.this
            if isinstance(target, exp.Table):
                result.writes.append(extract_table_reference(target))
            else:
                target = None
            result.reads.extend(extract_table_reference(t) for t in _source_tables(ast, target))

        elif isinstance(ast, exp.Drop):
            result.operation_type = "drop"
            # sqlglot 30 keeps dropped tables in a list (DROP TABLE a, b), not in `this`
            result.writes.extend(extract_table_reference(t) for t in ast.find_all(exp.Table))

        else:
            result.operation_type = "unknown"
            # Best effort: every table is a read
            result.reads.extend(extract_table_reference(t) for t in _source_tables(ast))

        # Remove duplicates while preserving order
        result.reads = list({ref.qualified_name: ref for ref in result.reads}.values())
        result.writes = list({ref.qualified_name: ref for ref in result.writes}.values())

    except SqlglotError as e:
        result.is_valid = False
        result.errors.append(f"SQL parse error: {e}")
        logger.warning("Failed to parse SQL: %s", e)
    except Exception as e:  # sqlglot can raise others on hostile input
        result.is_valid = False
        result.errors.append(f"Unexpected error: {e}")
        logger.exception("Unexpected error parsing SQL")

    return result


def parse_sql_file(file_path: str, dialect: str = "postgres") -> SqlParseResult:
    """Parse the SQL in a file."""
    try:
        with open(file_path, encoding="utf-8") as f:
            sql = f.read()
    except FileNotFoundError:
        result = SqlParseResult(is_valid=False)
        result.errors.append(f"File not found: {file_path}")
        return result
    except (OSError, UnicodeDecodeError) as e:
        result = SqlParseResult(is_valid=False)
        result.errors.append(f"Error reading file: {e}")
        return result
    return parse_sql(sql, dialect=dialect)
