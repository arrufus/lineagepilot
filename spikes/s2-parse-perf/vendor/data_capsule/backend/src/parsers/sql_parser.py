"""SQL parsing utilities for extracting table references from SQL statements."""

import logging
from dataclasses import dataclass, field
from typing import Optional

from sqlglot import parse_one, exp
from sqlglot.errors import ParseError as SqlglotParseError

logger = logging.getLogger(__name__)


@dataclass
class TableReference:
    """Represents a table reference extracted from SQL."""

    schema: Optional[str]
    table: str
    database: Optional[str] = None
    alias: Optional[str] = None

    @property
    def qualified_name(self) -> str:
        """Get fully qualified table name."""
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.table)
        return ".".join(parts)


@dataclass
class SqlParseResult:
    """Result of parsing a SQL statement."""

    reads: list[TableReference] = field(default_factory=list)
    writes: list[TableReference] = field(default_factory=list)
    operation_type: Optional[str] = None  # "select", "insert", "update", "merge", "delete", "create"
    is_valid: bool = True
    errors: list[str] = field(default_factory=list)


def extract_table_reference(table_exp: exp.Table) -> TableReference:
    """Extract table reference from sqlglot Table expression."""
    return TableReference(
        database=table_exp.catalog if hasattr(table_exp, "catalog") else None,
        schema=table_exp.db if hasattr(table_exp, "db") else None,
        table=table_exp.name,
        alias=table_exp.alias if hasattr(table_exp, "alias") else None,
    )


def parse_sql(sql: str, dialect: str = "postgres") -> SqlParseResult:
    """Parse SQL statement to extract table references.

    Args:
        sql: SQL statement to parse
        dialect: SQL dialect (postgres, snowflake, bigquery, mysql, etc.)

    Returns:
        SqlParseResult with reads/writes lists
    """
    result = SqlParseResult()

    if not sql or not sql.strip():
        result.is_valid = False
        result.errors.append("Empty SQL statement")
        return result

    try:
        # Parse SQL
        ast = parse_one(sql, dialect=dialect)

        # Determine operation type
        if isinstance(ast, exp.Select):
            result.operation_type = "select"
            # Extract all tables from FROM and JOIN clauses
            for table in ast.find_all(exp.Table):
                table_ref = extract_table_reference(table)
                result.reads.append(table_ref)

        elif isinstance(ast, exp.Insert):
            result.operation_type = "insert"
            # Target table (write) - for INSERT, the table might be in ast.this or wrapped in Schema
            target_table = None
            if hasattr(ast, "this") and ast.this:
                # ast.this might be a Schema wrapping the Table
                if isinstance(ast.this, exp.Schema) and hasattr(ast.this, "this"):
                    target_table = ast.this.this
                elif isinstance(ast.this, exp.Table):
                    target_table = ast.this

            if target_table and isinstance(target_table, exp.Table):
                table_ref = extract_table_reference(target_table)
                result.writes.append(table_ref)

            # Source tables (read) - from SELECT within INSERT
            for table in ast.find_all(exp.Table):
                if table != target_table:
                    table_ref = extract_table_reference(table)
                    result.reads.append(table_ref)

        elif isinstance(ast, exp.Update):
            result.operation_type = "update"
            # Target table (both read and write for UPDATE)
            if ast.this:
                table_ref = extract_table_reference(ast.this)
                result.writes.append(table_ref)
                result.reads.append(table_ref)
            # Additional tables in FROM/JOIN
            for table in ast.find_all(exp.Table):
                if table != ast.this:
                    table_ref = extract_table_reference(table)
                    result.reads.append(table_ref)

        elif isinstance(ast, exp.Merge):
            result.operation_type = "merge"
            # Target table (write)
            if hasattr(ast, "this") and ast.this and isinstance(ast.this, exp.Table):
                table_ref = extract_table_reference(ast.this)
                result.writes.append(table_ref)
            # Source table (read) - MERGE has multiple possible locations for source
            if hasattr(ast, "using") and ast.using:
                for table in ast.using.find_all(exp.Table):
                    table_ref = extract_table_reference(table)
                    result.reads.append(table_ref)
            else:
                # Fallback: find all tables not already in writes
                write_tables = {ref.qualified_name for ref in result.writes}
                for table in ast.find_all(exp.Table):
                    table_ref = extract_table_reference(table)
                    if table_ref.qualified_name not in write_tables:
                        result.reads.append(table_ref)

        elif isinstance(ast, exp.Delete):
            result.operation_type = "delete"
            # Target table (both read and write)
            if ast.this:
                table_ref = extract_table_reference(ast.this)
                result.writes.append(table_ref)
                result.reads.append(table_ref)

        elif isinstance(ast, exp.Create):
            result.operation_type = "create"
            # Created table (write)
            if ast.this:
                table_ref = extract_table_reference(ast.this)
                result.writes.append(table_ref)
            # Source tables (read) - from SELECT within CREATE TABLE AS
            for table in ast.find_all(exp.Table):
                if table != ast.this:
                    table_ref = extract_table_reference(table)
                    result.reads.append(table_ref)

        elif isinstance(ast, exp.Drop):
            result.operation_type = "drop"
            # Dropped table (write operation)
            if ast.this:
                table_ref = extract_table_reference(ast.this)
                result.writes.append(table_ref)

        else:
            result.operation_type = "unknown"
            # Best effort - extract all tables
            for table in ast.find_all(exp.Table):
                table_ref = extract_table_reference(table)
                result.reads.append(table_ref)

        # Remove duplicates while preserving order
        result.reads = list({ref.qualified_name: ref for ref in result.reads}.values())
        result.writes = list({ref.qualified_name: ref for ref in result.writes}.values())

    except SqlglotParseError as e:
        result.is_valid = False
        result.errors.append(f"SQL parse error: {str(e)}")
        logger.warning(f"Failed to parse SQL: {e}")
    except Exception as e:
        result.is_valid = False
        result.errors.append(f"Unexpected error: {str(e)}")
        logger.exception(f"Unexpected error parsing SQL: {e}")

    return result


def parse_sql_file(file_path: str, dialect: str = "postgres") -> SqlParseResult:
    """Parse SQL from a file.

    Args:
        file_path: Path to SQL file
        dialect: SQL dialect

    Returns:
        SqlParseResult
    """
    try:
        with open(file_path, "r") as f:
            sql = f.read()
        return parse_sql(sql, dialect=dialect)
    except FileNotFoundError:
        result = SqlParseResult()
        result.is_valid = False
        result.errors.append(f"File not found: {file_path}")
        return result
    except Exception as e:
        result = SqlParseResult()
        result.is_valid = False
        result.errors.append(f"Error reading file: {str(e)}")
        return result


def match_table_to_capsule_urn(
    table_ref: TableReference,
    source_type: str = "postgres",
    instance_name: Optional[str] = None,
) -> str:
    """Convert table reference to a DataCapsule URN.

    Args:
        table_ref: Table reference extracted from SQL
        source_type: Source system type (postgres, snowflake, bigquery, etc.)
        instance_name: Instance name for URN

    Returns:
        Capsule URN string
    """
    # Build URN based on source type
    if source_type == "dbt":
        # dbt URN format: urn:dcs:dbt:model:{project}.{layer}:{model_name}
        # Use schema as layer approximation
        layer = table_ref.schema or "unknown"
        return f"urn:dcs:dbt:model:{instance_name or 'default'}.{layer}:{table_ref.table}"

    else:
        # Standard database URN format: urn:dcs:{source}:table:{database}.{schema}:{table}
        # postgres: urn:dcs:postgres:table:postgres.public:customers
        # snowflake: urn:dcs:snowflake:table:analytics.raw:customers
        parts = []
        if table_ref.database:
            parts.append(table_ref.database)
        if table_ref.schema:
            parts.append(table_ref.schema)

        schema_part = ".".join(parts) if parts else "public"
        return f"urn:dcs:{source_type}:table:{schema_part}:{table_ref.table}"
