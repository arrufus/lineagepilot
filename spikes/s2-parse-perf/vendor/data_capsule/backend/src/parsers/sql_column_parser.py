"""SQL Column Parser for extracting column-level lineage from SQL statements.

This parser uses sqlglot to analyze SELECT statements and extract column mappings,
transformations, and dependencies.
"""

from dataclasses import dataclass
from typing import Optional

import sqlglot
from sqlglot import exp


@dataclass
class ColumnMapping:
    """Represents a source-to-target column mapping."""

    # Source columns (can be multiple for joins/aggregates)
    source_columns: list[str]  # Format: "table.column" or "schema.table.column"

    # Target column
    target_column: str  # Column name (or alias)

    # Transformation metadata
    transformation_type: str  # identity, cast, aggregate, formula, join, etc.
    transformation_logic: Optional[str] = None  # SQL expression

    # Detection metadata
    confidence: float = 1.0  # 0.0 to 1.0
    detected_by: str = "sql_parser"

    # Capsule context (optional)
    source_capsule_urn: Optional[str] = None
    target_capsule_urn: Optional[str] = None


class SQLColumnParser:
    """Extract column-level lineage from SQL statements using sqlglot."""

    def __init__(self, dialect: str = "postgres"):
        """Initialize parser with SQL dialect.

        Args:
            dialect: SQL dialect (postgres, mysql, snowflake, etc.)
        """
        self.dialect = dialect

    def parse_select_statement(self, sql: str) -> list[ColumnMapping]:
        """Parse SELECT statement to extract column mappings.

        Example:
            SELECT
                a.customer_id,
                a.order_date,
                SUM(b.amount) as total_amount,
                CAST(a.status AS VARCHAR) as status_text
            FROM orders a
            JOIN order_items b ON a.id = b.order_id
            GROUP BY a.customer_id, a.order_date

        Returns:
            [
                ColumnMapping(
                    source_columns=['orders.customer_id'],
                    target_column='customer_id',
                    transformation_type='identity',
                    confidence=1.0
                ),
                ColumnMapping(
                    source_columns=['order_items.amount'],
                    target_column='total_amount',
                    transformation_type='aggregate',
                    transformation_logic='SUM(b.amount)',
                    confidence=0.95
                ),
                ColumnMapping(
                    source_columns=['orders.status'],
                    target_column='status_text',
                    transformation_type='cast',
                    transformation_logic='CAST(a.status AS VARCHAR)',
                    confidence=0.95
                )
            ]

        Args:
            sql: SQL SELECT statement

        Returns:
            List of ColumnMapping objects
        """
        try:
            # Parse SQL into AST
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # Extract column mappings from SELECT clause
            mappings = []

            # Find the main SELECT statement
            if isinstance(parsed, exp.Select):
                mappings = self._extract_from_select(parsed)
            else:
                # Try to find nested SELECT
                for select in parsed.find_all(exp.Select):
                    mappings.extend(self._extract_from_select(select))
                    break  # Only process the outermost SELECT

            return mappings

        except Exception as e:
            # If parsing fails, return empty list
            # Log error in production
            print(f"Failed to parse SQL: {e}")
            return []

    def _extract_from_select(self, select_node: exp.Select) -> list[ColumnMapping]:
        """Extract column mappings from a SELECT node.

        Args:
            select_node: sqlglot SELECT expression

        Returns:
            List of ColumnMapping objects
        """
        mappings = []

        # Get table aliases from FROM and JOIN clauses
        table_aliases = self._extract_table_aliases(select_node)

        # Process each selected expression
        for projection in select_node.expressions:
            mapping = self._extract_column_mapping(projection, table_aliases)
            if mapping:
                mappings.append(mapping)

        return mappings

    def _extract_table_aliases(self, select_node: exp.Select) -> dict[str, str]:
        """Extract table aliases from FROM and JOIN clauses.

        Args:
            select_node: sqlglot SELECT expression

        Returns:
            Dictionary mapping alias -> table name
        """
        aliases = {}

        # Get FROM clause
        if select_node.args.get("from"):
            from_expr = select_node.args["from"].this
            if isinstance(from_expr, exp.Table):
                table_name = from_expr.name
                alias = from_expr.alias or table_name
                aliases[alias] = table_name

        # Get JOIN clauses
        for join in select_node.args.get("joins", []):
            if isinstance(join.this, exp.Table):
                table_name = join.this.name
                alias = join.this.alias or table_name
                aliases[alias] = table_name

        return aliases

    def _extract_column_mapping(
        self, expr: exp.Expression, table_aliases: dict[str, str]
    ) -> Optional[ColumnMapping]:
        """Extract column mapping from a SELECT expression.

        Args:
            expr: sqlglot expression (column, aggregate, etc.)
            table_aliases: Map of alias -> table name

        Returns:
            ColumnMapping or None if unable to parse
        """
        # Get target column name (alias or column name)
        target = self._get_target_column_name(expr)
        if not target:
            return None

        # Find source columns
        source_cols = self._find_source_columns(expr, table_aliases)

        # Determine transformation type and logic
        trans_type, trans_logic, confidence = self._analyze_transformation(expr)

        return ColumnMapping(
            source_columns=source_cols,
            target_column=target,
            transformation_type=trans_type,
            transformation_logic=trans_logic,
            confidence=confidence,
            detected_by="sql_parser",
        )

    def _get_target_column_name(self, expr: exp.Expression) -> Optional[str]:
        """Get the target column name from expression.

        Args:
            expr: sqlglot expression

        Returns:
            Column name or alias, or None
        """
        # Check if expression has an alias
        if isinstance(expr, exp.Alias):
            return expr.alias

        # If it's a Column, use its name
        if isinstance(expr, exp.Column):
            return expr.name

        # For other expressions, try to get name
        if hasattr(expr, "name"):
            return expr.name

        return None

    def _find_source_columns(
        self, expr: exp.Expression, table_aliases: dict[str, str]
    ) -> list[str]:
        """Find all source columns referenced in expression.

        Args:
            expr: sqlglot expression
            table_aliases: Map of alias -> table name

        Returns:
            List of qualified column names (table.column)
        """
        source_cols = []

        # Find all Column references in the expression
        for col in expr.find_all(exp.Column):
            # Get table/alias
            table = col.table if hasattr(col, "table") and col.table else None

            # Resolve alias to actual table name
            if table and table in table_aliases:
                table = table_aliases[table]

            # Build qualified name
            if table:
                qualified = f"{table}.{col.name}"
            else:
                qualified = col.name

            source_cols.append(qualified)

        return source_cols

    def _analyze_transformation(
        self, expr: exp.Expression
    ) -> tuple[str, Optional[str], float]:
        """Analyze the transformation applied to create target column.

        Args:
            expr: sqlglot expression

        Returns:
            Tuple of (transformation_type, transformation_logic, confidence)
        """
        # Handle Alias wrapper
        inner = expr
        if isinstance(expr, exp.Alias):
            inner = expr.this

        # Identity (direct column reference)
        if isinstance(inner, exp.Column):
            return ("identity", None, 1.0)

        # Cast
        if isinstance(inner, exp.Cast):
            return ("cast", inner.sql(), 0.95)

        # Aggregates (SUM, AVG, COUNT, etc.)
        if isinstance(inner, (exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)):
            return ("aggregate", inner.sql(), 0.95)

        # String operations (CONCAT, SUBSTRING, etc.)
        if isinstance(inner, (exp.Concat, exp.Substring, exp.Upper, exp.Lower)):
            return ("string_transform", inner.sql(), 0.9)

        # Arithmetic operations
        if isinstance(inner, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
            return ("arithmetic", inner.sql(), 0.9)

        # Date/time operations
        if isinstance(
            inner,
            (
                exp.DateAdd,
                exp.DateSub,
                exp.DateDiff,
                exp.Extract,
                exp.DateTrunc,
            ),
        ):
            return ("date_transform", inner.sql(), 0.9)

        # CASE WHEN
        if isinstance(inner, exp.Case):
            return ("conditional", inner.sql(), 0.85)

        # Subquery
        if isinstance(inner, exp.Subquery):
            return ("subquery", inner.sql(), 0.7)

        # Default: complex formula
        return ("formula", inner.sql(), 0.8)

    def parse_insert_statement(self, sql: str) -> list[ColumnMapping]:
        """Parse INSERT statement to extract column mappings.

        Example:
            INSERT INTO target_table (col1, col2)
            SELECT source_col1, source_col2
            FROM source_table

        Args:
            sql: SQL INSERT statement

        Returns:
            List of ColumnMapping objects
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # Find INSERT node
            if not isinstance(parsed, exp.Insert):
                return []

            # Get target columns
            target_cols = []
            if parsed.this and isinstance(parsed.this, exp.Schema):
                target_cols = [col.name for col in parsed.this.expressions]

            # Get SELECT statement
            select_node = None
            if parsed.expression and isinstance(parsed.expression, exp.Select):
                select_node = parsed.expression

            if not select_node or not target_cols:
                return []

            # Extract source columns from SELECT
            source_mappings = self._extract_from_select(select_node)

            # Map to target columns
            mappings = []
            for i, mapping in enumerate(source_mappings):
                if i < len(target_cols):
                    mappings.append(
                        ColumnMapping(
                            source_columns=mapping.source_columns,
                            target_column=target_cols[i],
                            transformation_type=mapping.transformation_type,
                            transformation_logic=mapping.transformation_logic,
                            confidence=mapping.confidence * 0.95,  # Slightly lower confidence
                            detected_by="sql_parser",
                        )
                    )

            return mappings

        except Exception as e:
            print(f"Failed to parse INSERT: {e}")
            return []

    def parse_update_statement(self, sql: str) -> list[ColumnMapping]:
        """Parse UPDATE statement to extract column mappings.

        Example:
            UPDATE target_table
            SET col1 = source_table.col1,
                col2 = UPPER(source_table.col2)
            FROM source_table
            WHERE target_table.id = source_table.target_id

        Args:
            sql: SQL UPDATE statement

        Returns:
            List of ColumnMapping objects
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            if not isinstance(parsed, exp.Update):
                return []

            mappings = []
            table_aliases = {}

            # Get FROM clause for aliases
            if parsed.args.get("from"):
                from_expr = parsed.args["from"].this
                if isinstance(from_expr, exp.Table):
                    table_name = from_expr.name
                    alias = from_expr.alias or table_name
                    table_aliases[alias] = table_name

            # Process SET expressions
            for set_expr in parsed.expressions:
                if isinstance(set_expr, exp.EQ):
                    # Left side is target column
                    target = set_expr.this.name if isinstance(set_expr.this, exp.Column) else None

                    if target:
                        # Right side is source expression
                        source_cols = self._find_source_columns(set_expr.expression, table_aliases)
                        trans_type, trans_logic, confidence = self._analyze_transformation(
                            set_expr.expression
                        )

                        mappings.append(
                            ColumnMapping(
                                source_columns=source_cols,
                                target_column=target,
                                transformation_type=trans_type,
                                transformation_logic=trans_logic,
                                confidence=confidence * 0.9,  # Lower confidence for UPDATEs
                                detected_by="sql_parser",
                            )
                        )

            return mappings

        except Exception as e:
            print(f"Failed to parse UPDATE: {e}")
            return []
