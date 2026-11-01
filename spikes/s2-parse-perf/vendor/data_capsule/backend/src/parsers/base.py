"""Base classes and interfaces for metadata parsers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class ParseErrorSeverity(str, Enum):
    """Severity levels for parse errors."""

    WARNING = "warning"
    ERROR = "error"


@dataclass
class ParseError:
    """Represents an error or warning during parsing."""

    message: str
    severity: ParseErrorSeverity = ParseErrorSeverity.WARNING
    location: Optional[str] = None
    context: Optional[dict[str, Any]] = None

    def __str__(self) -> str:
        loc = f" at {self.location}" if self.location else ""
        return f"[{self.severity.value.upper()}]{loc}: {self.message}"


@dataclass
class RawCapsule:
    """Raw capsule data extracted from source before transformation."""

    urn: str
    name: str
    capsule_type: str
    unique_id: str

    # Location
    database_name: Optional[str] = None
    schema_name: Optional[str] = None

    # Classification
    layer: Optional[str] = None
    domain_name: Optional[str] = None

    # Documentation
    description: Optional[str] = None
    materialization: Optional[str] = None

    # Relationships
    depends_on: list[str] = field(default_factory=list)

    # Metadata
    meta: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)

    # Quality
    has_tests: bool = False
    test_count: int = 0

    # Source-specific
    original_file_path: Optional[str] = None
    package_name: Optional[str] = None


@dataclass
class RawColumn:
    """Raw column data extracted from source before transformation."""

    urn: str
    capsule_urn: str
    name: str

    # Type info
    data_type: Optional[str] = None
    ordinal_position: Optional[int] = None
    is_nullable: bool = True

    # Classification
    semantic_type: Optional[str] = None
    pii_type: Optional[str] = None
    pii_detected_by: Optional[str] = None

    # Documentation
    description: Optional[str] = None

    # Metadata
    meta: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    # Statistics
    stats: dict[str, Any] = field(default_factory=dict)

    # Quality
    has_tests: bool = False
    test_count: int = 0


@dataclass
class RawEdge:
    """Raw edge data for lineage relationships."""

    source_urn: str
    target_urn: str
    edge_type: str = "flows_to"
    transformation: Optional[str] = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class RawColumnEdge:
    """Raw edge data for column-level lineage relationships."""

    source_column_urn: str
    target_column_urn: str
    transformation_type: Optional[str] = None  # passthrough, transform, aggregate, etc.
    transformation_expr: Optional[str] = None  # SQL expression if available
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class RawDomain:
    """Raw domain data."""

    name: str
    description: Optional[str] = None


@dataclass
class RawTag:
    """Raw tag data."""

    name: str
    category: Optional[str] = None


@dataclass
class RawPipeline:
    """Raw pipeline data for orchestration workflows (Airflow DAGs, etc.)."""

    urn: str
    name: str
    pipeline_type: str  # airflow_dag, dbt_run, databricks_job, etc.
    source_system_identifier: str  # Original ID in source system (e.g., DAG ID)

    # Optional metadata
    description: Optional[str] = None
    schedule_interval: Optional[str] = None
    owners: list[str] = field(default_factory=list)
    is_paused: bool = False
    is_active: bool = True

    # Metadata
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    # Domain inference
    domain_name: Optional[str] = None


@dataclass
class RawPipelineTask:
    """Raw pipeline task data (task within a workflow)."""

    urn: str
    name: str
    task_type: str  # python, sql, dbt, bash, sensor, etc.
    pipeline_urn: str  # Parent pipeline URN

    # Optional metadata
    description: Optional[str] = None
    operator: Optional[str] = None  # e.g., PythonOperator, DbtRunOperator
    operation_type: Optional[str] = None  # extract, transform, load, validate, etc.

    # Retry and timeout configuration
    retries: int = 0
    retry_delay_seconds: Optional[int] = None
    timeout_seconds: Optional[int] = None

    # Tool integration (for deep dbt/SQL integration)
    tool_reference: Optional[dict[str, Any]] = None

    # Metadata
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class RawOrchestrationEdge:
    """Raw edge data for orchestration relationships.

    Supports three edge types:
    - task_dependency: Task -> Task (DEPENDS_ON)
    - task_data: Task -> DataCapsule (PRODUCES/CONSUMES/TRANSFORMS/VALIDATES)
    - pipeline_trigger: Pipeline -> Pipeline (TRIGGERS)
    """

    source_urn: str
    target_urn: str
    edge_category: str  # "task_dependency", "task_data", "pipeline_trigger"
    edge_type: str  # depends_on, produces, consumes, transforms, validates, triggers

    # Edge-specific metadata
    operation: Optional[str] = None  # For task_data edges: insert, update, select, etc.
    access_pattern: Optional[str] = None  # For task_data edges: full_scan, selective, etc.
    transformation_type: Optional[str] = None  # For task_data edges
    validation_type: Optional[str] = None  # For task_data edges

    # General metadata
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class RawColumnMapping:
    """Raw column-level lineage mapping (Phase 6).

    Represents a source-to-target column mapping detected from SQL,
    dbt, or manual annotations.
    """

    # Source columns (can be multiple for joins/aggregates)
    source_columns: list[str]  # Qualified names: "table.column" or "schema.table.column"

    # Target column
    target_column: str  # Column name (or alias)

    # Transformation metadata
    transformation_type: str  # identity, cast, aggregate, formula, etc.
    transformation_logic: Optional[str] = None  # SQL expression

    # Detection metadata
    confidence: float = 1.0  # 0.0 to 1.0
    detected_by: str = "sql_parser"  # sql_parser, dbt_metadata, manual

    # Context (optional)
    task_urn: Optional[str] = None  # Task that performs this transformation
    source_capsule_urn: Optional[str] = None  # Source capsule URN
    target_capsule_urn: Optional[str] = None  # Target capsule URN

    # Additional metadata
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class ParseResult:
    """Result of parsing metadata from a source."""

    capsules: list[RawCapsule] = field(default_factory=list)
    columns: list[RawColumn] = field(default_factory=list)
    edges: list[RawEdge] = field(default_factory=list)
    column_edges: list[RawColumnEdge] = field(default_factory=list)
    domains: list[RawDomain] = field(default_factory=list)
    tags: list[RawTag] = field(default_factory=list)

    # Orchestration metadata (Airflow Integration)
    pipelines: list[RawPipeline] = field(default_factory=list)
    pipeline_tasks: list[RawPipelineTask] = field(default_factory=list)
    orchestration_edges: list[RawOrchestrationEdge] = field(default_factory=list)

    # Phase 6: Column-level lineage mappings
    column_mappings: list[RawColumnMapping] = field(default_factory=list)

    errors: list[ParseError] = field(default_factory=list)

    # Metadata about the parse
    source_type: str = ""
    source_name: str = ""
    source_version: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_errors(self) -> bool:
        """Check if there are any ERROR-level issues."""
        return any(e.severity == ParseErrorSeverity.ERROR for e in self.errors)

    @property
    def warning_count(self) -> int:
        """Count warnings."""
        return sum(1 for e in self.errors if e.severity == ParseErrorSeverity.WARNING)

    @property
    def error_count(self) -> int:
        """Count errors."""
        return sum(1 for e in self.errors if e.severity == ParseErrorSeverity.ERROR)

    def add_error(
        self,
        message: str,
        severity: ParseErrorSeverity = ParseErrorSeverity.WARNING,
        location: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add an error to the result."""
        self.errors.append(
            ParseError(
                message=message,
                severity=severity,
                location=location,
                context=context,
            )
        )

    def summary(self) -> dict[str, Any]:
        """Get summary statistics of the parse result."""
        return {
            "capsules": len(self.capsules),
            "columns": len(self.columns),
            "edges": len(self.edges),
            "column_edges": len(self.column_edges),
            "domains": len(self.domains),
            "tags": len(self.tags),
            "pipelines": len(self.pipelines),
            "pipeline_tasks": len(self.pipeline_tasks),
            "orchestration_edges": len(self.orchestration_edges),
            "warnings": self.warning_count,
            "errors": self.error_count,
        }


class MetadataParser(ABC):
    """Abstract base class for all metadata parsers."""

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Return the source type identifier (e.g., 'dbt', 'snowflake')."""
        pass

    @abstractmethod
    async def parse(self, config: dict[str, Any]) -> ParseResult:
        """
        Parse metadata from the source and return normalized results.

        Args:
            config: Parser-specific configuration dictionary

        Returns:
            ParseResult containing all extracted metadata
        """
        pass

    @abstractmethod
    def validate_config(self, config: dict[str, Any]) -> list[str]:
        """
        Validate parser configuration.

        Args:
            config: Configuration dictionary to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        pass


class ParserRegistry:
    """Registry of metadata parsers by source type."""

    def __init__(self) -> None:
        self._parsers: dict[str, type[MetadataParser]] = {}

    def register(self, source_type: str, parser_class: type[MetadataParser]) -> None:
        """Register a parser class for a source type."""
        self._parsers[source_type] = parser_class

    def get(self, source_type: str) -> MetadataParser:
        """Get a parser instance for the given source type."""
        if source_type not in self._parsers:
            available = ", ".join(self._parsers.keys()) or "none"
            raise ValueError(
                f"No parser registered for source type: {source_type}. "
                f"Available: {available}"
            )
        return self._parsers[source_type]()

    def available_sources(self) -> list[str]:
        """List available source types."""
        return list(self._parsers.keys())

    def is_registered(self, source_type: str) -> bool:
        """Check if a source type is registered."""
        return source_type in self._parsers
