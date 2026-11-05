"""Parse results shared by the metadata parsers.

Ported from data-capsule ``backend/src/parsers/base.py`` (commit 5e57fcc). Changes:

- Identity is the dbt ``unique_id`` (design section 5.1). The server's URNs are gone,
  because versioned models shared one (spike S2 finding 4d).
- ``RawCapsule`` is now ``RawNode``. Edges name their ends ``upstream_id`` and
  ``downstream_id``, so the direction is never in doubt.
- References that cannot be resolved are kept as ``RawUnresolved`` records, never
  dropped (design section 5.2, review finding F07).
- ``parse()`` is synchronous. It never awaited anything; indexing runs in a child
  process instead (S2 suspicion 3).
- The Airflow, pipeline and column-mapping records went with the server's Airflow
  parser. Phase 5 adds orchestration to the metadata graph.
- Record classes use ``slots`` to keep the indexer's memory down (S2 finding 2).
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ParseErrorSeverity(str, Enum):
    """Severity levels for parse errors."""

    WARNING = "warning"
    ERROR = "error"


@dataclass
class ParseError:
    """An error or warning raised while parsing."""

    message: str
    severity: ParseErrorSeverity = ParseErrorSeverity.WARNING
    location: str | None = None
    context: dict[str, Any] | None = None

    def __str__(self) -> str:
        loc = f" at {self.location}" if self.location else ""
        return f"[{self.severity.value.upper()}]{loc}: {self.message}"


def source_sha256(text: str) -> str:
    """A hash of a model's source text, for comparing a file with the manifest's ``raw_code``.

    Stripped, as dbt strips the file it reads. And with line endings normalised, because they
    are not the file's text: git checks the same file out with CRLF on Windows and LF elsewhere,
    so a manifest built on one must not call the file stale on the other.
    """
    return hashlib.sha256(text.replace("\r\n", "\n").strip().encode("utf-8")).hexdigest()


@dataclass(slots=True)
class RawNode:
    """A dbt node (model, source, seed, snapshot or exposure), keyed by ``unique_id``."""

    unique_id: str
    resource_type: str
    name: str
    package_name: str | None = None

    # Location
    database_name: str | None = None
    schema_name: str | None = None
    relation_name: str | None = None
    alias: str | None = None
    version: str | None = None

    # Classification; layer_source says how the layer was decided:
    # meta, tag, pattern (a name or path pattern) or default (seeds and sources)
    layer: str | None = None
    layer_source: str | None = None
    domain_name: str | None = None

    # Documentation
    description: str | None = None
    materialization: str | None = None

    # Relationships: the manifest's depends_on.nodes, in order, without duplicates
    depends_on: list[str] = field(default_factory=list)

    # Ownership: meta.owner for nodes, owner.name (or email) for exposures
    owner: str | None = None
    group: str | None = None

    # The node's own dbt meta, tags and config
    meta: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)

    # Quality, derived from the manifest's test.* nodes
    has_tests: bool = False
    test_count: int = 0
    test_types: list[str] = field(default_factory=list)

    # Files: the SQL or YAML that defines the node, and the schema.yml that documents it
    original_file_path: str | None = None
    patch_path: str | None = None

    # Kind-specific fields: source_name, identifier and loader for sources;
    # type, maturity, url and label for exposures
    properties: dict[str, Any] = field(default_factory=dict)

    # Column lineage inputs: compiled SQL from the manifest (after dbt compile or
    # docs generate), the file dbt compiles it to, and sql or python
    language: str | None = None
    compiled_code: str | None = None
    compiled_path: str | None = None

    # source_sha256() of a model's raw_code. Comparing it with the file says whether the manifest
    # has seen the file's current text. Not the manifest's own checksum: on the real fixtures
    # that does not match the file or raw_code.
    source_sha256: str | None = None

    # True if catalog.json has an entry for this node
    in_catalog: bool = False

    # Columns of a primary-key constraint in a dbt model contract
    primary_key: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RawColumn:
    """A column of a node, from the manifest and catalog."""

    node_id: str
    name: str

    # Type info
    data_type: str | None = None
    ordinal_position: int | None = None

    # Classification
    semantic_type: str | None = None
    pii_type: str | None = None
    pii_detected_by: str | None = None

    # Documentation
    description: str | None = None

    # Metadata
    meta: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    # Statistics from the catalog
    stats: dict[str, Any] = field(default_factory=dict)

    # Quality, derived from the manifest's test.* nodes
    has_tests: bool = False
    test_count: int = 0
    test_types: list[str] = field(default_factory=list)

    # True if catalog.json lists this column, so it really exists in the warehouse
    in_catalog: bool = False


@dataclass(slots=True)
class RawEdge:
    """A node-level lineage edge: data flows from ``upstream_id`` to ``downstream_id``."""

    upstream_id: str
    downstream_id: str
    transformation: str = "dependency"  # source, ref or dependency


@dataclass(slots=True)
class RawUnresolved:
    """A reference the parser could not resolve, kept so it can be shown to the user."""

    node_id: str  # where the reference appears
    reference: str  # what it points at
    reason: str


@dataclass
class RawDomain:
    """A business domain."""

    name: str
    description: str | None = None


@dataclass
class RawTag:
    """A tag, with a category when one can be inferred."""

    name: str
    category: str | None = None


class SchemaSupport(str, Enum):
    """How the manifest's ``metadata.dbt_schema_version`` relates to the tested version."""

    SUPPORTED = "supported"
    OLDER = "older"  # parsed best-effort; untested
    NEWER = "newer"  # parsed best-effort after structural checks
    UNVERSIONED = "unversioned"  # no readable version; treated like NEWER


@dataclass
class ParseResult:
    """Result of parsing metadata from a source.

    A result with ERROR-level errors is not trustworthy: the index built from it must be
    shown as ``unknown`` (design section 6.1), not as an empty or partial graph.
    """

    nodes: list[RawNode] = field(default_factory=list)
    columns: list[RawColumn] = field(default_factory=list)
    edges: list[RawEdge] = field(default_factory=list)
    unresolved: list[RawUnresolved] = field(default_factory=list)
    domains: list[RawDomain] = field(default_factory=list)
    tags: list[RawTag] = field(default_factory=list)

    errors: list[ParseError] = field(default_factory=list)

    # About the parse and the artifact it read (design section 5.2, artifact_version)
    source_type: str = ""
    project_name: str = ""
    adapter_type: str | None = None
    dbt_version: str | None = None
    schema_version: int | None = None
    schema_support: SchemaSupport | None = None
    generated_at: str | None = None
    # The files read, and their sha256, for evidence records and snapshot keys
    manifest_path: str | None = None
    manifest_sha256: str | None = None
    catalog_path: str | None = None
    catalog_sha256: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_errors(self) -> bool:
        """True if there is any ERROR-level issue."""
        return any(e.severity == ParseErrorSeverity.ERROR for e in self.errors)

    @property
    def warning_count(self) -> int:
        return sum(1 for e in self.errors if e.severity == ParseErrorSeverity.WARNING)

    @property
    def error_count(self) -> int:
        return sum(1 for e in self.errors if e.severity == ParseErrorSeverity.ERROR)

    def add_error(
        self,
        message: str,
        severity: ParseErrorSeverity = ParseErrorSeverity.WARNING,
        location: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.errors.append(
            ParseError(message=message, severity=severity, location=location, context=context)
        )

    def summary(self) -> dict[str, Any]:
        """Counts of everything in the result."""
        return {
            "nodes": len(self.nodes),
            "columns": len(self.columns),
            "edges": len(self.edges),
            "unresolved": len(self.unresolved),
            "domains": len(self.domains),
            "tags": len(self.tags),
            "warnings": self.warning_count,
            "errors": self.error_count,
        }


class MetadataParser(ABC):
    """Base class for metadata parsers."""

    @property
    @abstractmethod
    def source_type(self) -> str:
        """The source type identifier, for example ``dbt``."""

    @abstractmethod
    def parse(self, config: dict[str, Any]) -> ParseResult:
        """Parse metadata from the source and return normalised results."""

    @abstractmethod
    def validate_config(self, config: dict[str, Any]) -> list[str]:
        """Return configuration problems; an empty list means the config is valid."""
