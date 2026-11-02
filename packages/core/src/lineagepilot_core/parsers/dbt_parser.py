"""dbt parser for manifest.json and catalog.json.

Ported from data-capsule ``backend/src/parsers/dbt_parser.py`` (commit 5e57fcc), with the
fixes spike S2 called for (``spikes/s2-parse-perf/README.md``):

- Nodes are keyed by ``unique_id``, so versioned models no longer collide (finding 4d).
- ``has_tests``, ``test_count`` and ``test_types`` come from the manifest's ``test.*``
  nodes: ``attached_node`` and ``column_name`` for models, ``depends_on`` for sources
  (suspicion 2). The old code read a ``data_tests`` key that real manifests never have.
- Exposures are parsed (finding 4e). Sources keep ``original_file_path``, and every node
  keeps ``patch_path``, the YAML file that documents it.
- ``metadata.dbt_schema_version`` is checked, with structural checks for versions newer
  than v12 (finding 4i).
- The column-lineage pass is gone. It read three keys that no real Core or Fusion manifest
  has (finding 4c), and its column lookup was the O(columns x nodes) loop that broke the
  cold-index budget (suspicion 1). Column lineage comes from compiled SQL (task 1.9).
- Dependencies that do not resolve to a parsed node become ``RawUnresolved`` records.
- Everything read from the manifest is type-checked first: the workspace is untrusted
  input (design section 10), and one malformed field should cost one node at most.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lineagepilot_core.parsers import _json
from lineagepilot_core.parsers.base import (
    MetadataParser,
    ParseErrorSeverity,
    ParseResult,
    RawColumn,
    RawDomain,
    RawEdge,
    RawNode,
    RawTag,
    RawUnresolved,
    SchemaSupport,
)
from lineagepilot_core.parsers.dbt_config import DbtParserConfig

logger = logging.getLogger(__name__)

# dbt Core 1.8-1.12 and Fusion 2.0 all write manifest v12 (spike S2).
SUPPORTED_SCHEMA_VERSION = 12

# Lineage nodes read from manifest["nodes"]. Tests are read only as evidence of testing.
NODE_TYPES = ("model", "seed", "snapshot")
# Every resource type that can become a RawNode, so other prefixes are "not indexed".
INDEXED_PREFIXES = ("model.", "seed.", "snapshot.", "source.", "exposure.")

_SCHEMA_URL = re.compile(r"/manifest/v(\d+)\.json$")

_DOMAIN_SKIP_FOLDERS = frozenset(
    {
        "models",
        "staging",
        "intermediate",
        "marts",
        "raw",
        "sources",
        "reports",
        "analytics",
        "core",
        "base",
        "prep",
        "transform",
    }
)

_TIMESTAMP_INDICATORS = (
    "_at", "_date", "_time", "_ts", "timestamp",
    "created", "updated", "modified", "deleted",
    "_dts", "_datetime",
)  # fmt: skip

_METRIC_INDICATORS = (
    "amount", "total", "sum", "count", "avg", "average",
    "quantity", "qty", "price", "cost", "revenue", "profit",
    "_amt", "_cnt", "_sum",
)  # fmt: skip


def schema_version_number(url: Any) -> int | None:
    """The N in ``https://schemas.getdbt.com/dbt/manifest/vN.json``, or None."""
    if not isinstance(url, str):
        return None
    match = _SCHEMA_URL.search(url)
    return int(match.group(1)) if match else None


def classify_schema(version: int | None) -> SchemaSupport:
    if version is None:
        return SchemaSupport.UNVERSIONED
    if version == SUPPORTED_SCHEMA_VERSION:
        return SchemaSupport.SUPPORTED
    return SchemaSupport.OLDER if version < SUPPORTED_SCHEMA_VERSION else SchemaSupport.NEWER


def structural_problems(manifest: Any, support: SchemaSupport) -> list[str]:
    """Check that a manifest has the shape the parser relies on.

    Every version gets the basic shape checks. A newer or unversioned manifest also gets
    the checks from spike S2: models are present, every node has ``depends_on``, and
    ``depends_on`` agrees with ``parent_map``. Without them, a future manifest that renamed
    ``depends_on`` would parse into a graph with no edges and no errors.
    """
    if not isinstance(manifest, dict):
        return ["the manifest is not a JSON object"]

    problems = [
        f"'{key}' is missing or not an object"
        for key in ("metadata", "nodes")
        if not isinstance(manifest.get(key), dict)
    ]
    problems += [
        f"'{key}' is not an object"
        for key in ("sources", "exposures", "parent_map")
        if key in manifest and not isinstance(manifest[key], dict)
    ]
    if problems or support in (SchemaSupport.SUPPORTED, SchemaSupport.OLDER):
        return problems

    nodes = manifest["nodes"]
    if not any(_dict(n).get("resource_type") == "model" for n in nodes.values()):
        problems.append("no models found")
    without_deps = sum(
        1 for n in nodes.values() if not isinstance(_dict(n).get("depends_on"), dict)
    )
    if without_deps:
        problems.append(f"{without_deps} nodes have no 'depends_on'")
    parent_map = manifest.get("parent_map")
    if not isinstance(parent_map, dict):
        problems.append("'parent_map' is missing")
    elif not without_deps:
        disagree = sum(
            1
            for uid, node in nodes.items()
            if uid in parent_map and set(_deps(node)) != set(_str_list(parent_map[uid]))
        )
        if disagree:
            problems.append(f"'depends_on' disagrees with 'parent_map' for {disagree} nodes")
    return problems


@dataclass
class TestIndex:
    """Tests per node and per column, taken from the manifest's ``test.*`` nodes."""

    node_tests: dict[str, list[str]] = field(default_factory=dict)
    column_tests: dict[tuple[str, str], list[str]] = field(default_factory=dict)

    __test__ = False  # not a pytest test class

    def for_node(self, node_id: str) -> list[str]:
        return self.node_tests.get(node_id, [])

    def for_column(self, node_id: str, column: str) -> list[str]:
        return self.column_tests.get((node_id, column.lower()), [])


def index_tests(nodes: dict[str, Any]) -> TestIndex:
    """Attribute each test node to the nodes and columns it tests.

    A test's kind is ``test_metadata.name`` (``unique``, ``not_null``, ``relationships``,
    ...) or ``singular``. Its subject is ``attached_node`` when set. Generic tests on
    sources have no ``attached_node``, so their subject is the one source they depend on.
    Singular tests are attributed to every node they depend on.
    """
    node_tests: defaultdict[str, list[str]] = defaultdict(list)
    column_tests: defaultdict[tuple[str, str], list[str]] = defaultdict(list)

    for node in nodes.values():
        if not isinstance(node, dict) or node.get("resource_type") != "test":
            continue
        test_metadata = _dict(node.get("test_metadata"))
        kind = _str(test_metadata.get("name")) or "singular"

        attached = _str(node.get("attached_node"))
        if attached:
            subjects = [attached]
        else:
            deps = [d for d in _deps(node) if not d.startswith("test.")]
            sources = [d for d in deps if d.startswith("source.")]
            subjects = sources if test_metadata and len(sources) == 1 else deps

        column = _str(node.get("column_name"))
        for subject in subjects:
            node_tests[subject].append(kind)
            if column and len(subjects) == 1:
                column_tests[(subject, column.lower())].append(kind)

    return TestIndex(dict(node_tests), dict(column_tests))


class DbtParser(MetadataParser):
    """Parser for dbt manifest.json and catalog.json files."""

    @property
    def source_type(self) -> str:
        return "dbt"

    def validate_config(self, config: dict[str, Any]) -> list[str]:
        if "manifest_path" not in config:
            return ["manifest_path is required"]
        try:
            return DbtParserConfig.from_dict(config).validate()
        except (TypeError, ValueError) as e:
            return [f"Invalid configuration: {e}"]

    def parse(self, config: dict[str, Any] | DbtParserConfig) -> ParseResult:
        """Parse dbt artifacts. ERROR-level errors mean the result must not be trusted."""
        result = ParseResult(source_type=self.source_type)

        if isinstance(config, DbtParserConfig):
            parser_config = config
        else:
            try:
                parser_config = DbtParserConfig.from_dict(config)
            except (KeyError, TypeError, ValueError) as e:
                result.add_error(f"Invalid configuration: {e}", ParseErrorSeverity.ERROR)
                return result

        for problem in parser_config.validate():
            result.add_error(problem, ParseErrorSeverity.ERROR)
        if result.has_errors:
            return result

        manifest_path = parser_config.manifest_path
        manifest, result.manifest_sha256 = self._load_json(
            manifest_path, result, ParseErrorSeverity.ERROR
        )
        result.manifest_path = str(manifest_path)
        if result.has_errors:
            return result

        metadata = _dict(manifest.get("metadata")) if isinstance(manifest, dict) else {}
        result.schema_version = schema_version_number(metadata.get("dbt_schema_version"))
        result.schema_support = classify_schema(result.schema_version)
        result.dbt_version = _str(metadata.get("dbt_version"))
        result.generated_at = _str(metadata.get("generated_at"))
        result.adapter_type = _str(metadata.get("adapter_type"))

        problems = structural_problems(manifest, result.schema_support)
        for problem in problems:
            result.add_error(
                f"Manifest structure check failed: {problem}",
                ParseErrorSeverity.ERROR,
                location=str(manifest_path),
            )
        if problems:
            return result
        self._warn_on_schema_version(result, manifest_path)

        catalog: dict[str, Any] = {}
        if parser_config.catalog_path:
            loaded, result.catalog_sha256 = self._load_json(
                parser_config.catalog_path, result, ParseErrorSeverity.WARNING
            )
            result.catalog_path = str(parser_config.catalog_path)
            if isinstance(loaded, dict):
                catalog = loaded
            elif loaded is not None:
                result.add_error(
                    "Catalog is not a JSON object; column types and stats are unavailable",
                    location=str(parser_config.catalog_path),
                )

        result.project_name = parser_config.project_name or self._get_project_name(metadata)
        logger.info(
            "Parsing dbt project %s (dbt %s, manifest schema v%s)",
            result.project_name,
            result.dbt_version,
            result.schema_version,
        )

        tests = index_tests(manifest["nodes"])
        self._process_nodes(manifest, _dict(catalog.get("nodes")), parser_config, tests, result)
        self._process_sources(manifest, _dict(catalog.get("sources")), parser_config, tests, result)
        self._process_exposures(manifest, parser_config, result)
        self._build_lineage(result)
        self._extract_domains(result)
        self._extract_tags(result)

        logger.info("Parse complete: %s", result.summary())
        return result

    def _load_json(
        self, path: Path, result: ParseResult, severity: ParseErrorSeverity
    ) -> tuple[Any, str | None]:
        """The decoded file and the sha256 of its bytes; (None, ...) after an error."""
        try:
            data = path.read_bytes()
        except OSError as e:
            result.add_error(f"Cannot read {path}: {e}", severity, location=str(path))
            return None, None
        digest = hashlib.sha256(data).hexdigest()
        try:
            return _json.loads(data), digest
        except ValueError as e:
            result.add_error(f"Invalid JSON in {path}: {e}", severity, location=str(path))
            return None, digest

    def _warn_on_schema_version(self, result: ParseResult, path: Path) -> None:
        version = result.schema_version
        if result.schema_support is SchemaSupport.OLDER:
            message = (
                f"Manifest schema v{version} is older than v{SUPPORTED_SCHEMA_VERSION}, "
                "the tested version (dbt 1.8 and later). It was parsed best-effort."
            )
        elif result.schema_support is SchemaSupport.NEWER:
            message = (
                f"Manifest schema v{version} is newer than v{SUPPORTED_SCHEMA_VERSION}, "
                "the tested version. It passed the structural checks and was parsed best-effort."
            )
        elif result.schema_support is SchemaSupport.UNVERSIONED:
            message = (
                "Manifest has no readable metadata.dbt_schema_version. It passed the "
                "structural checks and was parsed best-effort."
            )
        else:
            return
        result.add_error(message, ParseErrorSeverity.WARNING, location=str(path))

    def _get_project_name(self, metadata: dict[str, Any]) -> str:
        return _str(metadata.get("project_name")) or _str(metadata.get("project_id")) or "default"

    def _process_nodes(
        self,
        manifest: dict[str, Any],
        catalog_nodes: dict[str, Any],
        config: DbtParserConfig,
        tests: TestIndex,
        result: ParseResult,
    ) -> None:
        """Models, seeds and snapshots, with their columns."""
        for node_id, node in manifest["nodes"].items():
            if not isinstance(node, dict) or node.get("resource_type") not in NODE_TYPES:
                continue
            try:
                raw = self._node_to_raw(node_id, node, config, tests)
                columns = self._extract_columns(
                    raw, node, _dict(catalog_nodes.get(node_id)), config, tests
                )
            except Exception as e:  # untrusted input: lose one node, never the index
                result.add_error(
                    f"Error processing {node.get('resource_type')} {node_id}: {e}",
                    location=node_id,
                )
                continue
            result.nodes.append(raw)
            result.columns.extend(columns)

    def _process_sources(
        self,
        manifest: dict[str, Any],
        catalog_sources: dict[str, Any],
        config: DbtParserConfig,
        tests: TestIndex,
        result: ParseResult,
    ) -> None:
        for source_id, source in _dict(manifest.get("sources")).items():
            try:
                source = _require_dict(source)
                meta = _meta(source)
                test_kinds = tests.for_node(source_id)
                raw = RawNode(
                    unique_id=source_id,
                    resource_type="source",
                    name=_require_str(source, "name"),
                    package_name=_str(source.get("package_name")),
                    database_name=_str(source.get("database")),
                    schema_name=_str(source.get("schema")),
                    relation_name=_str(source.get("relation_name")),
                    layer="bronze",  # sources are always bronze
                    domain_name=self._infer_domain(source, meta),
                    description=_str(source.get("description")),
                    owner=_owner(meta),
                    meta=meta,
                    tags=_str_list(source.get("tags")),
                    config=_dict(source.get("config")),
                    has_tests=bool(test_kinds),
                    test_count=len(test_kinds),
                    test_types=sorted(set(test_kinds)),
                    original_file_path=_str(source.get("original_file_path")),
                    patch_path=_patch_path(source.get("patch_path")),
                    properties={
                        "source_name": _str(source.get("source_name")),
                        "identifier": _str(source.get("identifier")),
                        "loader": _str(source.get("loader")),
                    },
                )
                columns = self._extract_columns(
                    raw, source, _dict(catalog_sources.get(source_id)), config, tests
                )
            except Exception as e:  # untrusted input: lose one source, never the index
                result.add_error(f"Error processing source {source_id}: {e}", location=source_id)
                continue
            result.nodes.append(raw)
            result.columns.extend(columns)

    def _process_exposures(
        self, manifest: dict[str, Any], config: DbtParserConfig, result: ParseResult
    ) -> None:
        for exposure_id, exposure in _dict(manifest.get("exposures")).items():
            try:
                exposure = _require_dict(exposure)
                meta = _meta(exposure)
                owner = _dict(exposure.get("owner"))
                raw = RawNode(
                    unique_id=exposure_id,
                    resource_type="exposure",
                    name=_require_str(exposure, "name"),
                    package_name=_str(exposure.get("package_name")),
                    domain_name=self._infer_domain(exposure, meta),
                    description=_str(exposure.get("description")),
                    depends_on=_deps(exposure),
                    owner=_str(owner.get("name")) or _str(owner.get("email")),
                    meta=meta,
                    tags=_str_list(exposure.get("tags")),
                    config=_dict(exposure.get("config")),
                    original_file_path=_str(exposure.get("original_file_path")),
                    properties={
                        "type": _str(exposure.get("type")),
                        "maturity": _str(exposure.get("maturity")),
                        "url": _str(exposure.get("url")),
                        "label": _str(exposure.get("label")),
                        "owner_email": _str(owner.get("email")),
                    },
                )
            except Exception as e:  # untrusted input: lose one exposure, never the index
                result.add_error(
                    f"Error processing exposure {exposure_id}: {e}", location=exposure_id
                )
                continue
            result.nodes.append(raw)

    def _node_to_raw(
        self, node_id: str, node: dict[str, Any], config: DbtParserConfig, tests: TestIndex
    ) -> RawNode:
        node_config = _dict(node.get("config"))
        meta = _meta(node)
        resource_type = node["resource_type"]
        test_kinds = tests.for_node(node_id)
        return RawNode(
            unique_id=node_id,
            resource_type=resource_type,
            name=_require_str(node, "name"),
            package_name=_str(node.get("package_name")),
            database_name=_str(node.get("database")),
            schema_name=_str(node.get("schema")),
            relation_name=_str(node.get("relation_name")),
            alias=_str(node.get("alias")),
            version=_version(node.get("version")),
            # Seeds are always bronze
            layer="bronze" if resource_type == "seed" else self._infer_layer(node, meta, config),
            domain_name=self._infer_domain(node, meta),
            description=_str(node.get("description")),
            materialization=_str(node_config.get("materialized")),
            depends_on=_deps(node),
            owner=_owner(meta),
            group=_str(node.get("group")) or _str(node_config.get("group")),
            meta=meta,
            tags=_str_list(node.get("tags")),
            config=node_config,
            has_tests=bool(test_kinds),
            test_count=len(test_kinds),
            test_types=sorted(set(test_kinds)),
            original_file_path=_str(node.get("original_file_path")),
            patch_path=_patch_path(node.get("patch_path")),
        )

    def _infer_layer(
        self, node: dict[str, Any], meta: dict[str, Any], config: DbtParserConfig
    ) -> str | None:
        """Architecture layer: meta.layer, then layer tags, then name and path patterns."""
        explicit = _str(meta.get("layer"))
        if explicit:
            return explicit.lower()

        for tag in _str_list(node.get("tags")):
            layer = config.layer_tags.get(tag.lower())
            if layer:
                return layer

        name = _str(node.get("name")) or ""
        path = (_str(node.get("original_file_path")) or "").replace("\\", "/")
        for pattern in config.layer_patterns:
            if pattern.matches_name(name) or pattern.matches_path(path):
                return pattern.layer
        return None

    def _infer_domain(self, node: dict[str, Any], meta: dict[str, Any]) -> str | None:
        """Business domain: meta.domain, then a ``domain:`` tag, then the folder path."""
        explicit = _str(meta.get("domain"))
        if explicit:
            return explicit.lower()

        for tag in _str_list(node.get("tags")):
            tag_lower = tag.lower()
            if tag_lower.startswith("domain:"):
                return tag_lower.replace("domain:", "")

        path = _str(node.get("original_file_path"))
        return self._domain_from_path(path) if path else None

    def _domain_from_path(self, path: str) -> str | None:
        # Backslashes too: dbt on Windows can write them
        parts = path.replace("\\", "/").split("/")
        for part in reversed(parts[:-1]):  # the last part is the file name
            part_lower = part.lower()
            if part_lower in _DOMAIN_SKIP_FOLDERS or part_lower.startswith("_"):
                continue
            if part_lower.isalpha() or "_" in part_lower:
                return part_lower
        return None

    def _extract_columns(
        self,
        raw: RawNode,
        node: dict[str, Any],
        catalog_entry: dict[str, Any],
        config: DbtParserConfig,
        tests: TestIndex,
    ) -> list[RawColumn]:
        """Columns from the manifest (docs, meta, declared type), enriched by the catalog."""
        columns_data: dict[str, dict[str, Any]] = {}

        for col_name, col_info in _dict(node.get("columns")).items():
            info = _dict(col_info)
            columns_data[col_name.lower()] = {
                "name": col_name,
                "description": _str(info.get("description")),
                "meta": _dict(info.get("meta")),
                "tags": _str_list(info.get("tags")),
                "data_type": _str(info.get("data_type")),
            }

        for col_name, col_info in _dict(catalog_entry.get("columns")).items():
            info = _dict(col_info)
            data = columns_data.setdefault(col_name.lower(), {"name": col_name})
            data["data_type"] = _str(info.get("type")) or data.get("data_type")
            index = info.get("index")
            data["index"] = (
                index if isinstance(index, int) and not isinstance(index, bool) else None
            )
            data["stats"] = self._extract_column_stats(info)

        columns: list[RawColumn] = []
        for ordinal, (key, data) in enumerate(columns_data.items(), start=1):
            col_name = data["name"]
            pii_type, detected_by = (
                self._detect_pii(col_name, data, config)
                if config.pii_detection_enabled
                else (None, None)
            )
            test_kinds = tests.for_column(raw.unique_id, key)
            columns.append(
                RawColumn(
                    node_id=raw.unique_id,
                    name=col_name,
                    data_type=data.get("data_type"),
                    ordinal_position=data.get("index") or ordinal,
                    semantic_type=self._infer_semantic_type(col_name, data, pii_type),
                    pii_type=pii_type,
                    pii_detected_by=detected_by,
                    description=data.get("description"),
                    meta=data.get("meta", {}),
                    tags=data.get("tags", []),
                    stats=data.get("stats", {}),
                    has_tests=bool(test_kinds),
                    test_count=len(test_kinds),
                    test_types=sorted(set(test_kinds)),
                )
            )
        return columns

    def _extract_column_stats(self, col_info: dict[str, Any]) -> dict[str, Any]:
        return {
            name: stat["value"]
            for name, stat in _dict(col_info.get("stats")).items()
            if isinstance(stat, dict) and "value" in stat
        }

    def _detect_pii(
        self, col_name: str, col_data: dict[str, Any], config: DbtParserConfig
    ) -> tuple[str | None, str | None]:
        """PII type from meta, then tags, then name patterns."""
        meta = col_data.get("meta", {})

        # pii: true (with an optional pii_type) or pii: "email"
        pii_value = meta.get("pii")
        if pii_value is True:
            return _str(meta.get("pii_type")) or "unknown", "tag"
        if isinstance(pii_value, str) and pii_value:
            return pii_value, "tag"

        if meta.get("sensitive"):
            return "sensitive", "tag"

        for tag in col_data.get("tags", []):
            tag_lower = tag.lower()
            if tag_lower == "pii":
                return "unknown", "tag"
            if tag_lower.startswith("pii:"):
                return tag_lower.replace("pii:", ""), "tag"

        for pattern in config.pii_patterns:
            if pattern.matches(col_name):
                return pattern.pii_type, "pattern"

        return None, None

    def _infer_semantic_type(
        self, col_name: str, col_data: dict[str, Any], pii_type: str | None
    ) -> str | None:
        if pii_type:
            return "pii"

        name_lower = col_name.lower()
        explicit = _str(col_data.get("meta", {}).get("semantic_type"))
        if explicit:
            return explicit

        # Keys
        if name_lower.endswith("_id") or name_lower.endswith("_key"):
            if "_sk" in name_lower or "surrogate" in name_lower:
                return "surrogate_key"
            if "natural" in name_lower or "business" in name_lower or "_bk" in name_lower:
                return "business_key"
            if name_lower.endswith("_fk"):
                return "foreign_key"
            # A generic ID is likely a foreign key unless it is the primary key
            if name_lower not in ("id", "pk", "primary_key"):
                return "foreign_key"

        if name_lower in ("id", "pk", "primary_key") or name_lower.endswith("_pk"):
            return "surrogate_key"

        if any(ind in name_lower for ind in _TIMESTAMP_INDICATORS):
            return "timestamp"

        if any(ind in name_lower for ind in _METRIC_INDICATORS):
            return "metric"

        return None

    def _build_lineage(self, result: ParseResult) -> None:
        """Node edges from depends_on. A dependency that does not resolve is recorded."""
        known = {node.unique_id for node in result.nodes}
        for node in result.nodes:
            for dep_id in node.depends_on:
                if dep_id in known:
                    result.edges.append(
                        RawEdge(
                            upstream_id=dep_id,
                            downstream_id=node.unique_id,
                            transformation=_transformation(dep_id),
                        )
                    )
                else:
                    result.unresolved.append(
                        RawUnresolved(
                            node_id=node.unique_id,
                            reference=dep_id,
                            reason=_unresolved_reason(dep_id),
                        )
                    )
        logger.debug(
            "Built %d lineage edges, %d unresolved", len(result.edges), len(result.unresolved)
        )

    def _extract_domains(self, result: ParseResult) -> None:
        names = {node.domain_name for node in result.nodes if node.domain_name}
        result.domains.extend(RawDomain(name=name) for name in sorted(names))

    def _extract_tags(self, result: ParseResult) -> None:
        names: set[str] = set()
        for node in result.nodes:
            names.update(node.tags)
        for column in result.columns:
            names.update(column.tags)

        for name in sorted(names):
            category = None
            if name.startswith("pii"):
                category = "sensitivity"
            elif name.startswith("domain:"):
                category = "domain"
            result.tags.append(RawTag(name=name, category=category))


def _transformation(dep_id: str) -> str:
    if dep_id.startswith("source."):
        return "source"
    if dep_id.startswith(("model.", "seed.")):
        return "ref"
    return "dependency"


def _unresolved_reason(dep_id: str) -> str:
    if dep_id.startswith(INDEXED_PREFIXES):
        return "not found among the parsed nodes"
    return f"{dep_id.split('.', 1)[0]} nodes are not indexed"


# Readers for untrusted manifest values: wrong types read as absent.


def _str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _str_list(value: Any) -> list[str]:
    return [v for v in value if isinstance(v, str)] if isinstance(value, list) else []


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _require_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"expected an object, got {type(value).__name__}")
    return value


def _require_str(node: dict[str, Any], key: str) -> str:
    value = node.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"'{key}' is missing or not a string")
    return value


def _deps(node: Any) -> list[str]:
    """depends_on.nodes, in order, without duplicates."""
    return list(dict.fromkeys(_str_list(_dict(_dict(node).get("depends_on")).get("nodes"))))


def _meta(node: dict[str, Any]) -> dict[str, Any]:
    # dbt mirrors meta into config.meta; Fusion keeps both too (spike S2)
    return _dict(_dict(node.get("config")).get("meta")) or _dict(node.get("meta"))


def _owner(meta: dict[str, Any]) -> str | None:
    return _str(meta.get("owner"))


def _patch_path(value: Any) -> str | None:
    """``project://models/x.yml`` becomes ``models/x.yml``."""
    path = _str(value)
    if path and "://" in path:
        return path.split("://", 1)[1]
    return path


def _version(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value)
