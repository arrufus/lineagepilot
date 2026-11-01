"""dbt metadata parser for manifest.json and catalog.json."""

import logging
from pathlib import Path
from typing import Any, Optional

import orjson

from src.parsers.base import (
    MetadataParser,
    ParseError,
    ParseErrorSeverity,
    ParseResult,
    RawCapsule,
    RawColumn,
    RawColumnEdge,
    RawDomain,
    RawEdge,
    RawTag,
)
from src.parsers.dbt_config import DbtParserConfig, PIIPattern, LayerPattern

logger = logging.getLogger(__name__)


class DbtParser(MetadataParser):
    """Parser for dbt manifest.json and catalog.json files."""

    @property
    def source_type(self) -> str:
        return "dbt"

    def validate_config(self, config: dict[str, Any]) -> list[str]:
        """Validate dbt parser configuration."""
        errors: list[str] = []

        if "manifest_path" not in config:
            errors.append("manifest_path is required")
            return errors

        try:
            parser_config = DbtParserConfig.from_dict(config)
            errors.extend(parser_config.validate())
        except Exception as e:
            errors.append(f"Invalid configuration: {e}")

        return errors

    async def parse(self, config: dict[str, Any]) -> ParseResult:
        """Parse dbt artifacts into normalized models."""
        result = ParseResult(source_type=self.source_type)

        # Create typed configuration
        try:
            parser_config = DbtParserConfig.from_dict(config)
        except Exception as e:
            result.add_error(
                f"Invalid configuration: {e}",
                severity=ParseErrorSeverity.ERROR,
            )
            return result

        # Validate configuration
        validation_errors = parser_config.validate()
        if validation_errors:
            for error in validation_errors:
                result.add_error(error, severity=ParseErrorSeverity.ERROR)
            return result

        # Load manifest
        manifest = await self._load_json(parser_config.manifest_path)
        if manifest is None:
            result.add_error(
                f"Failed to load manifest: {parser_config.manifest_path}",
                severity=ParseErrorSeverity.ERROR,
            )
            return result

        # Load catalog (optional)
        catalog: Optional[dict[str, Any]] = None
        if parser_config.catalog_path and parser_config.catalog_path.exists():
            catalog = await self._load_json(parser_config.catalog_path)
            if catalog is None:
                result.add_error(
                    f"Failed to load catalog: {parser_config.catalog_path}",
                    severity=ParseErrorSeverity.WARNING,
                )

        # Extract project info
        project_name = parser_config.project_name or self._get_project_name(manifest)
        result.source_name = project_name
        result.source_version = manifest.get("metadata", {}).get("dbt_version")

        logger.info(
            f"Parsing dbt project: {project_name}, "
            f"dbt version: {result.source_version}"
        )

        # Process nodes
        self._process_models(manifest, catalog, project_name, parser_config, result)

        if parser_config.include_sources:
            self._process_sources(manifest, catalog, project_name, parser_config, result)

        if parser_config.include_seeds:
            self._process_seeds(manifest, catalog, project_name, parser_config, result)

        if parser_config.include_snapshots:
            self._process_snapshots(manifest, catalog, project_name, parser_config, result)

        # Build lineage edges
        self._build_lineage(manifest, result)

        # Extract domains from capsules
        self._extract_domains(result)

        # Extract unique tags
        self._extract_tags(result)

        logger.info(
            f"Parse complete: {result.summary()}"
        )

        return result

    async def _load_json(self, path: Path) -> Optional[dict[str, Any]]:
        """Load and parse a JSON file."""
        try:
            with open(path, "rb") as f:
                return orjson.loads(f.read())
        except orjson.JSONDecodeError as e:
            logger.error(f"JSON decode error in {path}: {e}")
            return None
        except OSError as e:
            logger.error(f"Error reading {path}: {e}")
            return None

    def _get_project_name(self, manifest: dict[str, Any]) -> str:
        """Extract project name from manifest."""
        metadata = manifest.get("metadata", {})
        return metadata.get("project_name") or metadata.get("project_id") or "default"

    def _build_urn(
        self,
        config: DbtParserConfig,
        node_type: str,
        namespace: str,
        name: str,
    ) -> str:
        """Build a URN for a dbt node."""
        return f"{config.urn_prefix}:{node_type}:{namespace}:{name}"

    def _process_models(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]],
        project_name: str,
        config: DbtParserConfig,
        result: ParseResult,
    ) -> None:
        """Extract models from manifest."""
        nodes = manifest.get("nodes", {})
        model_count = 0

        for node_id, node in nodes.items():
            if node.get("resource_type") != "model":
                continue

            try:
                capsule = self._node_to_capsule(
                    node=node,
                    node_id=node_id,
                    node_type="model",
                    project_name=project_name,
                    config=config,
                )
                result.capsules.append(capsule)
                model_count += 1

                # Extract columns
                self._extract_columns(
                    capsule_urn=capsule.urn,
                    node=node,
                    node_id=node_id,
                    catalog=catalog,
                    config=config,
                    result=result,
                )

            except Exception as e:
                result.add_error(
                    f"Error processing model {node_id}: {e}",
                    severity=ParseErrorSeverity.WARNING,
                    location=node_id,
                )

        logger.debug(f"Processed {model_count} models")

    def _process_sources(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]],
        project_name: str,
        config: DbtParserConfig,
        result: ParseResult,
    ) -> None:
        """Extract sources from manifest."""
        sources = manifest.get("sources", {})
        source_count = 0

        for source_id, source in sources.items():
            try:
                # Build namespace from source name
                source_name = source.get("source_name", "default")
                namespace = f"{project_name}.{source_name}"

                urn = self._build_urn(config, "source", namespace, source["name"])

                capsule = RawCapsule(
                    urn=urn,
                    name=source["name"],
                    capsule_type="source",
                    unique_id=source_id,
                    layer="bronze",  # Sources are always bronze
                    database_name=source.get("database"),
                    schema_name=source.get("schema"),
                    description=source.get("description"),
                    meta={
                        "unique_id": source_id,
                        "source_name": source_name,
                        "loader": source.get("loader"),
                        "freshness": source.get("freshness"),
                        "loaded_at_field": source.get("loaded_at_field"),
                    },
                    tags=source.get("tags", []),
                    package_name=source.get("package_name"),
                )

                result.capsules.append(capsule)
                source_count += 1

                # Extract columns
                self._extract_columns(
                    capsule_urn=urn,
                    node=source,
                    node_id=source_id,
                    catalog=catalog,
                    config=config,
                    result=result,
                    is_source=True,
                )

            except Exception as e:
                result.add_error(
                    f"Error processing source {source_id}: {e}",
                    severity=ParseErrorSeverity.WARNING,
                    location=source_id,
                )

        logger.debug(f"Processed {source_count} sources")

    def _process_seeds(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]],
        project_name: str,
        config: DbtParserConfig,
        result: ParseResult,
    ) -> None:
        """Extract seeds from manifest."""
        nodes = manifest.get("nodes", {})
        seed_count = 0

        for node_id, node in nodes.items():
            if node.get("resource_type") != "seed":
                continue

            try:
                capsule = self._node_to_capsule(
                    node=node,
                    node_id=node_id,
                    node_type="seed",
                    project_name=project_name,
                    config=config,
                )
                # Seeds are always bronze layer
                capsule.layer = "bronze"
                result.capsules.append(capsule)
                seed_count += 1

                # Extract columns
                self._extract_columns(
                    capsule_urn=capsule.urn,
                    node=node,
                    node_id=node_id,
                    catalog=catalog,
                    config=config,
                    result=result,
                )

            except Exception as e:
                result.add_error(
                    f"Error processing seed {node_id}: {e}",
                    severity=ParseErrorSeverity.WARNING,
                    location=node_id,
                )

        logger.debug(f"Processed {seed_count} seeds")

    def _process_snapshots(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]],
        project_name: str,
        config: DbtParserConfig,
        result: ParseResult,
    ) -> None:
        """Extract snapshots from manifest."""
        nodes = manifest.get("nodes", {})
        snapshot_count = 0

        for node_id, node in nodes.items():
            if node.get("resource_type") != "snapshot":
                continue

            try:
                capsule = self._node_to_capsule(
                    node=node,
                    node_id=node_id,
                    node_type="snapshot",
                    project_name=project_name,
                    config=config,
                )
                result.capsules.append(capsule)
                snapshot_count += 1

                # Extract columns
                self._extract_columns(
                    capsule_urn=capsule.urn,
                    node=node,
                    node_id=node_id,
                    catalog=catalog,
                    config=config,
                    result=result,
                )

            except Exception as e:
                result.add_error(
                    f"Error processing snapshot {node_id}: {e}",
                    severity=ParseErrorSeverity.WARNING,
                    location=node_id,
                )

        logger.debug(f"Processed {snapshot_count} snapshots")

    def _node_to_capsule(
        self,
        node: dict[str, Any],
        node_id: str,
        node_type: str,
        project_name: str,
        config: DbtParserConfig,
    ) -> RawCapsule:
        """Convert a dbt node to a RawCapsule."""
        schema = node.get("schema", "default")
        namespace = f"{project_name}.{schema}"
        name = node["name"]

        urn = self._build_urn(config, node_type, namespace, name)

        # Infer layer
        layer = self._infer_layer(node, config)

        # Infer domain
        domain = self._infer_domain(node)

        # Get config
        node_config = node.get("config", {})

        # Check for tests
        has_tests = self._has_tests(node_id, node)
        test_count = self._count_tests(node_id, node)

        return RawCapsule(
            urn=urn,
            name=name,
            capsule_type=node_type,
            unique_id=node_id,
            layer=layer,
            domain_name=domain,
            database_name=node.get("database"),
            schema_name=schema,
            description=node.get("description"),
            materialization=node_config.get("materialized"),
            depends_on=node.get("depends_on", {}).get("nodes", []),
            meta={
                "unique_id": node_id,
                "package_name": node.get("package_name"),
                "original_file_path": node.get("original_file_path"),
                "config": node_config,
                "alias": node.get("alias"),
                "checksum": node.get("checksum"),
            },
            tags=node.get("tags", []),
            config=node_config,
            has_tests=has_tests,
            test_count=test_count,
            original_file_path=node.get("original_file_path"),
            package_name=node.get("package_name"),
        )

    def _infer_layer(
        self,
        node: dict[str, Any],
        config: DbtParserConfig,
    ) -> Optional[str]:
        """Infer architecture layer from node properties."""
        name = node.get("name", "")
        path = node.get("original_file_path", "")
        tags = node.get("tags", [])

        # 1. Check explicit layer in meta
        meta = node.get("config", {}).get("meta", {}) or node.get("meta", {})
        if meta.get("layer"):
            return meta["layer"].lower()

        # 2. Check tags for layer indicators
        for tag in tags:
            tag_lower = tag.lower()
            if tag_lower in config.layer_tags:
                return config.layer_tags[tag_lower]

        # 3. Check name and path patterns
        for pattern in config.layer_patterns:
            if pattern.matches_name(name):
                return pattern.layer
            if pattern.matches_path(path):
                return pattern.layer

        return None

    def _infer_domain(self, node: dict[str, Any]) -> Optional[str]:
        """Infer business domain from node properties."""
        # 1. Check explicit domain in meta
        meta = node.get("config", {}).get("meta", {}) or node.get("meta", {})
        if meta.get("domain"):
            return meta["domain"].lower()

        # 2. Check tags for domain
        tags = node.get("tags", [])
        for tag in tags:
            tag_lower = tag.lower()
            if tag_lower.startswith("domain:"):
                return tag_lower.replace("domain:", "")

        # 3. Infer from path
        path = node.get("original_file_path", "")
        if path:
            return self._domain_from_path(path)

        return None

    def _domain_from_path(self, path: str) -> Optional[str]:
        """Extract domain from file path."""
        parts = Path(path).parts

        # Folders to skip when looking for domain
        skip_folders = {
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

        # Look for domain-like folder names
        for part in reversed(parts[:-1]):  # Exclude filename
            part_lower = part.lower()
            if part_lower not in skip_folders and not part_lower.startswith("_"):
                # Validate it looks like a domain name
                if part_lower.isalpha() or "_" in part_lower:
                    return part_lower

        return None

    def _extract_columns(
        self,
        capsule_urn: str,
        node: dict[str, Any],
        node_id: str,
        catalog: Optional[dict[str, Any]],
        config: DbtParserConfig,
        result: ParseResult,
        is_source: bool = False,
    ) -> None:
        """Extract columns from node and catalog."""
        columns_data: dict[str, dict[str, Any]] = {}

        # Get columns from manifest (descriptions, meta, tests)
        manifest_columns = node.get("columns", {})
        for col_name, col_info in manifest_columns.items():
            columns_data[col_name.lower()] = {
                "name": col_name,
                "description": col_info.get("description"),
                "meta": col_info.get("meta", {}),
                "tags": col_info.get("tags", []),
                "data_tests": col_info.get("data_tests", []),
            }

        # Enrich with catalog data (types, stats)
        if catalog:
            catalog_key = "sources" if is_source else "nodes"
            catalog_node = catalog.get(catalog_key, {}).get(node_id)

            if catalog_node:
                for col_name, col_info in catalog_node.get("columns", {}).items():
                    col_key = col_name.lower()
                    if col_key not in columns_data:
                        columns_data[col_key] = {"name": col_name}

                    columns_data[col_key].update({
                        "data_type": col_info.get("type"),
                        "index": col_info.get("index"),
                        "stats": self._extract_column_stats(col_info),
                    })

        # Create RawColumn objects
        for ordinal, (col_key, col_data) in enumerate(columns_data.items(), start=1):
            col_name = col_data.get("name", col_key)
            column_urn = f"{capsule_urn}.{col_name}"

            # Detect PII
            pii_type, detected_by = self._detect_pii(
                col_name, col_data, config
            ) if config.pii_detection_enabled else (None, None)

            # Determine semantic type
            semantic_type = self._infer_semantic_type(col_name, col_data, pii_type)

            # Check for tests
            has_tests = bool(col_data.get("data_tests"))
            test_count = len(col_data.get("data_tests", []))

            column = RawColumn(
                urn=column_urn,
                capsule_urn=capsule_urn,
                name=col_name,
                data_type=col_data.get("data_type"),
                ordinal_position=col_data.get("index", ordinal),
                semantic_type=semantic_type,
                pii_type=pii_type,
                pii_detected_by=detected_by,
                description=col_data.get("description"),
                meta=col_data.get("meta", {}),
                tags=col_data.get("tags", []),
                stats=col_data.get("stats", {}),
                has_tests=has_tests,
                test_count=test_count,
            )

            result.columns.append(column)

    def _extract_column_stats(self, col_info: dict[str, Any]) -> dict[str, Any]:
        """Extract statistics from catalog column info."""
        stats: dict[str, Any] = {}
        raw_stats = col_info.get("stats", {})

        for stat_name, stat_info in raw_stats.items():
            if isinstance(stat_info, dict) and "value" in stat_info:
                stats[stat_name] = stat_info["value"]

        return stats

    def _detect_pii(
        self,
        col_name: str,
        col_data: dict[str, Any],
        config: DbtParserConfig,
    ) -> tuple[Optional[str], Optional[str]]:
        """Detect PII type from column metadata and name patterns."""
        # 1. Check explicit meta tags
        meta = col_data.get("meta", {})

        # Check for pii: true or pii: "email" style
        pii_value = meta.get("pii")
        if pii_value:
            if isinstance(pii_value, bool) and pii_value:
                # pii: true - check for specific type
                pii_type = meta.get("pii_type", "unknown")
                return pii_type, "tag"
            elif isinstance(pii_value, str):
                # pii: "email" style
                return pii_value, "tag"

        # Check for sensitive flag
        if meta.get("sensitive"):
            return "sensitive", "tag"

        # 2. Check column tags
        tags = col_data.get("tags", [])
        for tag in tags:
            tag_lower = tag.lower()
            if tag_lower == "pii":
                return "unknown", "tag"
            if tag_lower.startswith("pii:"):
                return tag_lower.replace("pii:", ""), "tag"

        # 3. Pattern-based detection
        for pattern in config.pii_patterns:
            if pattern.matches(col_name):
                return pattern.pii_type, "pattern"

        return None, None

    def _infer_semantic_type(
        self,
        col_name: str,
        col_data: dict[str, Any],
        pii_type: Optional[str],
    ) -> Optional[str]:
        """Infer semantic type from column characteristics."""
        if pii_type:
            return "pii"

        name_lower = col_name.lower()
        meta = col_data.get("meta", {})

        # Check explicit semantic_type in meta
        if meta.get("semantic_type"):
            return meta["semantic_type"]

        # Key detection
        if name_lower.endswith("_id") or name_lower.endswith("_key"):
            if "_sk" in name_lower or "surrogate" in name_lower:
                return "surrogate_key"
            if "natural" in name_lower or "business" in name_lower or "_bk" in name_lower:
                return "business_key"
            if name_lower.endswith("_fk"):
                return "foreign_key"
            # Generic ID - likely foreign key if not primary
            if name_lower not in ("id", "pk", "primary_key"):
                return "foreign_key"

        # Primary key patterns
        if name_lower in ("id", "pk", "primary_key") or name_lower.endswith("_pk"):
            return "surrogate_key"

        # Timestamp detection
        timestamp_indicators = [
            "_at", "_date", "_time", "_ts", "timestamp",
            "created", "updated", "modified", "deleted",
            "_dts", "_datetime",
        ]
        if any(ind in name_lower for ind in timestamp_indicators):
            return "timestamp"

        # Metric/measure detection
        metric_indicators = [
            "amount", "total", "sum", "count", "avg", "average",
            "quantity", "qty", "price", "cost", "revenue", "profit",
            "_amt", "_cnt", "_sum",
        ]
        if any(ind in name_lower for ind in metric_indicators):
            return "metric"

        return None

    def _has_tests(self, node_id: str, node: dict[str, Any]) -> bool:
        """Check if node has any tests."""
        # Check for data_tests in node
        if node.get("data_tests"):
            return True

        # Check columns for tests
        columns = node.get("columns", {})
        for col_info in columns.values():
            if col_info.get("data_tests"):
                return True

        return False

    def _count_tests(self, node_id: str, node: dict[str, Any]) -> int:
        """Count total tests for a node."""
        count = len(node.get("data_tests", []))

        # Count column-level tests
        columns = node.get("columns", {})
        for col_info in columns.values():
            count += len(col_info.get("data_tests", []))

        return count

    def _build_lineage(
        self,
        manifest: dict[str, Any],
        result: ParseResult,
    ) -> None:
        """Build lineage edges from depends_on relationships."""
        # Create URN lookup from unique_id
        urn_lookup: dict[str, str] = {}
        for capsule in result.capsules:
            urn_lookup[capsule.unique_id] = capsule.urn

        # Create column URN lookup
        column_urn_lookup: dict[str, str] = {}  # unique_id.column_name -> column_urn
        for column in result.columns:
            # Parse the column URN to get the node unique_id
            capsule_urn = column.capsule_urn
            for capsule in result.capsules:
                if capsule.urn == capsule_urn:
                    key = f"{capsule.unique_id}.{column.name.lower()}"
                    column_urn_lookup[key] = column.urn
                    break

        # Process edges from capsules
        for capsule in result.capsules:
            for dep_id in capsule.depends_on:
                source_urn = urn_lookup.get(dep_id)
                if source_urn:
                    # Determine transformation type
                    if dep_id.startswith("source."):
                        transformation = "source"
                    elif dep_id.startswith("model."):
                        transformation = "ref"
                    elif dep_id.startswith("seed."):
                        transformation = "ref"
                    else:
                        transformation = "dependency"

                    edge = RawEdge(
                        source_urn=source_urn,
                        target_urn=capsule.urn,
                        edge_type="flows_to",
                        transformation=transformation,
                    )
                    result.edges.append(edge)

        # Build column-level lineage if available in manifest
        self._build_column_lineage(manifest, result, urn_lookup, column_urn_lookup)

        logger.debug(f"Built {len(result.edges)} lineage edges and {len(result.column_edges)} column edges")

    def _build_column_lineage(
        self,
        manifest: dict[str, Any],
        result: ParseResult,
        urn_lookup: dict[str, str],
        column_urn_lookup: dict[str, str],
    ) -> None:
        """
        Build column-level lineage from dbt manifest.
        
        dbt can include column-level lineage in the manifest under 'catalog_col_lineage'
        or in the node's 'depends_on' columns section (dbt 1.6+).
        """
        nodes = manifest.get("nodes", {})
        
        for node_id, node in nodes.items():
            target_capsule_urn = urn_lookup.get(node_id)
            if not target_capsule_urn:
                continue
            
            # Check for column-level dependencies in manifest (dbt 1.6+ format)
            depends_on = node.get("depends_on", {})
            
            # Some dbt versions include column lineage info
            # Format: depends_on.columns = {"col_name": ["source.project.table.col"]}
            column_depends = depends_on.get("columns", {})
            if column_depends:
                self._process_column_depends(
                    node_id, target_capsule_urn, column_depends,
                    urn_lookup, column_urn_lookup, result
                )
            
            # Check for catalog column lineage (if generated by dbt docs)
            catalog_col_lineage = node.get("catalog_col_lineage", {})
            if catalog_col_lineage:
                self._process_catalog_column_lineage(
                    node_id, target_capsule_urn, catalog_col_lineage,
                    urn_lookup, column_urn_lookup, result
                )
            
            # Also check columns for explicit dependencies
            columns = node.get("columns", {})
            for col_name, col_info in columns.items():
                col_depends = col_info.get("depends_on", [])
                if col_depends:
                    target_col_key = f"{node_id}.{col_name.lower()}"
                    target_col_urn = column_urn_lookup.get(target_col_key)
                    if not target_col_urn:
                        continue
                    
                    for source_ref in col_depends:
                        # source_ref could be "model.project.table.column"
                        source_col_urn = self._resolve_column_ref(
                            source_ref, urn_lookup, column_urn_lookup
                        )
                        if source_col_urn:
                            edge = RawColumnEdge(
                                source_column_urn=source_col_urn,
                                target_column_urn=target_col_urn,
                                transformation_type="passthrough",
                            )
                            result.column_edges.append(edge)

    def _process_column_depends(
        self,
        node_id: str,
        target_capsule_urn: str,
        column_depends: dict[str, list[str]],
        urn_lookup: dict[str, str],
        column_urn_lookup: dict[str, str],
        result: ParseResult,
    ) -> None:
        """Process column-level depends_on information."""
        for col_name, source_refs in column_depends.items():
            target_col_key = f"{node_id}.{col_name.lower()}"
            target_col_urn = column_urn_lookup.get(target_col_key)
            if not target_col_urn:
                continue
            
            for source_ref in source_refs:
                source_col_urn = self._resolve_column_ref(
                    source_ref, urn_lookup, column_urn_lookup
                )
                if source_col_urn:
                    edge = RawColumnEdge(
                        source_column_urn=source_col_urn,
                        target_column_urn=target_col_urn,
                        transformation_type="transform",
                    )
                    result.column_edges.append(edge)

    def _process_catalog_column_lineage(
        self,
        node_id: str,
        target_capsule_urn: str,
        catalog_col_lineage: dict[str, Any],
        urn_lookup: dict[str, str],
        column_urn_lookup: dict[str, str],
        result: ParseResult,
    ) -> None:
        """Process catalog-style column lineage."""
        # catalog_col_lineage format varies but typically:
        # {"columns": {"target_col": {"sources": [{"node": "...", "column": "..."}]}}}
        columns_lineage = catalog_col_lineage.get("columns", catalog_col_lineage)
        
        for target_col, lineage_info in columns_lineage.items():
            target_col_key = f"{node_id}.{target_col.lower()}"
            target_col_urn = column_urn_lookup.get(target_col_key)
            if not target_col_urn:
                continue
            
            sources = lineage_info if isinstance(lineage_info, list) else lineage_info.get("sources", [])
            
            for source in sources:
                if isinstance(source, dict):
                    source_node = source.get("node") or source.get("unique_id")
                    source_col = source.get("column") or source.get("name")
                    transformation = source.get("transformation")
                else:
                    # source is a string reference
                    parts = str(source).rsplit(".", 1)
                    if len(parts) == 2:
                        source_node, source_col = parts
                    else:
                        continue
                
                if source_node and source_col:
                    source_col_key = f"{source_node}.{source_col.lower()}"
                    source_col_urn = column_urn_lookup.get(source_col_key)
                    
                    if source_col_urn:
                        edge = RawColumnEdge(
                            source_column_urn=source_col_urn,
                            target_column_urn=target_col_urn,
                            transformation_type=transformation or "transform",
                        )
                        result.column_edges.append(edge)

    def _resolve_column_ref(
        self,
        ref: str,
        urn_lookup: dict[str, str],
        column_urn_lookup: dict[str, str],
    ) -> Optional[str]:
        """Resolve a column reference string to a column URN."""
        # ref format: "model.project.table.column" or "source.project.source.table.column"
        parts = ref.rsplit(".", 1)
        if len(parts) != 2:
            return None
        
        node_id, col_name = parts
        col_key = f"{node_id}.{col_name.lower()}"
        return column_urn_lookup.get(col_key)

    def _extract_domains(self, result: ParseResult) -> None:
        """Extract unique domains from capsules."""
        domain_names: set[str] = set()

        for capsule in result.capsules:
            if capsule.domain_name:
                domain_names.add(capsule.domain_name)

        for name in sorted(domain_names):
            result.domains.append(RawDomain(name=name))

        logger.debug(f"Extracted {len(result.domains)} domains")

    def _extract_tags(self, result: ParseResult) -> None:
        """Extract unique tags from capsules and columns."""
        tag_names: set[str] = set()

        for capsule in result.capsules:
            tag_names.update(capsule.tags)

        for column in result.columns:
            tag_names.update(column.tags)

        for name in sorted(tag_names):
            # Categorize tags
            category = None
            if name.startswith("pii"):
                category = "sensitivity"
            elif name.startswith("domain:"):
                category = "domain"

            result.tags.append(RawTag(name=name, category=category))

        logger.debug(f"Extracted {len(result.tags)} unique tags")
