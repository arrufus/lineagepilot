"""Query a dbt manifest to link Airflow dbt tasks to dbt models.

Ported from data-capsule ``backend/src/parsers/dbt_manifest_resolver.py`` (commit
5e57fcc). Changes:

- JSON falls back to the standard library when orjson is absent.
- ``model_to_capsule_urn`` is gone with the server's URNs: a model's ID is its
  ``unique_id`` (design section 5.1).

The selector semantics are unchanged and simplified: a path selector matches a substring
of the ``unique_id``. Phase 5 (tasks 5.1-5.4) replaces them with the rules spike S3
found, including selector semantics that follow the manifest's dbt version.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lineagepilot_core.parsers import _json

logger = logging.getLogger(__name__)


@dataclass
class DbtModelReference:
    """A dbt model from the manifest."""

    unique_id: str  # model.my_project.customers
    name: str  # customers
    schema: str  # analytics
    database: str | None = None
    package_name: str | None = None
    materialization: str = "table"  # table, view, incremental, ephemeral
    tags: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)

    @property
    def fqn(self) -> str:
        """``database.schema.name``, leaving out the parts that are absent."""
        return ".".join(part for part in (self.database, self.schema, self.name) if part)


class DbtManifestResolver:
    """Resolves dbt selectors against a manifest."""

    def __init__(self, manifest_path: str | None = None):
        self.manifest_path = manifest_path
        self._manifest_data: dict[str, Any] | None = None
        self._models_cache: dict[str, DbtModelReference] = {}

    def load_manifest(self, manifest_path: str | None = None) -> bool:
        """Load a manifest. Returns True on success."""
        path = manifest_path or self.manifest_path
        if not path:
            logger.warning("No manifest path provided")
            return False

        manifest_file = Path(path)
        if not manifest_file.exists():
            logger.warning("Manifest file not found: %s", path)
            return False

        try:
            data = _json.load_file(manifest_file)
        except (OSError, ValueError) as e:
            logger.error("Cannot read manifest %s: %s", path, e)
            return False
        if not isinstance(data, dict):
            logger.error("Manifest %s is not a JSON object", path)
            return False

        self._manifest_data = data
        self._build_models_cache()
        logger.info("Loaded dbt manifest with %d models", len(self._models_cache))
        return True

    def _build_models_cache(self) -> None:
        self._models_cache = {}
        nodes = (self._manifest_data or {}).get("nodes")
        for unique_id, node in (nodes if isinstance(nodes, dict) else {}).items():
            if not unique_id.startswith("model.") or not isinstance(node, dict):
                continue
            config = node.get("config") if isinstance(node.get("config"), dict) else {}
            depends_on = node.get("depends_on") if isinstance(node.get("depends_on"), dict) else {}
            self._models_cache[unique_id] = DbtModelReference(
                unique_id=unique_id,
                name=_str(node.get("name")) or "",
                schema=_str(node.get("schema")) or "",
                database=_str(node.get("database")),
                package_name=_str(node.get("package_name")),
                materialization=_str(config.get("materialized")) or "table",
                tags=_str_list(node.get("tags")),
                depends_on=_str_list(depends_on.get("nodes")),
            )

    def resolve_selector(self, selector: str) -> list[DbtModelReference]:
        """Resolve a dbt selector to models.

        Supports ``tag:name``, a model name, ``path/to/models/*`` and
        ``package.model_name``.
        """
        if not self._models_cache:
            logger.warning("No manifest loaded")
            return []

        if selector.startswith("tag:"):
            return self._filter_by_tag(selector[4:])
        if "." in selector and not selector.endswith("*"):
            return self._filter_by_package_model(selector)
        if "*" in selector:
            return self._filter_by_path(selector)
        return self._filter_by_name(selector)

    def _filter_by_tag(self, tag_name: str) -> list[DbtModelReference]:
        return [m for m in self._models_cache.values() if tag_name in m.tags]

    def _filter_by_name(self, model_name: str) -> list[DbtModelReference]:
        return [m for m in self._models_cache.values() if m.name == model_name]

    def _filter_by_package_model(self, selector: str) -> list[DbtModelReference]:
        package_name, model_name = selector.split(".", 1)
        return [
            m
            for m in self._models_cache.values()
            if m.package_name == package_name and m.name == model_name
        ]

    def _filter_by_path(self, path_pattern: str) -> list[DbtModelReference]:
        # Simplified, as before: the path prefix is matched against the unique_id
        path_prefix = path_pattern.rstrip("/*").lower()
        return [m for uid, m in self._models_cache.items() if path_prefix in uid.lower()]

    def get_model_by_name(self, model_name: str) -> DbtModelReference | None:
        """The model with this name, or None. With several, the first is returned."""
        matches = self._filter_by_name(model_name)
        if not matches:
            return None
        if len(matches) > 1:
            logger.warning("Multiple models found for name '%s', returning first", model_name)
        return matches[0]

    def extract_from_operator_config(self, operator_config: dict[str, Any]) -> dict[str, Any]:
        """Extract the dbt selector and models from an Airflow operator's config.

        Returns a dict with ``selector``, ``models`` and ``operation_type``.
        """
        result: dict[str, Any] = {
            "selector": None,
            "models": [],
            "operation_type": "merge",  # the default for dbt
        }

        if "select" in operator_config:
            result["selector"] = operator_config["select"]
        elif "models" in operator_config:
            models_value = operator_config["models"]
            if isinstance(models_value, str):
                result["selector"] = models_value
            elif isinstance(models_value, list):
                result["models"] = models_value

        if isinstance(result["selector"], str) and self._manifest_data:
            models = self.resolve_selector(result["selector"])
            result["models"] = [model.name for model in models]

        return result


def _str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _str_list(value: Any) -> list[str]:
    return [v for v in value if isinstance(v, str)] if isinstance(value, list) else []
