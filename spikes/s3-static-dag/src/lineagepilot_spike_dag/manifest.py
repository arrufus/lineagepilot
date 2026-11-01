"""A small, read-only view of a dbt ``manifest.json``.

Only the fields the selector engine and the Cosmos task-id mapping need are kept.
The file is read as JSON text with a size cap; nothing in it is executed.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

DEFAULT_MANIFEST_CAP = 200 * 1024 * 1024


class ManifestError(Exception):
    pass


@dataclass
class Node:
    unique_id: str
    resource_type: str
    name: str
    package_name: str
    fqn: list[str]
    original_file_path: str
    patch_path: str | None
    tags: list[str]
    config: dict[str, Any]
    depends_on: list[str]
    version: Any = None
    source_name: str | None = None
    external: bool = False

    @property
    def materialized(self) -> str | None:
        return self.config.get("materialized")


@dataclass
class Manifest:
    nodes: dict[str, Node]
    selectors: dict[str, Any]
    project_name: str | None
    metadata: dict[str, Any]
    path: str | None = None
    approximate: bool = False
    notes: list[str] = field(default_factory=list)
    children: dict[str, set[str]] = field(default_factory=dict)
    parents: dict[str, set[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.rebuild_graph()

    def rebuild_graph(self) -> None:
        self.children = {uid: set() for uid in self.nodes}
        self.parents = {uid: set() for uid in self.nodes}
        for uid, node in self.nodes.items():
            for parent in node.depends_on:
                if parent in self.nodes:
                    self.parents[uid].add(parent)
                    self.children[parent].add(uid)

    def of_type(self, *types: str) -> list[Node]:
        return [n for n in self.nodes.values() if n.resource_type in types]

    def artifact_version(self) -> dict[str, Any]:
        return {
            "generated_at": self.metadata.get("generated_at"),
            "dbt_version": self.metadata.get("dbt_version"),
            "dbt_schema_version": self.metadata.get("dbt_schema_version"),
            "approximate": self.approximate,
        }


def _node_from_json(uid: str, raw: dict[str, Any]) -> Node:
    config = raw.get("config") or {}
    tags = raw.get("tags")
    if tags is None:
        tags = config.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    depends = (raw.get("depends_on") or {}).get("nodes") or []
    ofp = raw.get("original_file_path") or ""
    resource_type = raw.get("resource_type") or uid.split(".", 1)[0]
    # dbt-loom and other plugins inject nodes from other projects with no file of their own.
    external = resource_type in ("model", "seed", "snapshot") and not ofp
    return Node(
        unique_id=uid,
        resource_type=resource_type,
        name=raw.get("name") or uid.split(".")[-1],
        package_name=raw.get("package_name") or uid.split(".")[1],
        fqn=list(raw.get("fqn") or []),
        original_file_path=ofp,
        patch_path=raw.get("patch_path"),
        tags=list(tags),
        config=config,
        depends_on=list(depends),
        version=raw.get("version"),
        source_name=raw.get("source_name"),
        external=external,
    )


def manifest_from_dict(data: dict[str, Any], path: str | None = None) -> Manifest:
    if not isinstance(data, dict) or "nodes" not in data:
        raise ManifestError("not a dbt manifest (no 'nodes' key)")
    nodes: dict[str, Node] = {}
    for section in ("nodes", "sources", "exposures", "metrics", "semantic_models", "saved_queries", "unit_tests"):
        for uid, raw in (data.get(section) or {}).items():
            if isinstance(raw, dict):
                nodes[uid] = _node_from_json(uid, raw)
    metadata = data.get("metadata") or {}
    project_name = metadata.get("project_name")
    if not project_name:
        # dbt < 1.3 manifests do not record the project name; use the most common root package
        counts: dict[str, int] = {}
        for n in nodes.values():
            if n.resource_type == "model" and not n.external:
                counts[n.package_name] = counts.get(n.package_name, 0) + 1
        project_name = max(counts, key=counts.get) if counts else None  # type: ignore[arg-type]
    selectors = data.get("selectors") or {}
    return Manifest(nodes=nodes, selectors=selectors, project_name=project_name, metadata=metadata, path=path)


def load_manifest(path: str, size_cap: int = DEFAULT_MANIFEST_CAP) -> Manifest:
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        raise ManifestError(f"cannot stat manifest: {exc}") from exc
    if size > size_cap:
        raise ManifestError(f"manifest is {size} bytes, over the {size_cap}-byte cap")
    try:
        with open(path, "rb") as fh:
            data = json.loads(fh.read().decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestError(f"cannot read manifest: {exc}") from exc
    return manifest_from_dict(data, path=path)


def load_dbt_ls_file(path: str, size_cap: int = DEFAULT_MANIFEST_CAP) -> list[dict[str, Any]]:
    """Read a ``dbt ls --output json`` file (one JSON object per line; log lines are skipped)."""
    if os.path.getsize(path) > size_cap:
        raise ManifestError("dbt ls file over size cap")
    out: list[dict[str, Any]] = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict) and "unique_id" in obj:
                out.append(obj)
    return out
