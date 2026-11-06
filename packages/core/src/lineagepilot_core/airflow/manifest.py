"""A small, read-only view of a dbt ``manifest.json``.

Only the fields the selector engine and the Cosmos task-id mapping need are kept.
The file is read as JSON text with a size cap; nothing in it is executed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lineagepilot_core.airflow.safe_io import is_regular_file, read_capped
from lineagepilot_core.parsers import _json

# As for the index: only stops a crafted file from exhausting memory
DEFAULT_MANIFEST_CAP = 1 << 30


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

    @property
    def dbt_version(self) -> str | None:
        version = self.metadata.get("dbt_version")
        return version if isinstance(version, str) else None

    def artifact_version(self) -> dict[str, Any]:
        return {
            "generated_at": self.metadata.get("generated_at"),
            "dbt_version": self.metadata.get("dbt_version"),
            "dbt_schema_version": self.metadata.get("dbt_schema_version"),
            "approximate": self.approximate,
        }


def _node_from_json(uid: str, raw: dict[str, Any]) -> Node:
    config = raw.get("config") if isinstance(raw.get("config"), dict) else {}
    tags = raw.get("tags")
    if tags is None:
        tags = config.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    depends_on = raw.get("depends_on") if isinstance(raw.get("depends_on"), dict) else {}
    ofp = raw.get("original_file_path")
    ofp = ofp if isinstance(ofp, str) else ""
    resource_type = raw.get("resource_type") or uid.split(".", 1)[0]
    # dbt-loom and other plugins inject nodes from other projects with no file of their own.
    external = resource_type in ("model", "seed", "snapshot") and not ofp
    return Node(
        unique_id=uid,
        resource_type=resource_type,
        name=raw.get("name") or uid.split(".")[-1],
        package_name=raw.get("package_name") or (uid.split(".") + [""])[1],
        fqn=_strings(raw.get("fqn")),
        original_file_path=ofp,
        patch_path=raw.get("patch_path") if isinstance(raw.get("patch_path"), str) else None,
        tags=_strings(tags),
        config=config,
        depends_on=_strings(depends_on.get("nodes")),
        version=raw.get("version"),
        source_name=raw.get("source_name"),
        external=external,
    )


def _strings(value: Any) -> list[str]:
    return [v for v in value if isinstance(v, str)] if isinstance(value, list) else []


def manifest_from_dict(data: dict[str, Any], path: str | None = None) -> Manifest:
    if not isinstance(data, dict) or "nodes" not in data:
        raise ManifestError("not a dbt manifest (no 'nodes' key)")
    nodes: dict[str, Node] = {}
    for section in (
        "nodes",
        "sources",
        "exposures",
        "metrics",
        "semantic_models",
        "saved_queries",
        "unit_tests",
    ):
        for uid, raw in (data.get(section) or {}).items():
            if isinstance(raw, dict):
                nodes[uid] = _node_from_json(uid, raw)
    metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    project_name = metadata.get("project_name")
    if not project_name:
        # dbt < 1.3 manifests do not record the project name; use the most common root package
        counts: dict[str, int] = {}
        for n in nodes.values():
            if n.resource_type == "model" and not n.external:
                counts[n.package_name] = counts.get(n.package_name, 0) + 1
        project_name = max(counts, key=counts.get) if counts else None  # type: ignore[arg-type]
    selectors = data.get("selectors") if isinstance(data.get("selectors"), dict) else {}
    return Manifest(
        nodes=nodes, selectors=selectors, project_name=project_name, metadata=metadata, path=path
    )


def load_manifest(path: str, size_cap: int = DEFAULT_MANIFEST_CAP) -> Manifest:
    if not is_regular_file(path):
        raise ManifestError("the manifest is not a regular file")
    if Path(path).stat().st_size > size_cap:
        raise ManifestError(f"the manifest is over the {size_cap}-byte limit")
    try:
        data = _json.load_file(path)
    except (OSError, ValueError) as exc:
        raise ManifestError(f"cannot read the manifest: {type(exc).__name__}") from exc
    return manifest_from_dict(data, path=path)


def load_dbt_ls_file(path: str, size_cap: int = DEFAULT_MANIFEST_CAP) -> list[dict[str, Any]]:
    """Read a ``dbt ls --output json`` file (one JSON object per line; log lines are skipped)."""
    data = read_capped(path, size_cap)
    if data is None:
        raise ManifestError("the dbt ls file is not a regular file under the size limit")
    out: list[dict[str, Any]] = []
    for line in data.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            continue
        if isinstance(obj, dict) and "unique_id" in obj:
            out.append(obj)
    return out
