"""Graph snapshots: fast warm starts and stored baselines (design section 4.3, task 1.8).

A snapshot is one JSON file holding the whole graph: nodes, columns, edges with their
evidence, unresolved records and column-lineage states. Spike S2 found JSON loads faster
than SQLite. Artifact versions and evidence records are stored once, in tables, and
referenced by index, because thousands of edges share them.

Snapshots are keyed by the sha256 of the manifest and catalog, the snapshot format and
the core version, so a changed artifact or an upgraded LineagePilot never reads a stale
one. Writes are atomic. A snapshot that fails to load for any reason is treated as
missing: the caller rebuilds the graph from the artifacts, which stay the source of
truth.
"""

from __future__ import annotations

import gc
import hashlib
import logging
import os
import tempfile
from collections.abc import Iterator
from dataclasses import MISSING, fields
from pathlib import Path
from typing import Any

from lineagepilot_core import __version__
from lineagepilot_core.graph.model import (
    ArtifactVersion,
    Column,
    ColumnLineageStatus,
    Confidence,
    EdgeKind,
    Evidence,
    LineageState,
    Location,
    MetadataGraph,
    Node,
    NodeKind,
    Origin,
    Unresolved,
)
from lineagepilot_core.parsers import _json

logger = logging.getLogger(__name__)

SNAPSHOT_FORMAT = 1

_REQUIRED = object()


def _defaults(record_type: type) -> list[tuple[str, Any]]:
    """(field name, default) for a dataclass, with _REQUIRED for fields without one."""
    out = []
    for f in fields(record_type):
        if f.default is not MISSING:
            out.append((f.name, f.default))
        elif f.default_factory is not MISSING:
            out.append((f.name, f.default_factory()))
        else:
            out.append((f.name, _REQUIRED))
    return out


_NODE_FIELDS = _defaults(Node)
_COLUMN_FIELDS = _defaults(Column)


class SnapshotError(ValueError):
    """A snapshot that cannot be used."""


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def snapshot_key(manifest_sha256: str, catalog_sha256: str | None, column_lineage: bool) -> str:
    """The cache key: what the graph was built from, and by which code."""
    parts = [
        f"format={SNAPSHOT_FORMAT}",
        f"core={__version__}",
        f"manifest={manifest_sha256}",
        f"catalog={catalog_sha256 or '-'}",
        f"column_lineage={column_lineage}",
    ]
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


# Encoding


def to_dict(graph: MetadataGraph) -> dict[str, Any]:
    artifacts: dict[ArtifactVersion, int] = {}
    evidence: dict[Evidence, int] = {}

    def artifact_index(artifact: ArtifactVersion | None) -> int | None:
        if artifact is None:
            return None
        return artifacts.setdefault(artifact, len(artifacts))

    def evidence_index(item: Evidence) -> int:
        if item not in evidence:
            evidence[item] = len(evidence)
        return evidence[item]

    nodes = [_encode(node, _NODE_FIELDS) for node in graph.nodes.values()]
    columns = [_encode(column, _COLUMN_FIELDS) for column in graph.columns.values()]
    edges = [
        [
            edge.kind.value,
            edge.upstream,
            edge.downstream,
            edge.transformation,
            [evidence_index(e) for e in edge.evidence],
        ]
        for edge in graph.edges
    ]
    unresolved = [
        [u.subject, u.reference, u.reason, evidence_index(u.evidence)] for u in graph.unresolved
    ]
    manifest = artifact_index(graph.manifest)
    evidence_table = [
        [e.origin.value, e.confidence.value, artifact_index(e.artifact), _location(e.location)]
        for e in evidence
    ]
    return {
        "format": SNAPSHOT_FORMAT,
        "core": __version__,
        "adapter_type": graph.adapter_type,
        "project_name": graph.project_name,
        "schema_version": graph.schema_version,
        "manifest": manifest,
        "artifacts": [[a.path, a.sha256, a.generated_at, a.dbt_version] for a in artifacts],
        "evidence": evidence_table,
        "nodes": nodes,
        "columns": columns,
        "edges": edges,
        "unresolved": unresolved,
        "column_lineage": {
            node_id: [status.state.value, status.reason]
            for node_id, status in graph.column_lineage.items()
        },
    }


def _encode(record: Node | Column, record_fields: list[tuple[str, Any]]) -> dict[str, Any]:
    """The fields that differ from their defaults; enums as their values."""
    out: dict[str, Any] = {}
    for name, default in record_fields:
        value = getattr(record, name)
        if default is not _REQUIRED and value == default:
            continue
        out[name] = value.value if isinstance(value, NodeKind) else value
    return out


def _location(location: Location | None) -> list[Any] | None:
    if location is None:
        return None
    return [
        location.path,
        location.start_line,
        location.start_character,
        location.end_line,
        location.end_character,
    ]


# Decoding


def from_dict(data: Any, consume: bool = False) -> MetadataGraph:
    """Rebuild a graph. Raises SnapshotError for anything malformed or from another
    format or core version.

    The garbage collector is paused while the graph is rebuilt: the loader creates no
    reference cycles, and collections over a growing heap of 240,000 objects took half
    the warm start at 5,000 models (task 1.8).

    With ``consume``, each record is released as soon as it is in the graph, which keeps
    the peak memory of a warm start down; ``data`` is emptied.
    """
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        return _from_dict(data, consume)
    except SnapshotError:
        raise
    except (KeyError, IndexError, TypeError, ValueError, AttributeError) as e:
        raise SnapshotError(f"malformed snapshot: {type(e).__name__}: {e}") from None
    finally:
        if was_enabled:
            gc.enable()


def _records(data: dict[str, Any], key: str, consume: bool) -> Iterator[Any]:
    """The records of one section, each released after use when consuming."""
    records = data.pop(key) if consume else data[key]
    if not isinstance(records, list):
        raise TypeError(f"'{key}' is not a list")
    for index in range(len(records)):
        yield records[index]
        if consume:
            records[index] = None


def _from_dict(data: Any, consume: bool) -> MetadataGraph:
    if not isinstance(data, dict):
        raise SnapshotError("not a snapshot")
    if data.get("format") != SNAPSHOT_FORMAT or data.get("core") != __version__:
        raise SnapshotError(
            f"snapshot format {data.get('format')} from core {data.get('core')} "
            f"does not match format {SNAPSHOT_FORMAT}, core {__version__}"
        )
    artifacts = [ArtifactVersion(*row) for row in data["artifacts"]]
    evidence = [
        Evidence(
            Origin(origin),
            Confidence(confidence),
            artifacts[artifact] if artifact is not None else None,
            Location(*location) if location is not None else None,
        )
        for origin, confidence, artifact, location in data["evidence"]
    ]

    graph = MetadataGraph(adapter_type=data["adapter_type"])
    graph.project_name = data["project_name"]
    graph.schema_version = data["schema_version"]
    if data["manifest"] is not None:
        graph.manifest = artifacts[data["manifest"]]
    node_kinds = {kind.value: kind for kind in NodeKind}
    for record in _records(data, "nodes", consume):
        graph.add_node(Node(**{**record, "kind": node_kinds[record["kind"]]}))
    for record in _records(data, "columns", consume):
        graph.add_column(Column(**record))
    edge_kinds = {kind.value: kind for kind in EdgeKind}
    for kind, upstream, downstream, transformation, refs in _records(data, "edges", consume):
        if not refs:
            raise SnapshotError(f"edge {upstream} -> {downstream} has no evidence")
        edge = graph.add_edge(
            edge_kinds[kind], upstream, downstream, evidence[refs[0]], transformation
        )
        for ref in refs[1:]:
            edge.evidence.append(evidence[ref])
    for subject, reference, reason, ref in _records(data, "unresolved", consume):
        graph.add_unresolved(Unresolved(subject, reference, reason, evidence[ref]))
    for node_id, (state, reason) in data["column_lineage"].items():
        graph.node(node_id)  # must exist
        graph.column_lineage[node_id] = ColumnLineageStatus(LineageState(state), reason)
    return graph


# Files


def save_snapshot(graph: MetadataGraph, path: str | Path) -> None:
    """Write atomically: a reader never sees half a snapshot."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _json.dumps(to_dict(graph))
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".snapshot-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def load_snapshot(path: str | Path) -> MetadataGraph:
    """Raises SnapshotError if the file is missing, unreadable or malformed."""
    was_enabled = gc.isenabled()
    gc.disable()  # decoding allocates the same objects; see from_dict
    try:
        data = _json.load_file(path)
    except (OSError, ValueError) as e:
        raise SnapshotError(f"cannot read snapshot {path}: {e}") from None
    finally:
        if was_enabled:
            gc.enable()
    return from_dict(data, consume=True)


class SnapshotCache:
    """Snapshots in a directory, by key. The newest ``keep`` are kept as baselines."""

    def __init__(self, directory: str | Path, keep: int = 3) -> None:
        self.directory = Path(directory)
        self.keep = keep

    def path(self, key: str) -> Path:
        if not key.isalnum():
            raise ValueError(f"not a snapshot key: {key!r}")
        return self.directory / f"{key}.json"

    def get(self, key: str) -> MetadataGraph | None:
        path = self.path(key)
        if not path.is_file():
            return None
        try:
            graph = load_snapshot(path)
        except SnapshotError as e:
            logger.warning("Ignoring snapshot %s: %s", path.name, e)
            path.unlink(missing_ok=True)
            return None
        os.utime(path)  # most recently used
        return graph

    def put(self, key: str, graph: MetadataGraph) -> Path:
        path = self.path(key)
        save_snapshot(graph, path)
        self._prune()
        return path

    def _prune(self) -> None:
        snapshots = sorted(
            self.directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        for old in snapshots[self.keep :]:
            old.unlink(missing_ok=True)
