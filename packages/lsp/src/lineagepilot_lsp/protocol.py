"""Generated from protocol/lineage-protocol.schema.json by protocol/generate.py. Do not edit.

The lineage/* protocol (design section 9). The server validates every request it
receives with ``<Params>.from_json``; tests validate what it sends with ``validate``.
"""

# ruff: noqa: E501
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Literal

PROTOCOL_VERSION = 1

# method -> (params definition, result definition)
REQUESTS = {
    "lineage/status": [
        "EmptyParams",
        "Status"
    ],
    "lineage/refresh": [
        "EmptyParams",
        "Status"
    ],
    "lineage/graph": [
        "GraphParams",
        "GraphResult"
    ],
    "lineage/impact": [
        "ImpactParams",
        "ImpactResult"
    ],
    "lineage/export": [
        "ExportParams",
        "ExportResult"
    ],
    "lineage/nodesForFile": [
        "NodesForFileParams",
        "NodesForFileResult"
    ]
}

# method -> params definition
NOTIFICATIONS = {
    "lineage/indexUpdated": "Status"
}


class ErrorCode:
    NOT_READY = -32001
    UNKNOWN_NODE = -32002
    PROTOCOL_MISMATCH = -32003


# The schema's definitions, as data for the validator
DEFS: dict[str, Any] = {'Id': {'type': 'string', 'minLength': 1, 'maxLength': 4096}, 'Name': {'type': 'string', 'maxLength': 4096}, 'Text': {'type': 'string', 'maxLength': 65536}, 'Uri': {'type': 'string', 'minLength': 1, 'maxLength': 8192}, 'Count': {'type': 'integer', 'minimum': 0}, 'NodeKind': {'enum': ['model', 'seed', 'snapshot', 'source', 'exposure', 'relation', 'airflow_dag', 'airflow_task', 'column']}, 'EdgeKind': {'enum': ['depends_on', 'derives', 'materialises', 'orchestrates', 'task_depends_on']}, 'Confidence': {'enum': ['declared', 'inferred', 'heuristic']}, 'Origin': {'enum': ['manifest', 'compiled_sql', 'catalog', 'dag_static', 'annotation']}, 'Direction': {'enum': ['upstream', 'downstream', 'both']}, 'Side': {'enum': ['root', 'upstream', 'downstream']}, 'Granularity': {'enum': ['model', 'column']}, 'LineageState': {'enum': ['resolved', 'partial', 'unknown']}, 'IndexState': {'enum': ['starting', 'ready', 'unknown', 'failed']}, 'ColumnStage': {'enum': ['pending', 'computing', 'done', 'off']}, 'ExportFormat': {'enum': ['json', 'mermaid', 'dot', 'graphml', 'cypher', 'pii-markdown', 'pii-csv']}, 'Position': {'type': 'object', 'properties': {'line': {'$ref': '#/$defs/Count'}, 'character': {'$ref': '#/$defs/Count'}}, 'required': ['line', 'character'], 'additionalProperties': False}, 'Range': {'type': 'object', 'properties': {'start': {'$ref': '#/$defs/Position'}, 'end': {'$ref': '#/$defs/Position'}}, 'required': ['start', 'end'], 'additionalProperties': False}, 'Location': {'description': 'A file to open, and where in it when known.', 'type': 'object', 'properties': {'uri': {'$ref': '#/$defs/Uri'}, 'range': {'anyOf': [{'$ref': '#/$defs/Range'}, {'type': 'null'}]}}, 'required': ['uri', 'range'], 'additionalProperties': False}, 'Unknown': {'description': 'A reference that could not be resolved, or a model whose column lineage is unknown. Shown, never dropped.', 'type': 'object', 'properties': {'subject': {'$ref': '#/$defs/Id'}, 'reference': {'$ref': '#/$defs/Text'}, 'reason': {'$ref': '#/$defs/Text'}}, 'required': ['subject', 'reference', 'reason'], 'additionalProperties': False}, 'EmptyParams': {'type': 'object', 'properties': {}, 'additionalProperties': False}, 'GraphParams': {'type': 'object', 'properties': {'root': {'$ref': '#/$defs/Id', 'description': 'A node ID, or a column ID at column granularity.'}, 'direction': {'$ref': '#/$defs/Direction'}, 'depth': {'type': 'integer', 'minimum': 1, 'maximum': 10}, 'granularity': {'$ref': '#/$defs/Granularity'}, 'maxNodes': {'type': 'integer', 'minimum': 1, 'maximum': 5000}}, 'required': ['root', 'direction', 'depth', 'granularity'], 'additionalProperties': False}, 'GraphNode': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'kind': {'$ref': '#/$defs/NodeKind'}, 'name': {'$ref': '#/$defs/Name'}, 'side': {'$ref': '#/$defs/Side'}, 'depth': {'$ref': '#/$defs/Count'}, 'parent': {'anyOf': [{'$ref': '#/$defs/Id'}, {'type': 'null'}], 'description': 'The node a column belongs to.'}, 'location': {'anyOf': [{'$ref': '#/$defs/Location'}, {'type': 'null'}]}, 'layer': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'materialization': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'description': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'columnLineage': {'anyOf': [{'$ref': '#/$defs/LineageState'}, {'type': 'null'}]}, 'columnLineageReason': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'pii': {'type': 'boolean'}}, 'required': ['id', 'kind', 'name', 'side', 'depth', 'parent', 'location', 'layer', 'materialization', 'owner', 'description', 'columnLineage', 'columnLineageReason', 'pii'], 'additionalProperties': False}, 'GraphEdge': {'type': 'object', 'properties': {'kind': {'$ref': '#/$defs/EdgeKind'}, 'upstream': {'$ref': '#/$defs/Id'}, 'downstream': {'$ref': '#/$defs/Id'}, 'relation': {'anyOf': [{'$ref': '#/$defs/Id'}, {'type': 'null'}], 'description': 'The physical table between a source and the seed, model or snapshot that builds it.'}, 'confidence': {'$ref': '#/$defs/Confidence'}, 'origins': {'type': 'array', 'items': {'$ref': '#/$defs/Origin'}, 'maxItems': 5}, 'transformation': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['kind', 'upstream', 'downstream', 'relation', 'confidence', 'origins', 'transformation'], 'additionalProperties': False}, 'GraphResult': {'type': 'object', 'properties': {'root': {'$ref': '#/$defs/Id'}, 'direction': {'$ref': '#/$defs/Direction'}, 'granularity': {'$ref': '#/$defs/Granularity'}, 'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/GraphNode'}}, 'edges': {'type': 'array', 'items': {'$ref': '#/$defs/GraphEdge'}}, 'truncated': {'type': 'boolean'}, 'unknowns': {'type': 'array', 'items': {'$ref': '#/$defs/Unknown'}}}, 'required': ['root', 'direction', 'granularity', 'nodes', 'edges', 'truncated', 'unknowns'], 'additionalProperties': False}, 'ImpactParams': {'type': 'object', 'properties': {'targets': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}, 'minItems': 1, 'maxItems': 200}, 'maxDepth': {'type': 'integer', 'minimum': 1, 'maximum': 100}}, 'required': ['targets'], 'additionalProperties': False}, 'ImpactedNode': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'kind': {'$ref': '#/$defs/NodeKind'}, 'name': {'$ref': '#/$defs/Name'}, 'depth': {'$ref': '#/$defs/Count'}, 'granularity': {'$ref': '#/$defs/Granularity'}, 'confidence': {'$ref': '#/$defs/Confidence'}, 'columns': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}}, 'reason': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'location': {'anyOf': [{'$ref': '#/$defs/Location'}, {'type': 'null'}]}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['id', 'kind', 'name', 'depth', 'granularity', 'confidence', 'columns', 'reason', 'location', 'owner'], 'additionalProperties': False}, 'ImpactResult': {'type': 'object', 'properties': {'targets': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}}, 'truncated': {'type': 'boolean'}, 'counts': {'type': 'object', 'additionalProperties': {'$ref': '#/$defs/Count'}}, 'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/ImpactedNode'}}, 'unknowns': {'type': 'array', 'items': {'$ref': '#/$defs/Unknown'}}, 'columnLineage': {'$ref': '#/$defs/ColumnStage', 'description': 'While computing, models not yet analysed are impacted at model level.'}}, 'required': ['targets', 'truncated', 'counts', 'nodes', 'unknowns', 'columnLineage'], 'additionalProperties': False}, 'ExportParams': {'type': 'object', 'properties': {'format': {'$ref': '#/$defs/ExportFormat'}, 'root': {'$ref': '#/$defs/Id', 'description': 'Export the neighbourhood of this node; the whole graph when absent.'}, 'direction': {'$ref': '#/$defs/Direction'}, 'depth': {'type': 'integer', 'minimum': 1, 'maximum': 10}}, 'required': ['format'], 'additionalProperties': False}, 'ExportResult': {'type': 'object', 'properties': {'format': {'$ref': '#/$defs/ExportFormat'}, 'filename': {'$ref': '#/$defs/Name'}, 'content': {'type': 'string'}, 'nodes': {'$ref': '#/$defs/Count'}, 'edges': {'$ref': '#/$defs/Count'}}, 'required': ['format', 'filename', 'content', 'nodes', 'edges'], 'additionalProperties': False}, 'NodesForFileParams': {'type': 'object', 'properties': {'uri': {'$ref': '#/$defs/Uri'}}, 'required': ['uri'], 'additionalProperties': False}, 'NodesForFileResult': {'type': 'object', 'properties': {'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/GraphNode'}}}, 'required': ['nodes'], 'additionalProperties': False}, 'IndexError': {'type': 'object', 'properties': {'severity': {'enum': ['error', 'warning']}, 'message': {'$ref': '#/$defs/Text'}, 'location': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}}, 'required': ['severity', 'message', 'location'], 'additionalProperties': False}, 'ServerInfo': {'type': 'object', 'properties': {'version': {'$ref': '#/$defs/Name'}, 'python': {'$ref': '#/$defs/Name'}, 'executable': {'$ref': '#/$defs/Text'}, 'pid': {'$ref': '#/$defs/Count'}}, 'required': ['version', 'python', 'executable', 'pid'], 'additionalProperties': False}, 'ProjectInfo': {'type': 'object', 'properties': {'root': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'name': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'problems': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}, 'maxItems': 100}}, 'required': ['root', 'name', 'problems'], 'additionalProperties': False}, 'ManifestInfo': {'type': 'object', 'properties': {'path': {'$ref': '#/$defs/Text'}, 'exists': {'type': 'boolean'}, 'generatedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'dbtVersion': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'schemaVersion': {'anyOf': [{'type': 'integer'}, {'type': 'null'}]}, 'modifiedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['path', 'exists', 'generatedAt', 'dbtVersion', 'schemaVersion', 'modifiedAt'], 'additionalProperties': False}, 'CatalogInfo': {'type': 'object', 'properties': {'path': {'$ref': '#/$defs/Text'}, 'exists': {'type': 'boolean'}, 'modifiedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['path', 'exists', 'modifiedAt'], 'additionalProperties': False}, 'Artifacts': {'type': 'object', 'properties': {'manifest': {'anyOf': [{'$ref': '#/$defs/ManifestInfo'}, {'type': 'null'}]}, 'catalog': {'anyOf': [{'$ref': '#/$defs/CatalogInfo'}, {'type': 'null'}]}}, 'required': ['manifest', 'catalog'], 'additionalProperties': False}, 'Counts': {'type': 'object', 'properties': {'nodes': {'$ref': '#/$defs/Count'}, 'columns': {'$ref': '#/$defs/Count'}, 'edges': {'$ref': '#/$defs/Count'}, 'unresolved': {'$ref': '#/$defs/Count'}, 'nodeKinds': {'type': 'object', 'additionalProperties': {'$ref': '#/$defs/Count'}}}, 'required': ['nodes', 'columns', 'edges', 'unresolved', 'nodeKinds'], 'additionalProperties': False}, 'ColumnStates': {'type': 'object', 'properties': {'resolved': {'$ref': '#/$defs/Count'}, 'partial': {'$ref': '#/$defs/Count'}, 'unknown': {'$ref': '#/$defs/Count'}}, 'required': ['resolved', 'partial', 'unknown'], 'additionalProperties': False}, 'Stale': {'description': 'Source files changed after the manifest was written.', 'type': 'object', 'properties': {'count': {'$ref': '#/$defs/Count'}, 'examples': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}, 'maxItems': 5}}, 'required': ['count', 'examples'], 'additionalProperties': False}, 'RulesStatus': {'description': 'The conformance rules behind the diagnostics. In Restricted Mode only the built-in rules run.', 'type': 'object', 'properties': {'pack': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}], 'description': 'The rule pack file, if the project has one.'}, 'packLoaded': {'type': 'boolean'}, 'restricted': {'type': 'boolean'}, 'problems': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}, 'maxItems': 50}, 'findings': {'$ref': '#/$defs/Count'}, 'score': {'anyOf': [{'type': 'number', 'minimum': 0, 'maximum': 100}, {'type': 'null'}]}, 'coverage': {'anyOf': [{'type': 'number', 'minimum': 0, 'maximum': 100}, {'type': 'null'}]}}, 'required': ['pack', 'packLoaded', 'restricted', 'problems', 'findings', 'score', 'coverage'], 'additionalProperties': False}, 'Status': {'type': 'object', 'properties': {'protocolVersion': {'anyOf': [{'type': 'integer'}, {'type': 'null'}]}, 'server': {'$ref': '#/$defs/ServerInfo'}, 'project': {'$ref': '#/$defs/ProjectInfo'}, 'trusted': {'type': 'boolean'}, 'state': {'$ref': '#/$defs/IndexState'}, 'indexing': {'type': 'boolean'}, 'generation': {'$ref': '#/$defs/Count'}, 'columnLineage': {'$ref': '#/$defs/ColumnStage'}, 'message': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'errors': {'type': 'array', 'items': {'$ref': '#/$defs/IndexError'}, 'maxItems': 50}, 'errorCount': {'$ref': '#/$defs/Count'}, 'artifacts': {'$ref': '#/$defs/Artifacts'}, 'counts': {'anyOf': [{'$ref': '#/$defs/Counts'}, {'type': 'null'}]}, 'columnStates': {'anyOf': [{'$ref': '#/$defs/ColumnStates'}, {'type': 'null'}]}, 'stale': {'$ref': '#/$defs/Stale'}, 'fromCache': {'type': 'boolean'}, 'indexedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'rules': {'$ref': '#/$defs/RulesStatus'}}, 'required': ['protocolVersion', 'server', 'project', 'trusted', 'state', 'indexing', 'generation', 'columnLineage', 'message', 'errors', 'errorCount', 'artifacts', 'counts', 'columnStates', 'stale', 'fromCache', 'indexedAt', 'rules'], 'additionalProperties': False}}

class ValidationError(ValueError):
    """A message that does not match the protocol schema."""

    def __init__(self, path: str, message: str) -> None:
        super().__init__(f"{path}: {message}")
        self.path = path


def validate(type_name: str, value: Any) -> None:
    """Raise ValidationError unless ``value`` matches the named definition."""
    _check(DEFS[type_name], value, type_name)


def _check(node: dict[str, Any], value: Any, path: str) -> None:
    if "$ref" in node:
        _check(DEFS[node["$ref"].rsplit("/", 1)[1]], value, path)
        return
    if "anyOf" in node:
        errors = []
        for branch in node["anyOf"]:
            try:
                _check(branch, value, path)
                return
            except ValidationError as e:
                errors.append(str(e))
        raise ValidationError(path, "matches none of: " + "; ".join(errors))
    if "enum" in node:
        if not isinstance(value, str) or value not in node["enum"]:
            raise ValidationError(path, f"must be one of {node['enum']}, not {_show(value)}")
        return
    kind = node["type"]
    if not _TYPE_CHECKS[kind](value):
        raise ValidationError(path, f"must be {kind}, not {_show(value)}")
    if kind == "string":
        _within(len(value), node, "minLength", "maxLength", path, "characters")
    elif kind in ("integer", "number"):
        _within(value, node, "minimum", "maximum", path, "")
    elif kind == "array":
        _within(len(value), node, "minItems", "maxItems", path, "items")
        for i, item in enumerate(value):
            _check(node["items"], item, f"{path}[{i}]")
    elif kind == "object":
        props = node.get("properties")
        if props is None:
            for key, item in value.items():
                _check(node["additionalProperties"], item, f"{path}.{key}")
            return
        for key in node.get("required", []):
            if key not in value:
                raise ValidationError(path, f"missing {key!r}")
        for key, item in value.items():
            if key not in props:
                raise ValidationError(path, f"unexpected {key!r}")
            _check(props[key], item, f"{path}.{key}")


def _within(size: float, node: dict[str, Any], low: str, high: str, path: str, unit: str) -> None:
    if low in node and size < node[low]:
        raise ValidationError(path, f"must be at least {node[low]} {unit}".rstrip())
    if high in node and size > node[high]:
        raise ValidationError(path, f"must be at most {node[high]} {unit}".rstrip())


def _show(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 40 else text[:37] + "..."


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return (_is_int(value) or isinstance(value, float)) and value == value and abs(value) != float("inf")


_TYPE_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "integer": _is_int,
    "number": _is_number,
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
    "object": lambda v: isinstance(v, dict) and all(isinstance(k, str) for k in v),
    "array": lambda v: isinstance(v, list),
}


def _build(node: dict[str, Any], value: Any) -> Any:
    """A validated value, with records as their dataclasses."""
    if "$ref" in node:
        name = node["$ref"].rsplit("/", 1)[1]
        record = RECORDS.get(name)
        return record._build(value) if record else _build(DEFS[name], value)
    if "anyOf" in node:
        for branch in node["anyOf"]:
            try:
                _check(branch, value, "")
            except ValidationError:
                continue
            return _build(branch, value)
    if isinstance(value, list):
        return [_build(node["items"], item) for item in value]
    if isinstance(value, dict) and "additionalProperties" in node:
        return {key: _build(node["additionalProperties"], item) for key, item in value.items()}
    return value


def _unbuild(value: Any) -> Any:
    if hasattr(value, "to_json"):
        return value.to_json()
    if isinstance(value, list):
        return [_unbuild(item) for item in value]
    if isinstance(value, dict):
        return {key: _unbuild(item) for key, item in value.items()}
    return value


class _Record:
    """Base of the generated dataclasses."""

    _NAME: ClassVar[str]
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]]  # (attribute, JSON key, required)

    @classmethod
    def from_json(cls, data: Any) -> Any:
        """Validate a decoded JSON message and build the dataclass."""
        validate(cls._NAME, data)
        return cls._build(data)

    @classmethod
    def _build(cls, data: dict[str, Any]) -> Any:
        props = DEFS[cls._NAME]["properties"]
        return cls(**{attr: _build(props[key], data[key]) for attr, key, _ in cls._FIELDS if key in data})

    def to_json(self) -> dict[str, Any]:
        out = {}
        for attr, key, required in self._FIELDS:
            value = getattr(self, attr)
            if required or value is not None:
                out[key] = _unbuild(value)
        return out


Id = str

Name = str

Text = str

Uri = str

Count = int

NodeKind = Literal["model", "seed", "snapshot", "source", "exposure", "relation", "airflow_dag", "airflow_task", "column"]

EdgeKind = Literal["depends_on", "derives", "materialises", "orchestrates", "task_depends_on"]

Confidence = Literal["declared", "inferred", "heuristic"]

Origin = Literal["manifest", "compiled_sql", "catalog", "dag_static", "annotation"]

Direction = Literal["upstream", "downstream", "both"]

Side = Literal["root", "upstream", "downstream"]

Granularity = Literal["model", "column"]

LineageState = Literal["resolved", "partial", "unknown"]

IndexState = Literal["starting", "ready", "unknown", "failed"]

ColumnStage = Literal["pending", "computing", "done", "off"]

ExportFormat = Literal["json", "mermaid", "dot", "graphml", "cypher", "pii-markdown", "pii-csv"]


@dataclass(frozen=True)
class Position(_Record):
    _NAME: ClassVar[str] = "Position"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('line', 'line', True), ('character', 'character', True))

    line: Count
    character: Count


@dataclass(frozen=True)
class Range(_Record):
    _NAME: ClassVar[str] = "Range"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('start', 'start', True), ('end', 'end', True))

    start: Position
    end: Position


@dataclass(frozen=True)
class Location(_Record):
    """A file to open, and where in it when known."""

    _NAME: ClassVar[str] = "Location"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('uri', 'uri', True), ('range', 'range', True))

    uri: Uri
    range: Range | None


@dataclass(frozen=True)
class Unknown(_Record):
    """A reference that could not be resolved, or a model whose column lineage is unknown. Shown, never dropped."""

    _NAME: ClassVar[str] = "Unknown"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('subject', 'subject', True), ('reference', 'reference', True), ('reason', 'reason', True))

    subject: Id
    reference: Text
    reason: Text


@dataclass(frozen=True)
class EmptyParams(_Record):
    _NAME: ClassVar[str] = "EmptyParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = ()


@dataclass(frozen=True)
class GraphParams(_Record):
    _NAME: ClassVar[str] = "GraphParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('root', 'root', True), ('direction', 'direction', True), ('depth', 'depth', True), ('granularity', 'granularity', True), ('max_nodes', 'maxNodes', False))

    root: Id
    direction: Direction
    depth: int
    granularity: Granularity
    max_nodes: int | None = None


@dataclass(frozen=True)
class GraphNode(_Record):
    _NAME: ClassVar[str] = "GraphNode"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True), ('kind', 'kind', True), ('name', 'name', True), ('side', 'side', True), ('depth', 'depth', True), ('parent', 'parent', True), ('location', 'location', True), ('layer', 'layer', True), ('materialization', 'materialization', True), ('owner', 'owner', True), ('description', 'description', True), ('column_lineage', 'columnLineage', True), ('column_lineage_reason', 'columnLineageReason', True), ('pii', 'pii', True))

    id: Id
    kind: NodeKind
    name: Name
    side: Side
    depth: Count
    parent: Id | None
    location: Location | None
    layer: Name | None
    materialization: Name | None
    owner: Name | None
    description: Text | None
    column_lineage: LineageState | None
    column_lineage_reason: Text | None
    pii: bool


@dataclass(frozen=True)
class GraphEdge(_Record):
    _NAME: ClassVar[str] = "GraphEdge"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('kind', 'kind', True), ('upstream', 'upstream', True), ('downstream', 'downstream', True), ('relation', 'relation', True), ('confidence', 'confidence', True), ('origins', 'origins', True), ('transformation', 'transformation', True))

    kind: EdgeKind
    upstream: Id
    downstream: Id
    relation: Id | None
    confidence: Confidence
    origins: list[Origin]
    transformation: Name | None


@dataclass(frozen=True)
class GraphResult(_Record):
    _NAME: ClassVar[str] = "GraphResult"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('root', 'root', True), ('direction', 'direction', True), ('granularity', 'granularity', True), ('nodes', 'nodes', True), ('edges', 'edges', True), ('truncated', 'truncated', True), ('unknowns', 'unknowns', True))

    root: Id
    direction: Direction
    granularity: Granularity
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    truncated: bool
    unknowns: list[Unknown]


@dataclass(frozen=True)
class ImpactParams(_Record):
    _NAME: ClassVar[str] = "ImpactParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('targets', 'targets', True), ('max_depth', 'maxDepth', False))

    targets: list[Id]
    max_depth: int | None = None


@dataclass(frozen=True)
class ImpactedNode(_Record):
    _NAME: ClassVar[str] = "ImpactedNode"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True), ('kind', 'kind', True), ('name', 'name', True), ('depth', 'depth', True), ('granularity', 'granularity', True), ('confidence', 'confidence', True), ('columns', 'columns', True), ('reason', 'reason', True), ('location', 'location', True), ('owner', 'owner', True))

    id: Id
    kind: NodeKind
    name: Name
    depth: Count
    granularity: Granularity
    confidence: Confidence
    columns: list[Id]
    reason: Text | None
    location: Location | None
    owner: Name | None


@dataclass(frozen=True)
class ImpactResult(_Record):
    _NAME: ClassVar[str] = "ImpactResult"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('targets', 'targets', True), ('truncated', 'truncated', True), ('counts', 'counts', True), ('nodes', 'nodes', True), ('unknowns', 'unknowns', True), ('column_lineage', 'columnLineage', True))

    targets: list[Id]
    truncated: bool
    counts: dict[str, Count]
    nodes: list[ImpactedNode]
    unknowns: list[Unknown]
    column_lineage: ColumnStage


@dataclass(frozen=True)
class ExportParams(_Record):
    _NAME: ClassVar[str] = "ExportParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('format', 'format', True), ('root', 'root', False), ('direction', 'direction', False), ('depth', 'depth', False))

    format: ExportFormat
    root: Id | None = None
    direction: Direction | None = None
    depth: int | None = None


@dataclass(frozen=True)
class ExportResult(_Record):
    _NAME: ClassVar[str] = "ExportResult"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('format', 'format', True), ('filename', 'filename', True), ('content', 'content', True), ('nodes', 'nodes', True), ('edges', 'edges', True))

    format: ExportFormat
    filename: Name
    content: str
    nodes: Count
    edges: Count


@dataclass(frozen=True)
class NodesForFileParams(_Record):
    _NAME: ClassVar[str] = "NodesForFileParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('uri', 'uri', True),)

    uri: Uri


@dataclass(frozen=True)
class NodesForFileResult(_Record):
    _NAME: ClassVar[str] = "NodesForFileResult"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('nodes', 'nodes', True),)

    nodes: list[GraphNode]


@dataclass(frozen=True)
class IndexError(_Record):
    _NAME: ClassVar[str] = "IndexError"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('severity', 'severity', True), ('message', 'message', True), ('location', 'location', True))

    severity: Literal["error", "warning"]
    message: Text
    location: Text | None


@dataclass(frozen=True)
class ServerInfo(_Record):
    _NAME: ClassVar[str] = "ServerInfo"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('version', 'version', True), ('python', 'python', True), ('executable', 'executable', True), ('pid', 'pid', True))

    version: Name
    python: Name
    executable: Text
    pid: Count


@dataclass(frozen=True)
class ProjectInfo(_Record):
    _NAME: ClassVar[str] = "ProjectInfo"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('root', 'root', True), ('name', 'name', True), ('problems', 'problems', True))

    root: Text | None
    name: Name | None
    problems: list[Text]


@dataclass(frozen=True)
class ManifestInfo(_Record):
    _NAME: ClassVar[str] = "ManifestInfo"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('path', 'path', True), ('exists', 'exists', True), ('generated_at', 'generatedAt', True), ('dbt_version', 'dbtVersion', True), ('schema_version', 'schemaVersion', True), ('modified_at', 'modifiedAt', True))

    path: Text
    exists: bool
    generated_at: Name | None
    dbt_version: Name | None
    schema_version: int | None
    modified_at: Name | None


@dataclass(frozen=True)
class CatalogInfo(_Record):
    _NAME: ClassVar[str] = "CatalogInfo"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('path', 'path', True), ('exists', 'exists', True), ('modified_at', 'modifiedAt', True))

    path: Text
    exists: bool
    modified_at: Name | None


@dataclass(frozen=True)
class Artifacts(_Record):
    _NAME: ClassVar[str] = "Artifacts"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('manifest', 'manifest', True), ('catalog', 'catalog', True))

    manifest: ManifestInfo | None
    catalog: CatalogInfo | None


@dataclass(frozen=True)
class Counts(_Record):
    _NAME: ClassVar[str] = "Counts"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('nodes', 'nodes', True), ('columns', 'columns', True), ('edges', 'edges', True), ('unresolved', 'unresolved', True), ('node_kinds', 'nodeKinds', True))

    nodes: Count
    columns: Count
    edges: Count
    unresolved: Count
    node_kinds: dict[str, Count]


@dataclass(frozen=True)
class ColumnStates(_Record):
    _NAME: ClassVar[str] = "ColumnStates"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('resolved', 'resolved', True), ('partial', 'partial', True), ('unknown', 'unknown', True))

    resolved: Count
    partial: Count
    unknown: Count


@dataclass(frozen=True)
class Stale(_Record):
    """Source files changed after the manifest was written."""

    _NAME: ClassVar[str] = "Stale"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('count', 'count', True), ('examples', 'examples', True))

    count: Count
    examples: list[Text]


@dataclass(frozen=True)
class RulesStatus(_Record):
    """The conformance rules behind the diagnostics. In Restricted Mode only the built-in rules run."""

    _NAME: ClassVar[str] = "RulesStatus"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('pack', 'pack', True), ('pack_loaded', 'packLoaded', True), ('restricted', 'restricted', True), ('problems', 'problems', True), ('findings', 'findings', True), ('score', 'score', True), ('coverage', 'coverage', True))

    pack: Text | None
    pack_loaded: bool
    restricted: bool
    problems: list[Text]
    findings: Count
    score: float | None
    coverage: float | None


@dataclass(frozen=True)
class Status(_Record):
    _NAME: ClassVar[str] = "Status"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('protocol_version', 'protocolVersion', True), ('server', 'server', True), ('project', 'project', True), ('trusted', 'trusted', True), ('state', 'state', True), ('indexing', 'indexing', True), ('generation', 'generation', True), ('column_lineage', 'columnLineage', True), ('message', 'message', True), ('errors', 'errors', True), ('error_count', 'errorCount', True), ('artifacts', 'artifacts', True), ('counts', 'counts', True), ('column_states', 'columnStates', True), ('stale', 'stale', True), ('from_cache', 'fromCache', True), ('indexed_at', 'indexedAt', True), ('rules', 'rules', True))

    protocol_version: int | None
    server: ServerInfo
    project: ProjectInfo
    trusted: bool
    state: IndexState
    indexing: bool
    generation: Count
    column_lineage: ColumnStage
    message: Text | None
    errors: list[IndexError]
    error_count: Count
    artifacts: Artifacts
    counts: Counts | None
    column_states: ColumnStates | None
    stale: Stale
    from_cache: bool
    indexed_at: Name | None
    rules: RulesStatus


RECORDS: dict[str, type[_Record]] = {
    "Position": Position,
    "Range": Range,
    "Location": Location,
    "Unknown": Unknown,
    "EmptyParams": EmptyParams,
    "GraphParams": GraphParams,
    "GraphNode": GraphNode,
    "GraphEdge": GraphEdge,
    "GraphResult": GraphResult,
    "ImpactParams": ImpactParams,
    "ImpactedNode": ImpactedNode,
    "ImpactResult": ImpactResult,
    "ExportParams": ExportParams,
    "ExportResult": ExportResult,
    "NodesForFileParams": NodesForFileParams,
    "NodesForFileResult": NodesForFileResult,
    "IndexError": IndexError,
    "ServerInfo": ServerInfo,
    "ProjectInfo": ProjectInfo,
    "ManifestInfo": ManifestInfo,
    "CatalogInfo": CatalogInfo,
    "Artifacts": Artifacts,
    "Counts": Counts,
    "ColumnStates": ColumnStates,
    "Stale": Stale,
    "RulesStatus": RulesStatus,
    "Status": Status,
}
