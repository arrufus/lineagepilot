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
    ],
    "lineage/node": [
        "NodeParams",
        "NodeResult"
    ],
    "lineage/reviewChanges": [
        "ReviewParams",
        "ReviewResult"
    ],
    "lineage/airflow": [
        "AirflowParams",
        "AirflowResult"
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
DEFS: dict[str, Any] = {'Id': {'type': 'string', 'minLength': 1, 'maxLength': 4096}, 'Name': {'type': 'string', 'maxLength': 4096}, 'Text': {'type': 'string', 'maxLength': 65536}, 'Uri': {'type': 'string', 'minLength': 1, 'maxLength': 8192}, 'Count': {'type': 'integer', 'minimum': 0}, 'NodeKind': {'enum': ['model', 'seed', 'snapshot', 'source', 'exposure', 'relation', 'airflow_dag', 'airflow_task', 'column']}, 'EdgeKind': {'enum': ['depends_on', 'derives', 'materialises', 'orchestrates', 'task_depends_on']}, 'Confidence': {'enum': ['declared', 'inferred', 'heuristic']}, 'Origin': {'enum': ['manifest', 'compiled_sql', 'catalog', 'dag_static', 'annotation']}, 'Direction': {'enum': ['upstream', 'downstream', 'both']}, 'Side': {'enum': ['root', 'upstream', 'downstream']}, 'Granularity': {'enum': ['model', 'column']}, 'LineageState': {'enum': ['resolved', 'partial', 'unknown']}, 'IndexState': {'enum': ['starting', 'ready', 'unknown', 'failed']}, 'ColumnStage': {'enum': ['pending', 'computing', 'done', 'off']}, 'ExportFormat': {'enum': ['json', 'mermaid', 'dot', 'graphml', 'cypher', 'pii-markdown', 'pii-csv']}, 'AirflowStage': {'enum': ['pending', 'scanning', 'done', 'failed']}, 'Position': {'type': 'object', 'properties': {'line': {'$ref': '#/$defs/Count'}, 'character': {'$ref': '#/$defs/Count'}}, 'required': ['line', 'character'], 'additionalProperties': False}, 'Range': {'type': 'object', 'properties': {'start': {'$ref': '#/$defs/Position'}, 'end': {'$ref': '#/$defs/Position'}}, 'required': ['start', 'end'], 'additionalProperties': False}, 'Location': {'description': 'A file to open, and where in it when known.', 'type': 'object', 'properties': {'uri': {'$ref': '#/$defs/Uri'}, 'range': {'anyOf': [{'$ref': '#/$defs/Range'}, {'type': 'null'}]}}, 'required': ['uri', 'range'], 'additionalProperties': False}, 'Unknown': {'description': 'A reference that could not be resolved, or a model whose column lineage is unknown. Shown, never dropped.', 'type': 'object', 'properties': {'subject': {'$ref': '#/$defs/Id'}, 'reference': {'$ref': '#/$defs/Text'}, 'reason': {'$ref': '#/$defs/Text'}}, 'required': ['subject', 'reference', 'reason'], 'additionalProperties': False}, 'EmptyParams': {'type': 'object', 'properties': {}, 'additionalProperties': False}, 'GraphParams': {'type': 'object', 'properties': {'root': {'$ref': '#/$defs/Id', 'description': 'A node ID, or a column ID at column granularity.'}, 'direction': {'$ref': '#/$defs/Direction'}, 'depth': {'type': 'integer', 'minimum': 1, 'maximum': 10}, 'granularity': {'$ref': '#/$defs/Granularity'}, 'maxNodes': {'type': 'integer', 'minimum': 1, 'maximum': 5000}}, 'required': ['root', 'direction', 'depth', 'granularity'], 'additionalProperties': False}, 'GraphNode': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'kind': {'$ref': '#/$defs/NodeKind'}, 'name': {'$ref': '#/$defs/Name'}, 'side': {'$ref': '#/$defs/Side'}, 'depth': {'$ref': '#/$defs/Count'}, 'parent': {'anyOf': [{'$ref': '#/$defs/Id'}, {'type': 'null'}], 'description': 'The node a column belongs to.'}, 'location': {'anyOf': [{'$ref': '#/$defs/Location'}, {'type': 'null'}]}, 'layer': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'materialization': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'description': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'columnLineage': {'anyOf': [{'$ref': '#/$defs/LineageState'}, {'type': 'null'}]}, 'columnLineageReason': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'pii': {'type': 'boolean'}}, 'required': ['id', 'kind', 'name', 'side', 'depth', 'parent', 'location', 'layer', 'materialization', 'owner', 'description', 'columnLineage', 'columnLineageReason', 'pii'], 'additionalProperties': False}, 'GraphEdge': {'type': 'object', 'properties': {'kind': {'$ref': '#/$defs/EdgeKind'}, 'upstream': {'$ref': '#/$defs/Id'}, 'downstream': {'$ref': '#/$defs/Id'}, 'relation': {'anyOf': [{'$ref': '#/$defs/Id'}, {'type': 'null'}], 'description': 'The physical table between a source and the seed, model or snapshot that builds it.'}, 'confidence': {'$ref': '#/$defs/Confidence'}, 'origins': {'type': 'array', 'items': {'$ref': '#/$defs/Origin'}, 'maxItems': 5}, 'transformation': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['kind', 'upstream', 'downstream', 'relation', 'confidence', 'origins', 'transformation'], 'additionalProperties': False}, 'GraphResult': {'type': 'object', 'properties': {'root': {'$ref': '#/$defs/Id'}, 'direction': {'$ref': '#/$defs/Direction'}, 'granularity': {'$ref': '#/$defs/Granularity'}, 'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/GraphNode'}}, 'edges': {'type': 'array', 'items': {'$ref': '#/$defs/GraphEdge'}}, 'truncated': {'type': 'boolean'}, 'unknowns': {'type': 'array', 'items': {'$ref': '#/$defs/Unknown'}}}, 'required': ['root', 'direction', 'granularity', 'nodes', 'edges', 'truncated', 'unknowns'], 'additionalProperties': False}, 'ImpactParams': {'type': 'object', 'properties': {'targets': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}, 'minItems': 1, 'maxItems': 200}, 'maxDepth': {'type': 'integer', 'minimum': 1, 'maximum': 100}}, 'required': ['targets'], 'additionalProperties': False}, 'ImpactedNode': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'kind': {'$ref': '#/$defs/NodeKind'}, 'name': {'$ref': '#/$defs/Name'}, 'depth': {'$ref': '#/$defs/Count'}, 'granularity': {'$ref': '#/$defs/Granularity'}, 'confidence': {'$ref': '#/$defs/Confidence'}, 'columns': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}}, 'reason': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'location': {'anyOf': [{'$ref': '#/$defs/Location'}, {'type': 'null'}]}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['id', 'kind', 'name', 'depth', 'granularity', 'confidence', 'columns', 'reason', 'location', 'owner'], 'additionalProperties': False}, 'ImpactResult': {'type': 'object', 'properties': {'targets': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}}, 'truncated': {'type': 'boolean'}, 'counts': {'type': 'object', 'additionalProperties': {'$ref': '#/$defs/Count'}}, 'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/ImpactedNode'}}, 'unknowns': {'type': 'array', 'items': {'$ref': '#/$defs/Unknown'}}, 'columnLineage': {'$ref': '#/$defs/ColumnStage', 'description': 'While computing, models not yet analysed are impacted at model level.'}}, 'required': ['targets', 'truncated', 'counts', 'nodes', 'unknowns', 'columnLineage'], 'additionalProperties': False}, 'ExportParams': {'type': 'object', 'properties': {'format': {'$ref': '#/$defs/ExportFormat'}, 'root': {'$ref': '#/$defs/Id', 'description': 'Export the neighbourhood of this node; the whole graph when absent.'}, 'direction': {'$ref': '#/$defs/Direction'}, 'depth': {'type': 'integer', 'minimum': 1, 'maximum': 10}}, 'required': ['format'], 'additionalProperties': False}, 'ExportResult': {'type': 'object', 'properties': {'format': {'$ref': '#/$defs/ExportFormat'}, 'filename': {'$ref': '#/$defs/Name'}, 'content': {'type': 'string'}, 'nodes': {'$ref': '#/$defs/Count'}, 'edges': {'$ref': '#/$defs/Count'}}, 'required': ['format', 'filename', 'content', 'nodes', 'edges'], 'additionalProperties': False}, 'NodesForFileParams': {'type': 'object', 'properties': {'uri': {'$ref': '#/$defs/Uri'}}, 'required': ['uri'], 'additionalProperties': False}, 'NodesForFileResult': {'type': 'object', 'properties': {'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/GraphNode'}}}, 'required': ['nodes'], 'additionalProperties': False}, 'NodeParams': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id', 'description': 'A node ID or a column ID.'}}, 'required': ['id'], 'additionalProperties': False}, 'ColumnInfo': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'name': {'$ref': '#/$defs/Name'}, 'dataType': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'description': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'pii': {'type': 'boolean'}, 'tests': {'$ref': '#/$defs/Count'}}, 'required': ['id', 'name', 'dataType', 'description', 'pii', 'tests'], 'additionalProperties': False}, 'NodeResult': {'description': 'One node or column: its details, its columns (for a node), and how many direct neighbours it has at its granularity.', 'type': 'object', 'properties': {'node': {'$ref': '#/$defs/GraphNode'}, 'columns': {'type': 'array', 'items': {'$ref': '#/$defs/ColumnInfo'}}, 'upstream': {'$ref': '#/$defs/Count'}, 'downstream': {'$ref': '#/$defs/Count'}}, 'required': ['node', 'columns', 'upstream', 'downstream'], 'additionalProperties': False}, 'FileStatus': {'enum': ['added', 'modified', 'deleted', 'renamed', 'untracked']}, 'ChangedFile': {'description': 'A file that differs from the base. Files outside the project are counted and otherwise ignored.', 'type': 'object', 'properties': {'uri': {'$ref': '#/$defs/Uri'}, 'status': {'$ref': '#/$defs/FileStatus'}, 'previousUri': {'anyOf': [{'$ref': '#/$defs/Uri'}, {'type': 'null'}], 'description': 'Where a renamed file was.'}}, 'required': ['uri', 'status', 'previousUri'], 'additionalProperties': False}, 'ReviewBaseline': {'description': 'What the changes are compared with: the ref asked for and the commit it came to, for the record, and a baseline manifest to compare columns with.', 'type': 'object', 'properties': {'ref': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'commit': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'manifestPath': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}], 'description': 'Absolute, or relative to the project. Null compares columns with catalog.json.'}}, 'required': ['ref', 'commit', 'manifestPath'], 'additionalProperties': False}, 'ReviewParams': {'type': 'object', 'properties': {'changedFiles': {'type': 'array', 'items': {'$ref': '#/$defs/ChangedFile'}, 'maxItems': 20000}, 'baseline': {'$ref': '#/$defs/ReviewBaseline'}}, 'required': ['changedFiles', 'baseline'], 'additionalProperties': False}, 'ColumnDiffState': {'enum': ['known', 'unknown', 'not_compared']}, 'BaselineKind': {'enum': ['baseline_manifest', 'catalog', 'none']}, 'ColumnTypeChange': {'type': 'object', 'properties': {'name': {'$ref': '#/$defs/Name'}, 'before': {'$ref': '#/$defs/Name'}, 'after': {'$ref': '#/$defs/Name'}}, 'required': ['name', 'before', 'after'], 'additionalProperties': False}, 'ColumnDiff': {'description': "A changed node's columns against a baseline, by name and declared type. known with nothing added or removed does not rule out a change in how a column is computed.", 'type': 'object', 'properties': {'state': {'$ref': '#/$defs/ColumnDiffState'}, 'baseline': {'$ref': '#/$defs/BaselineKind'}, 'added': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'removed': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'typeChanged': {'type': 'array', 'items': {'$ref': '#/$defs/ColumnTypeChange'}}, 'stale': {'type': 'boolean', 'description': 'The file changed after the manifest was written, so the comparison waits for dbt compile.'}, 'reason': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}}, 'required': ['state', 'baseline', 'added', 'removed', 'typeChanged', 'stale', 'reason'], 'additionalProperties': False}, 'ReviewFile': {'description': 'A changed file, and whether it defines the node or only describes it.', 'type': 'object', 'properties': {'path': {'$ref': '#/$defs/Text'}, 'status': {'$ref': '#/$defs/FileStatus'}, 'role': {'enum': ['definition', 'properties']}}, 'required': ['path', 'status', 'role'], 'additionalProperties': False}, 'ChangedNode': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'kind': {'$ref': '#/$defs/NodeKind'}, 'name': {'$ref': '#/$defs/Name'}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'status': {'enum': ['added', 'modified', 'deleted', 'renamed', 'untracked', 'properties']}, 'files': {'type': 'array', 'items': {'$ref': '#/$defs/ReviewFile'}}, 'columns': {'$ref': '#/$defs/ColumnDiff'}}, 'required': ['id', 'kind', 'name', 'owner', 'status', 'files', 'columns'], 'additionalProperties': False}, 'ImpactEvidence': {'description': 'One step that reached an impacted node or column, and what says so.', 'type': 'object', 'properties': {'source': {'$ref': '#/$defs/Id'}, 'target': {'$ref': '#/$defs/Id'}, 'confidence': {'$ref': '#/$defs/Confidence'}, 'origins': {'type': 'array', 'items': {'$ref': '#/$defs/Origin'}, 'maxItems': 5}}, 'required': ['source', 'target', 'confidence', 'origins'], 'additionalProperties': False}, 'ReviewImpacted': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'kind': {'$ref': '#/$defs/NodeKind'}, 'name': {'$ref': '#/$defs/Name'}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'file': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'depth': {'$ref': '#/$defs/Count'}, 'reach': {'$ref': '#/$defs/Granularity', 'description': 'column: traced from a changed column. model: only known to depend on a changed node.'}, 'confidence': {'$ref': '#/$defs/Confidence'}, 'origins': {'type': 'array', 'items': {'$ref': '#/$defs/Origin'}, 'maxItems': 5}, 'columns': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'reason': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'via': {'type': 'array', 'items': {'$ref': '#/$defs/ImpactEvidence'}, 'maxItems': 5}, 'viaMore': {'$ref': '#/$defs/Count'}, 'airflow': {'anyOf': [{'$ref': '#/$defs/ImpactAirflow'}, {'type': 'null'}], 'description': 'For an Airflow task or DAG: what it has to do with the change.'}}, 'required': ['id', 'kind', 'name', 'owner', 'file', 'depth', 'reach', 'confidence', 'origins', 'columns', 'reason', 'via', 'viaMore', 'airflow'], 'additionalProperties': False}, 'OwnerGroup': {'description': 'Impacted nodes by owner: meta.owner or group. A null owner holds the nodes nobody owns.', 'type': 'object', 'properties': {'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}}}, 'required': ['owner', 'nodes'], 'additionalProperties': False}, 'ImpactAirflow': {'description': 'How a task or DAG relates to a change. A Cosmos DAG or group is one link in the file; tasks holds the task ids derived for the nodes it runs.', 'type': 'object', 'properties': {'dag': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'operator': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'how': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'runs': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'runsMore': {'$ref': '#/$defs/Count'}, 'tasks': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'tasksMore': {'$ref': '#/$defs/Count'}, 'after': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'notes': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}}}, 'required': ['dag', 'operator', 'how', 'runs', 'runsMore', 'tasks', 'tasksMore', 'after', 'notes'], 'additionalProperties': False}, 'AirflowSummary': {'description': 'What a scan of the DAG files looked at and found. analysed is false when none was read: the review then says that, rather than that no task is impacted.', 'type': 'object', 'properties': {'analysed': {'type': 'boolean'}, 'reason': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'dagFiles': {'$ref': '#/$defs/Count'}, 'dags': {'$ref': '#/$defs/Count'}, 'tasks': {'$ref': '#/$defs/Count'}, 'links': {'$ref': '#/$defs/Count'}, 'unknowns': {'$ref': '#/$defs/Count'}, 'opaqueTasks': {'$ref': '#/$defs/Count', 'description': 'Tasks whose commands cannot be read and do not mention dbt: counted, not listed.'}, 'folders': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}}, 'problems': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}}}, 'required': ['analysed', 'reason', 'dagFiles', 'dags', 'tasks', 'links', 'unknowns', 'opaqueTasks', 'folders', 'problems'], 'additionalProperties': False}, 'AirflowParams': {'type': 'object', 'properties': {'nodes': {'anyOf': [{'type': 'array', 'items': {'$ref': '#/$defs/Id'}, 'maxItems': 200}, {'type': 'null'}], 'description': 'Only the tasks that run or read these nodes; null for every DAG and task.'}}, 'required': ['nodes'], 'additionalProperties': False}, 'AirflowTask': {'description': 'A task, or a DAG that is one link as a whole (a Cosmos DbtDag).', 'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'kind': {'$ref': '#/$defs/NodeKind'}, 'name': {'$ref': '#/$defs/Name'}, 'operator': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'how': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'nodes': {'type': 'array', 'items': {'$ref': '#/$defs/Id'}}, 'derivedTasks': {'type': 'array', 'items': {'$ref': '#/$defs/Name'}}, 'confidence': {'anyOf': [{'$ref': '#/$defs/Confidence'}, {'type': 'null'}]}, 'origins': {'type': 'array', 'items': {'$ref': '#/$defs/Origin'}, 'maxItems': 5}, 'notes': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}}, 'location': {'anyOf': [{'$ref': '#/$defs/Location'}, {'type': 'null'}]}}, 'required': ['id', 'kind', 'name', 'operator', 'owner', 'how', 'nodes', 'derivedTasks', 'confidence', 'origins', 'notes', 'location'], 'additionalProperties': False}, 'AirflowDag': {'type': 'object', 'properties': {'id': {'$ref': '#/$defs/Id'}, 'name': {'$ref': '#/$defs/Name'}, 'owner': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'location': {'anyOf': [{'$ref': '#/$defs/Location'}, {'type': 'null'}]}, 'tasks': {'type': 'array', 'items': {'$ref': '#/$defs/AirflowTask'}}}, 'required': ['id', 'name', 'owner', 'location', 'tasks'], 'additionalProperties': False}, 'AirflowResult': {'description': 'The DAGs and tasks touching some nodes, or all of them (the Airflow view).', 'type': 'object', 'properties': {'stage': {'$ref': '#/$defs/AirflowStage'}, 'summary': {'$ref': '#/$defs/AirflowSummary'}, 'dags': {'type': 'array', 'items': {'$ref': '#/$defs/AirflowDag'}}, 'unknowns': {'type': 'array', 'items': {'$ref': '#/$defs/Unknown'}}}, 'required': ['stage', 'summary', 'dags', 'unknowns'], 'additionalProperties': False}, 'AirflowStatus': {'type': 'object', 'properties': {'stage': {'$ref': '#/$defs/AirflowStage'}, 'generation': {'$ref': '#/$defs/Count', 'description': 'One per scan started.'}, 'summary': {'$ref': '#/$defs/AirflowSummary'}}, 'required': ['stage', 'generation', 'summary'], 'additionalProperties': False}, 'OtherFile': {'description': 'A changed file no node maps to, that cannot change one.', 'type': 'object', 'properties': {'path': {'$ref': '#/$defs/Text'}, 'status': {'$ref': '#/$defs/FileStatus'}}, 'required': ['path', 'status'], 'additionalProperties': False}, 'BaselineInfo': {'type': 'object', 'properties': {'name': {'$ref': '#/$defs/Name'}, 'sha256': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'generatedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'dbtVersion': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['name', 'sha256', 'generatedAt', 'dbtVersion'], 'additionalProperties': False}, 'ReviewArtifacts': {'type': 'object', 'properties': {'manifestGeneratedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'dbtVersion': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'baselineManifest': {'anyOf': [{'$ref': '#/$defs/BaselineInfo'}, {'type': 'null'}]}}, 'required': ['manifestGeneratedAt', 'dbtVersion', 'baselineManifest'], 'additionalProperties': False}, 'ReviewBase': {'type': 'object', 'properties': {'ref': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'commit': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['ref', 'commit'], 'additionalProperties': False}, 'ReviewSummary': {'type': 'object', 'properties': {'changedFiles': {'$ref': '#/$defs/Count'}, 'changedNodes': {'$ref': '#/$defs/Count'}, 'changedColumns': {'$ref': '#/$defs/Count'}, 'staleNodes': {'$ref': '#/$defs/Count'}, 'impacted': {'type': 'object', 'additionalProperties': {'$ref': '#/$defs/Count'}}, 'impactedByColumn': {'$ref': '#/$defs/Count'}, 'unknowns': {'$ref': '#/$defs/Count'}}, 'required': ['changedFiles', 'changedNodes', 'changedColumns', 'staleNodes', 'impacted', 'impactedByColumn', 'unknowns'], 'additionalProperties': False}, 'ReviewResult': {'description': 'A Change Impact Review (design section 8). The same inputs give the same result; locations aside, it is what lineagepilot review --json prints, and markdown is what it prints without --json.', 'type': 'object', 'properties': {'project': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'base': {'anyOf': [{'$ref': '#/$defs/ReviewBase'}, {'type': 'null'}]}, 'artifacts': {'$ref': '#/$defs/ReviewArtifacts'}, 'columnLineage': {'$ref': '#/$defs/ColumnStage'}, 'summary': {'$ref': '#/$defs/ReviewSummary'}, 'changedNodes': {'type': 'array', 'items': {'$ref': '#/$defs/ChangedNode'}}, 'impacted': {'type': 'array', 'items': {'$ref': '#/$defs/ReviewImpacted'}}, 'owners': {'type': 'array', 'items': {'$ref': '#/$defs/OwnerGroup'}}, 'airflow': {'$ref': '#/$defs/AirflowSummary'}, 'unknowns': {'type': 'array', 'items': {'$ref': '#/$defs/Unknown'}}, 'otherFiles': {'type': 'array', 'items': {'$ref': '#/$defs/OtherFile'}}, 'outsideProject': {'$ref': '#/$defs/Count'}, 'notes': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}}, 'markdown': {'$ref': '#/$defs/Text'}, 'locations': {'type': 'object', 'additionalProperties': {'$ref': '#/$defs/Location'}, 'description': 'Where to open each node, by ID. Only the language server adds this: it differs per machine.'}}, 'required': ['project', 'base', 'artifacts', 'columnLineage', 'summary', 'changedNodes', 'impacted', 'owners', 'airflow', 'unknowns', 'otherFiles', 'outsideProject', 'notes', 'markdown', 'locations'], 'additionalProperties': False}, 'IndexError': {'type': 'object', 'properties': {'severity': {'enum': ['error', 'warning']}, 'message': {'$ref': '#/$defs/Text'}, 'location': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}}, 'required': ['severity', 'message', 'location'], 'additionalProperties': False}, 'ServerInfo': {'type': 'object', 'properties': {'version': {'$ref': '#/$defs/Name'}, 'python': {'$ref': '#/$defs/Name'}, 'executable': {'$ref': '#/$defs/Text'}, 'pid': {'$ref': '#/$defs/Count'}, 'isolated': {'type': 'boolean', 'description': "Started with python -I, so nothing from the workspace or the user's site-packages can shadow the bundle."}, 'modules': {'type': 'object', 'additionalProperties': {'$ref': '#/$defs/Text'}, 'description': 'Where the key libraries were imported from, to check that the bundle is what runs.'}}, 'required': ['version', 'python', 'executable', 'pid', 'isolated', 'modules'], 'additionalProperties': False}, 'ProjectInfo': {'type': 'object', 'properties': {'root': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'name': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'problems': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}, 'maxItems': 100}}, 'required': ['root', 'name', 'problems'], 'additionalProperties': False}, 'ManifestInfo': {'type': 'object', 'properties': {'path': {'$ref': '#/$defs/Text'}, 'exists': {'type': 'boolean'}, 'generatedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'dbtVersion': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'schemaVersion': {'anyOf': [{'type': 'integer'}, {'type': 'null'}]}, 'modifiedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['path', 'exists', 'generatedAt', 'dbtVersion', 'schemaVersion', 'modifiedAt'], 'additionalProperties': False}, 'CatalogInfo': {'type': 'object', 'properties': {'path': {'$ref': '#/$defs/Text'}, 'exists': {'type': 'boolean'}, 'modifiedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}}, 'required': ['path', 'exists', 'modifiedAt'], 'additionalProperties': False}, 'Artifacts': {'type': 'object', 'properties': {'manifest': {'anyOf': [{'$ref': '#/$defs/ManifestInfo'}, {'type': 'null'}]}, 'catalog': {'anyOf': [{'$ref': '#/$defs/CatalogInfo'}, {'type': 'null'}]}}, 'required': ['manifest', 'catalog'], 'additionalProperties': False}, 'Counts': {'type': 'object', 'properties': {'nodes': {'$ref': '#/$defs/Count'}, 'columns': {'$ref': '#/$defs/Count'}, 'edges': {'$ref': '#/$defs/Count'}, 'unresolved': {'$ref': '#/$defs/Count'}, 'nodeKinds': {'type': 'object', 'additionalProperties': {'$ref': '#/$defs/Count'}}}, 'required': ['nodes', 'columns', 'edges', 'unresolved', 'nodeKinds'], 'additionalProperties': False}, 'ColumnStates': {'type': 'object', 'properties': {'resolved': {'$ref': '#/$defs/Count'}, 'partial': {'$ref': '#/$defs/Count'}, 'unknown': {'$ref': '#/$defs/Count'}}, 'required': ['resolved', 'partial', 'unknown'], 'additionalProperties': False}, 'Stale': {'description': 'Source files changed after the manifest was written.', 'type': 'object', 'properties': {'count': {'$ref': '#/$defs/Count'}, 'examples': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}, 'maxItems': 5}}, 'required': ['count', 'examples'], 'additionalProperties': False}, 'RulesStatus': {'description': 'The conformance rules behind the diagnostics. In Restricted Mode only the built-in rules run.', 'type': 'object', 'properties': {'pack': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}], 'description': 'The rule pack file, if the project has one.'}, 'packLoaded': {'type': 'boolean'}, 'restricted': {'type': 'boolean'}, 'problems': {'type': 'array', 'items': {'$ref': '#/$defs/Text'}, 'maxItems': 50}, 'findings': {'$ref': '#/$defs/Count'}, 'score': {'anyOf': [{'type': 'number', 'minimum': 0, 'maximum': 100}, {'type': 'null'}]}, 'coverage': {'anyOf': [{'type': 'number', 'minimum': 0, 'maximum': 100}, {'type': 'null'}]}}, 'required': ['pack', 'packLoaded', 'restricted', 'problems', 'findings', 'score', 'coverage'], 'additionalProperties': False}, 'Status': {'type': 'object', 'properties': {'protocolVersion': {'anyOf': [{'type': 'integer'}, {'type': 'null'}]}, 'server': {'$ref': '#/$defs/ServerInfo'}, 'project': {'$ref': '#/$defs/ProjectInfo'}, 'trusted': {'type': 'boolean'}, 'state': {'$ref': '#/$defs/IndexState'}, 'indexing': {'type': 'boolean'}, 'generation': {'$ref': '#/$defs/Count'}, 'columnLineage': {'$ref': '#/$defs/ColumnStage'}, 'message': {'anyOf': [{'$ref': '#/$defs/Text'}, {'type': 'null'}]}, 'errors': {'type': 'array', 'items': {'$ref': '#/$defs/IndexError'}, 'maxItems': 50}, 'errorCount': {'$ref': '#/$defs/Count'}, 'artifacts': {'$ref': '#/$defs/Artifacts'}, 'counts': {'anyOf': [{'$ref': '#/$defs/Counts'}, {'type': 'null'}]}, 'columnStates': {'anyOf': [{'$ref': '#/$defs/ColumnStates'}, {'type': 'null'}]}, 'stale': {'$ref': '#/$defs/Stale'}, 'fromCache': {'type': 'boolean'}, 'indexedAt': {'anyOf': [{'$ref': '#/$defs/Name'}, {'type': 'null'}]}, 'rules': {'$ref': '#/$defs/RulesStatus'}, 'airflow': {'$ref': '#/$defs/AirflowStatus'}}, 'required': ['protocolVersion', 'server', 'project', 'trusted', 'state', 'indexing', 'generation', 'columnLineage', 'message', 'errors', 'errorCount', 'artifacts', 'counts', 'columnStates', 'stale', 'fromCache', 'indexedAt', 'rules', 'airflow'], 'additionalProperties': False}}

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

AirflowStage = Literal["pending", "scanning", "done", "failed"]

FileStatus = Literal["added", "modified", "deleted", "renamed", "untracked"]

ColumnDiffState = Literal["known", "unknown", "not_compared"]

BaselineKind = Literal["baseline_manifest", "catalog", "none"]


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
class NodeParams(_Record):
    _NAME: ClassVar[str] = "NodeParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True),)

    id: Id


@dataclass(frozen=True)
class ColumnInfo(_Record):
    _NAME: ClassVar[str] = "ColumnInfo"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True), ('name', 'name', True), ('data_type', 'dataType', True), ('description', 'description', True), ('pii', 'pii', True), ('tests', 'tests', True))

    id: Id
    name: Name
    data_type: Name | None
    description: Text | None
    pii: bool
    tests: Count


@dataclass(frozen=True)
class NodeResult(_Record):
    """One node or column: its details, its columns (for a node), and how many direct neighbours it has at its granularity."""

    _NAME: ClassVar[str] = "NodeResult"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('node', 'node', True), ('columns', 'columns', True), ('upstream', 'upstream', True), ('downstream', 'downstream', True))

    node: GraphNode
    columns: list[ColumnInfo]
    upstream: Count
    downstream: Count


@dataclass(frozen=True)
class ChangedFile(_Record):
    """A file that differs from the base. Files outside the project are counted and otherwise ignored."""

    _NAME: ClassVar[str] = "ChangedFile"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('uri', 'uri', True), ('status', 'status', True), ('previous_uri', 'previousUri', True))

    uri: Uri
    status: FileStatus
    previous_uri: Uri | None


@dataclass(frozen=True)
class ReviewBaseline(_Record):
    """What the changes are compared with: the ref asked for and the commit it came to, for the record, and a baseline manifest to compare columns with."""

    _NAME: ClassVar[str] = "ReviewBaseline"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('ref', 'ref', True), ('commit', 'commit', True), ('manifest_path', 'manifestPath', True))

    ref: Name | None
    commit: Name | None
    manifest_path: Text | None


@dataclass(frozen=True)
class ReviewParams(_Record):
    _NAME: ClassVar[str] = "ReviewParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('changed_files', 'changedFiles', True), ('baseline', 'baseline', True))

    changed_files: list[ChangedFile]
    baseline: ReviewBaseline


@dataclass(frozen=True)
class ColumnTypeChange(_Record):
    _NAME: ClassVar[str] = "ColumnTypeChange"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('name', 'name', True), ('before', 'before', True), ('after', 'after', True))

    name: Name
    before: Name
    after: Name


@dataclass(frozen=True)
class ColumnDiff(_Record):
    """A changed node's columns against a baseline, by name and declared type. known with nothing added or removed does not rule out a change in how a column is computed."""

    _NAME: ClassVar[str] = "ColumnDiff"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('state', 'state', True), ('baseline', 'baseline', True), ('added', 'added', True), ('removed', 'removed', True), ('type_changed', 'typeChanged', True), ('stale', 'stale', True), ('reason', 'reason', True))

    state: ColumnDiffState
    baseline: BaselineKind
    added: list[Name]
    removed: list[Name]
    type_changed: list[ColumnTypeChange]
    stale: bool
    reason: Text | None


@dataclass(frozen=True)
class ReviewFile(_Record):
    """A changed file, and whether it defines the node or only describes it."""

    _NAME: ClassVar[str] = "ReviewFile"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('path', 'path', True), ('status', 'status', True), ('role', 'role', True))

    path: Text
    status: FileStatus
    role: Literal["definition", "properties"]


@dataclass(frozen=True)
class ChangedNode(_Record):
    _NAME: ClassVar[str] = "ChangedNode"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True), ('kind', 'kind', True), ('name', 'name', True), ('owner', 'owner', True), ('status', 'status', True), ('files', 'files', True), ('columns', 'columns', True))

    id: Id
    kind: NodeKind
    name: Name
    owner: Name | None
    status: Literal["added", "modified", "deleted", "renamed", "untracked", "properties"]
    files: list[ReviewFile]
    columns: ColumnDiff


@dataclass(frozen=True)
class ImpactEvidence(_Record):
    """One step that reached an impacted node or column, and what says so."""

    _NAME: ClassVar[str] = "ImpactEvidence"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('source', 'source', True), ('target', 'target', True), ('confidence', 'confidence', True), ('origins', 'origins', True))

    source: Id
    target: Id
    confidence: Confidence
    origins: list[Origin]


@dataclass(frozen=True)
class ImpactAirflow(_Record):
    """How a task or DAG relates to a change. A Cosmos DAG or group is one link in the file; tasks holds the task ids derived for the nodes it runs."""

    _NAME: ClassVar[str] = "ImpactAirflow"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('dag', 'dag', True), ('operator', 'operator', True), ('how', 'how', True), ('runs', 'runs', True), ('runs_more', 'runsMore', True), ('tasks', 'tasks', True), ('tasks_more', 'tasksMore', True), ('after', 'after', True), ('notes', 'notes', True))

    dag: Name | None
    operator: Name | None
    how: list[Name]
    runs: list[Name]
    runs_more: Count
    tasks: list[Name]
    tasks_more: Count
    after: list[Name]
    notes: list[Text]


@dataclass(frozen=True)
class ReviewImpacted(_Record):
    _NAME: ClassVar[str] = "ReviewImpacted"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True), ('kind', 'kind', True), ('name', 'name', True), ('owner', 'owner', True), ('file', 'file', True), ('depth', 'depth', True), ('reach', 'reach', True), ('confidence', 'confidence', True), ('origins', 'origins', True), ('columns', 'columns', True), ('reason', 'reason', True), ('via', 'via', True), ('via_more', 'viaMore', True), ('airflow', 'airflow', True))

    id: Id
    kind: NodeKind
    name: Name
    owner: Name | None
    file: Text | None
    depth: Count
    reach: Granularity
    confidence: Confidence
    origins: list[Origin]
    columns: list[Name]
    reason: Text | None
    via: list[ImpactEvidence]
    via_more: Count
    airflow: ImpactAirflow | None


@dataclass(frozen=True)
class OwnerGroup(_Record):
    """Impacted nodes by owner: meta.owner or group. A null owner holds the nodes nobody owns."""

    _NAME: ClassVar[str] = "OwnerGroup"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('owner', 'owner', True), ('nodes', 'nodes', True))

    owner: Name | None
    nodes: list[Id]


@dataclass(frozen=True)
class AirflowSummary(_Record):
    """What a scan of the DAG files looked at and found. analysed is false when none was read: the review then says that, rather than that no task is impacted."""

    _NAME: ClassVar[str] = "AirflowSummary"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('analysed', 'analysed', True), ('reason', 'reason', True), ('dag_files', 'dagFiles', True), ('dags', 'dags', True), ('tasks', 'tasks', True), ('links', 'links', True), ('unknowns', 'unknowns', True), ('opaque_tasks', 'opaqueTasks', True), ('folders', 'folders', True), ('problems', 'problems', True))

    analysed: bool
    reason: Text | None
    dag_files: Count
    dags: Count
    tasks: Count
    links: Count
    unknowns: Count
    opaque_tasks: Count
    folders: list[Text]
    problems: list[Text]


@dataclass(frozen=True)
class AirflowParams(_Record):
    _NAME: ClassVar[str] = "AirflowParams"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('nodes', 'nodes', True),)

    nodes: list[Id] | None


@dataclass(frozen=True)
class AirflowTask(_Record):
    """A task, or a DAG that is one link as a whole (a Cosmos DbtDag)."""

    _NAME: ClassVar[str] = "AirflowTask"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True), ('kind', 'kind', True), ('name', 'name', True), ('operator', 'operator', True), ('owner', 'owner', True), ('how', 'how', True), ('nodes', 'nodes', True), ('derived_tasks', 'derivedTasks', True), ('confidence', 'confidence', True), ('origins', 'origins', True), ('notes', 'notes', True), ('location', 'location', True))

    id: Id
    kind: NodeKind
    name: Name
    operator: Name | None
    owner: Name | None
    how: list[Name]
    nodes: list[Id]
    derived_tasks: list[Name]
    confidence: Confidence | None
    origins: list[Origin]
    notes: list[Text]
    location: Location | None


@dataclass(frozen=True)
class AirflowDag(_Record):
    _NAME: ClassVar[str] = "AirflowDag"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('id', 'id', True), ('name', 'name', True), ('owner', 'owner', True), ('location', 'location', True), ('tasks', 'tasks', True))

    id: Id
    name: Name
    owner: Name | None
    location: Location | None
    tasks: list[AirflowTask]


@dataclass(frozen=True)
class AirflowResult(_Record):
    """The DAGs and tasks touching some nodes, or all of them (the Airflow view)."""

    _NAME: ClassVar[str] = "AirflowResult"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('stage', 'stage', True), ('summary', 'summary', True), ('dags', 'dags', True), ('unknowns', 'unknowns', True))

    stage: AirflowStage
    summary: AirflowSummary
    dags: list[AirflowDag]
    unknowns: list[Unknown]


@dataclass(frozen=True)
class AirflowStatus(_Record):
    _NAME: ClassVar[str] = "AirflowStatus"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('stage', 'stage', True), ('generation', 'generation', True), ('summary', 'summary', True))

    stage: AirflowStage
    generation: Count
    summary: AirflowSummary


@dataclass(frozen=True)
class OtherFile(_Record):
    """A changed file no node maps to, that cannot change one."""

    _NAME: ClassVar[str] = "OtherFile"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('path', 'path', True), ('status', 'status', True))

    path: Text
    status: FileStatus


@dataclass(frozen=True)
class BaselineInfo(_Record):
    _NAME: ClassVar[str] = "BaselineInfo"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('name', 'name', True), ('sha256', 'sha256', True), ('generated_at', 'generatedAt', True), ('dbt_version', 'dbtVersion', True))

    name: Name
    sha256: Name | None
    generated_at: Name | None
    dbt_version: Name | None


@dataclass(frozen=True)
class ReviewArtifacts(_Record):
    _NAME: ClassVar[str] = "ReviewArtifacts"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('manifest_generated_at', 'manifestGeneratedAt', True), ('dbt_version', 'dbtVersion', True), ('baseline_manifest', 'baselineManifest', True))

    manifest_generated_at: Name | None
    dbt_version: Name | None
    baseline_manifest: BaselineInfo | None


@dataclass(frozen=True)
class ReviewBase(_Record):
    _NAME: ClassVar[str] = "ReviewBase"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('ref', 'ref', True), ('commit', 'commit', True))

    ref: Name | None
    commit: Name | None


@dataclass(frozen=True)
class ReviewSummary(_Record):
    _NAME: ClassVar[str] = "ReviewSummary"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('changed_files', 'changedFiles', True), ('changed_nodes', 'changedNodes', True), ('changed_columns', 'changedColumns', True), ('stale_nodes', 'staleNodes', True), ('impacted', 'impacted', True), ('impacted_by_column', 'impactedByColumn', True), ('unknowns', 'unknowns', True))

    changed_files: Count
    changed_nodes: Count
    changed_columns: Count
    stale_nodes: Count
    impacted: dict[str, Count]
    impacted_by_column: Count
    unknowns: Count


@dataclass(frozen=True)
class ReviewResult(_Record):
    """A Change Impact Review (design section 8). The same inputs give the same result; locations aside, it is what lineagepilot review --json prints, and markdown is what it prints without --json."""

    _NAME: ClassVar[str] = "ReviewResult"
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('project', 'project', True), ('base', 'base', True), ('artifacts', 'artifacts', True), ('column_lineage', 'columnLineage', True), ('summary', 'summary', True), ('changed_nodes', 'changedNodes', True), ('impacted', 'impacted', True), ('owners', 'owners', True), ('airflow', 'airflow', True), ('unknowns', 'unknowns', True), ('other_files', 'otherFiles', True), ('outside_project', 'outsideProject', True), ('notes', 'notes', True), ('markdown', 'markdown', True), ('locations', 'locations', True))

    project: Name | None
    base: ReviewBase | None
    artifacts: ReviewArtifacts
    column_lineage: ColumnStage
    summary: ReviewSummary
    changed_nodes: list[ChangedNode]
    impacted: list[ReviewImpacted]
    owners: list[OwnerGroup]
    airflow: AirflowSummary
    unknowns: list[Unknown]
    other_files: list[OtherFile]
    outside_project: Count
    notes: list[Text]
    markdown: Text
    locations: dict[str, Location]


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
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('version', 'version', True), ('python', 'python', True), ('executable', 'executable', True), ('pid', 'pid', True), ('isolated', 'isolated', True), ('modules', 'modules', True))

    version: Name
    python: Name
    executable: Text
    pid: Count
    isolated: bool
    modules: dict[str, Text]


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
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = (('protocol_version', 'protocolVersion', True), ('server', 'server', True), ('project', 'project', True), ('trusted', 'trusted', True), ('state', 'state', True), ('indexing', 'indexing', True), ('generation', 'generation', True), ('column_lineage', 'columnLineage', True), ('message', 'message', True), ('errors', 'errors', True), ('error_count', 'errorCount', True), ('artifacts', 'artifacts', True), ('counts', 'counts', True), ('column_states', 'columnStates', True), ('stale', 'stale', True), ('from_cache', 'fromCache', True), ('indexed_at', 'indexedAt', True), ('rules', 'rules', True), ('airflow', 'airflow', True))

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
    airflow: AirflowStatus


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
    "NodeParams": NodeParams,
    "ColumnInfo": ColumnInfo,
    "NodeResult": NodeResult,
    "ChangedFile": ChangedFile,
    "ReviewBaseline": ReviewBaseline,
    "ReviewParams": ReviewParams,
    "ColumnTypeChange": ColumnTypeChange,
    "ColumnDiff": ColumnDiff,
    "ReviewFile": ReviewFile,
    "ChangedNode": ChangedNode,
    "ImpactEvidence": ImpactEvidence,
    "ImpactAirflow": ImpactAirflow,
    "ReviewImpacted": ReviewImpacted,
    "OwnerGroup": OwnerGroup,
    "AirflowSummary": AirflowSummary,
    "AirflowParams": AirflowParams,
    "AirflowTask": AirflowTask,
    "AirflowDag": AirflowDag,
    "AirflowResult": AirflowResult,
    "AirflowStatus": AirflowStatus,
    "OtherFile": OtherFile,
    "BaselineInfo": BaselineInfo,
    "ReviewArtifacts": ReviewArtifacts,
    "ReviewBase": ReviewBase,
    "ReviewSummary": ReviewSummary,
    "ReviewResult": ReviewResult,
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
