// Generated from protocol/lineage-protocol.schema.json by protocol/generate.py. Do not edit.
// Validates everything the language server sends to the extension (design section 9).
import * as z from 'zod';

export const PROTOCOL_VERSION = 1;

export const ErrorCode = {
  NotReady: -32001,
  UnknownNode: -32002,
  ProtocolMismatch: -32003,
} as const;

export const Id = z.string().min(1).max(4096);
export type Id = z.infer<typeof Id>;

export const Name = z.string().max(4096);
export type Name = z.infer<typeof Name>;

export const Text = z.string().max(65536);
export type Text = z.infer<typeof Text>;

export const Uri = z.string().min(1).max(8192);
export type Uri = z.infer<typeof Uri>;

export const Count = z.int().min(0);
export type Count = z.infer<typeof Count>;

export const NodeKind = z.enum(["model", "seed", "snapshot", "source", "exposure", "relation", "airflow_dag", "airflow_task", "column"]);
export type NodeKind = z.infer<typeof NodeKind>;

export const EdgeKind = z.enum(["depends_on", "derives", "materialises", "orchestrates", "task_depends_on"]);
export type EdgeKind = z.infer<typeof EdgeKind>;

export const Confidence = z.enum(["declared", "inferred", "heuristic"]);
export type Confidence = z.infer<typeof Confidence>;

export const Origin = z.enum(["manifest", "compiled_sql", "catalog", "dag_static", "annotation"]);
export type Origin = z.infer<typeof Origin>;

export const Direction = z.enum(["upstream", "downstream", "both"]);
export type Direction = z.infer<typeof Direction>;

export const Side = z.enum(["root", "upstream", "downstream"]);
export type Side = z.infer<typeof Side>;

export const Granularity = z.enum(["model", "column"]);
export type Granularity = z.infer<typeof Granularity>;

export const LineageState = z.enum(["resolved", "partial", "unknown"]);
export type LineageState = z.infer<typeof LineageState>;

export const IndexState = z.enum(["starting", "ready", "unknown", "failed"]);
export type IndexState = z.infer<typeof IndexState>;

export const ColumnStage = z.enum(["pending", "computing", "done", "off"]);
export type ColumnStage = z.infer<typeof ColumnStage>;

export const ExportFormat = z.enum(["json", "mermaid", "dot", "graphml", "cypher", "pii-markdown", "pii-csv"]);
export type ExportFormat = z.infer<typeof ExportFormat>;

export const AirflowStage = z.enum(["pending", "scanning", "done", "failed"]);
export type AirflowStage = z.infer<typeof AirflowStage>;

export const Position = z.strictObject({
  line: Count,
  character: Count,
});
export type Position = z.infer<typeof Position>;

export const Range = z.strictObject({
  start: Position,
  end: Position,
});
export type Range = z.infer<typeof Range>;

/** A file to open, and where in it when known. */
export const Location = z.strictObject({
  uri: Uri,
  range: Range.nullable(),
});
export type Location = z.infer<typeof Location>;

/** A reference that could not be resolved, or a model whose column lineage is unknown. Shown, never dropped. */
export const Unknown = z.strictObject({
  subject: Id,
  reference: Text,
  reason: Text,
});
export type Unknown = z.infer<typeof Unknown>;

export const EmptyParams = z.strictObject({});
export type EmptyParams = z.infer<typeof EmptyParams>;

export const GraphParams = z.strictObject({
  root: Id,
  direction: Direction,
  depth: z.int().min(1).max(10),
  granularity: Granularity,
  maxNodes: z.int().min(1).max(5000).optional(),
});
export type GraphParams = z.infer<typeof GraphParams>;

export const GraphNode = z.strictObject({
  id: Id,
  kind: NodeKind,
  name: Name,
  side: Side,
  depth: Count,
  parent: Id.nullable(),
  location: Location.nullable(),
  layer: Name.nullable(),
  materialization: Name.nullable(),
  owner: Name.nullable(),
  description: Text.nullable(),
  columnLineage: LineageState.nullable(),
  columnLineageReason: Text.nullable(),
  pii: z.boolean(),
});
export type GraphNode = z.infer<typeof GraphNode>;

export const GraphEdge = z.strictObject({
  kind: EdgeKind,
  upstream: Id,
  downstream: Id,
  relation: Id.nullable(),
  confidence: Confidence,
  origins: z.array(Origin).max(5),
  transformation: Name.nullable(),
});
export type GraphEdge = z.infer<typeof GraphEdge>;

export const GraphResult = z.strictObject({
  root: Id,
  direction: Direction,
  granularity: Granularity,
  nodes: z.array(GraphNode),
  edges: z.array(GraphEdge),
  truncated: z.boolean(),
  unknowns: z.array(Unknown),
});
export type GraphResult = z.infer<typeof GraphResult>;

export const ImpactParams = z.strictObject({
  targets: z.array(Id).min(1).max(200),
  maxDepth: z.int().min(1).max(100).optional(),
});
export type ImpactParams = z.infer<typeof ImpactParams>;

export const ImpactedNode = z.strictObject({
  id: Id,
  kind: NodeKind,
  name: Name,
  depth: Count,
  granularity: Granularity,
  confidence: Confidence,
  columns: z.array(Id),
  reason: Text.nullable(),
  location: Location.nullable(),
  owner: Name.nullable(),
});
export type ImpactedNode = z.infer<typeof ImpactedNode>;

export const ImpactResult = z.strictObject({
  targets: z.array(Id),
  truncated: z.boolean(),
  counts: z.record(z.string(), Count),
  nodes: z.array(ImpactedNode),
  unknowns: z.array(Unknown),
  columnLineage: ColumnStage,
});
export type ImpactResult = z.infer<typeof ImpactResult>;

export const ExportParams = z.strictObject({
  format: ExportFormat,
  root: Id.optional(),
  direction: Direction.optional(),
  depth: z.int().min(1).max(10).optional(),
});
export type ExportParams = z.infer<typeof ExportParams>;

export const ExportResult = z.strictObject({
  format: ExportFormat,
  filename: Name,
  content: z.string(),
  nodes: Count,
  edges: Count,
});
export type ExportResult = z.infer<typeof ExportResult>;

export const NodesForFileParams = z.strictObject({
  uri: Uri,
});
export type NodesForFileParams = z.infer<typeof NodesForFileParams>;

export const NodesForFileResult = z.strictObject({
  nodes: z.array(GraphNode),
});
export type NodesForFileResult = z.infer<typeof NodesForFileResult>;

export const NodeParams = z.strictObject({
  id: Id,
});
export type NodeParams = z.infer<typeof NodeParams>;

export const ColumnInfo = z.strictObject({
  id: Id,
  name: Name,
  dataType: Name.nullable(),
  description: Text.nullable(),
  pii: z.boolean(),
  tests: Count,
});
export type ColumnInfo = z.infer<typeof ColumnInfo>;

/** One node or column: its details, its columns (for a node), and how many direct neighbours it has at its granularity. */
export const NodeResult = z.strictObject({
  node: GraphNode,
  columns: z.array(ColumnInfo),
  upstream: Count,
  downstream: Count,
});
export type NodeResult = z.infer<typeof NodeResult>;

export const FileStatus = z.enum(["added", "modified", "deleted", "renamed", "untracked"]);
export type FileStatus = z.infer<typeof FileStatus>;

/** A file that differs from the base. Files outside the project are counted and otherwise ignored. */
export const ChangedFile = z.strictObject({
  uri: Uri,
  status: FileStatus,
  previousUri: Uri.nullable(),
});
export type ChangedFile = z.infer<typeof ChangedFile>;

/** What the changes are compared with: the ref asked for and the commit it came to, for the record, and a baseline manifest to compare columns with. */
export const ReviewBaseline = z.strictObject({
  ref: Name.nullable(),
  commit: Name.nullable(),
  manifestPath: Text.nullable(),
});
export type ReviewBaseline = z.infer<typeof ReviewBaseline>;

export const ReviewParams = z.strictObject({
  changedFiles: z.array(ChangedFile).max(20000),
  baseline: ReviewBaseline,
});
export type ReviewParams = z.infer<typeof ReviewParams>;

export const ColumnDiffState = z.enum(["known", "unknown", "not_compared"]);
export type ColumnDiffState = z.infer<typeof ColumnDiffState>;

export const BaselineKind = z.enum(["baseline_manifest", "catalog", "none"]);
export type BaselineKind = z.infer<typeof BaselineKind>;

export const ColumnTypeChange = z.strictObject({
  name: Name,
  before: Name,
  after: Name,
});
export type ColumnTypeChange = z.infer<typeof ColumnTypeChange>;

/** A changed node's columns against a baseline, by name and declared type. known with nothing added or removed does not rule out a change in how a column is computed. */
export const ColumnDiff = z.strictObject({
  state: ColumnDiffState,
  baseline: BaselineKind,
  added: z.array(Name),
  removed: z.array(Name),
  typeChanged: z.array(ColumnTypeChange),
  stale: z.boolean(),
  reason: Text.nullable(),
});
export type ColumnDiff = z.infer<typeof ColumnDiff>;

/** A changed file, and whether it defines the node or only describes it. */
export const ReviewFile = z.strictObject({
  path: Text,
  status: FileStatus,
  role: z.enum(["definition", "properties"]),
});
export type ReviewFile = z.infer<typeof ReviewFile>;

export const ChangedNode = z.strictObject({
  id: Id,
  kind: NodeKind,
  name: Name,
  owner: Name.nullable(),
  status: z.enum(["added", "modified", "deleted", "renamed", "untracked", "properties"]),
  files: z.array(ReviewFile),
  columns: ColumnDiff,
});
export type ChangedNode = z.infer<typeof ChangedNode>;

/** One step that reached an impacted node or column, and what says so. */
export const ImpactEvidence = z.strictObject({
  source: Id,
  target: Id,
  confidence: Confidence,
  origins: z.array(Origin).max(5),
});
export type ImpactEvidence = z.infer<typeof ImpactEvidence>;

/** How a task or DAG relates to a change. A Cosmos DAG or group is one link in the file; tasks holds the task ids derived for the nodes it runs. */
export const ImpactAirflow = z.strictObject({
  dag: Name.nullable(),
  operator: Name.nullable(),
  how: z.array(Name),
  runs: z.array(Name),
  runsMore: Count,
  tasks: z.array(Name),
  tasksMore: Count,
  after: z.array(Name),
  notes: z.array(Text),
});
export type ImpactAirflow = z.infer<typeof ImpactAirflow>;

export const ReviewImpacted = z.strictObject({
  id: Id,
  kind: NodeKind,
  name: Name,
  owner: Name.nullable(),
  file: Text.nullable(),
  depth: Count,
  reach: Granularity,
  confidence: Confidence,
  origins: z.array(Origin).max(5),
  columns: z.array(Name),
  reason: Text.nullable(),
  via: z.array(ImpactEvidence).max(5),
  viaMore: Count,
  airflow: ImpactAirflow.nullable(),
});
export type ReviewImpacted = z.infer<typeof ReviewImpacted>;

/** Impacted nodes by owner: meta.owner or group. A null owner holds the nodes nobody owns. */
export const OwnerGroup = z.strictObject({
  owner: Name.nullable(),
  nodes: z.array(Id),
});
export type OwnerGroup = z.infer<typeof OwnerGroup>;

/** What a scan of the DAG files looked at and found. analysed is false when none was read: the review then says that, rather than that no task is impacted. */
export const AirflowSummary = z.strictObject({
  analysed: z.boolean(),
  reason: Text.nullable(),
  dagFiles: Count,
  dags: Count,
  tasks: Count,
  links: Count,
  unknowns: Count,
  opaqueTasks: Count,
  folders: z.array(Text),
  problems: z.array(Text),
});
export type AirflowSummary = z.infer<typeof AirflowSummary>;

export const AirflowParams = z.strictObject({
  nodes: z.array(Id).max(200).nullable(),
});
export type AirflowParams = z.infer<typeof AirflowParams>;

/** A task, or a DAG that is one link as a whole (a Cosmos DbtDag). */
export const AirflowTask = z.strictObject({
  id: Id,
  kind: NodeKind,
  name: Name,
  operator: Name.nullable(),
  owner: Name.nullable(),
  how: z.array(Name),
  nodes: z.array(Id),
  derivedTasks: z.array(Name),
  confidence: Confidence.nullable(),
  origins: z.array(Origin).max(5),
  notes: z.array(Text),
  location: Location.nullable(),
});
export type AirflowTask = z.infer<typeof AirflowTask>;

export const AirflowDag = z.strictObject({
  id: Id,
  name: Name,
  owner: Name.nullable(),
  location: Location.nullable(),
  tasks: z.array(AirflowTask),
});
export type AirflowDag = z.infer<typeof AirflowDag>;

/** The DAGs and tasks touching some nodes, or all of them (the Airflow view). */
export const AirflowResult = z.strictObject({
  stage: AirflowStage,
  summary: AirflowSummary,
  dags: z.array(AirflowDag),
  unknowns: z.array(Unknown),
});
export type AirflowResult = z.infer<typeof AirflowResult>;

export const AirflowStatus = z.strictObject({
  stage: AirflowStage,
  generation: Count,
  summary: AirflowSummary,
});
export type AirflowStatus = z.infer<typeof AirflowStatus>;

/** A changed file no node maps to, that cannot change one. */
export const OtherFile = z.strictObject({
  path: Text,
  status: FileStatus,
});
export type OtherFile = z.infer<typeof OtherFile>;

export const BaselineInfo = z.strictObject({
  name: Name,
  sha256: Name.nullable(),
  generatedAt: Name.nullable(),
  dbtVersion: Name.nullable(),
});
export type BaselineInfo = z.infer<typeof BaselineInfo>;

export const ReviewArtifacts = z.strictObject({
  manifestGeneratedAt: Name.nullable(),
  dbtVersion: Name.nullable(),
  baselineManifest: BaselineInfo.nullable(),
});
export type ReviewArtifacts = z.infer<typeof ReviewArtifacts>;

export const ReviewBase = z.strictObject({
  ref: Name.nullable(),
  commit: Name.nullable(),
});
export type ReviewBase = z.infer<typeof ReviewBase>;

export const ReviewSummary = z.strictObject({
  changedFiles: Count,
  changedNodes: Count,
  changedColumns: Count,
  staleNodes: Count,
  impacted: z.record(z.string(), Count),
  impactedByColumn: Count,
  unknowns: Count,
});
export type ReviewSummary = z.infer<typeof ReviewSummary>;

/** A Change Impact Review (design section 8). The same inputs give the same result; locations aside, it is what lineagepilot review --json prints, and markdown is what it prints without --json. */
export const ReviewResult = z.strictObject({
  project: Name.nullable(),
  base: ReviewBase.nullable(),
  artifacts: ReviewArtifacts,
  columnLineage: ColumnStage,
  summary: ReviewSummary,
  changedNodes: z.array(ChangedNode),
  impacted: z.array(ReviewImpacted),
  owners: z.array(OwnerGroup),
  airflow: AirflowSummary,
  unknowns: z.array(Unknown),
  otherFiles: z.array(OtherFile),
  outsideProject: Count,
  notes: z.array(Text),
  markdown: Text,
  locations: z.record(z.string(), Location),
});
export type ReviewResult = z.infer<typeof ReviewResult>;

export const IndexError = z.strictObject({
  severity: z.enum(["error", "warning"]),
  message: Text,
  location: Text.nullable(),
});
export type IndexError = z.infer<typeof IndexError>;

export const ServerInfo = z.strictObject({
  version: Name,
  python: Name,
  executable: Text,
  pid: Count,
  isolated: z.boolean(),
  modules: z.record(z.string(), Text),
});
export type ServerInfo = z.infer<typeof ServerInfo>;

export const ProjectInfo = z.strictObject({
  root: Text.nullable(),
  name: Name.nullable(),
  problems: z.array(Text).max(100),
});
export type ProjectInfo = z.infer<typeof ProjectInfo>;

export const ManifestInfo = z.strictObject({
  path: Text,
  exists: z.boolean(),
  generatedAt: Name.nullable(),
  dbtVersion: Name.nullable(),
  schemaVersion: z.int().nullable(),
  modifiedAt: Name.nullable(),
});
export type ManifestInfo = z.infer<typeof ManifestInfo>;

export const CatalogInfo = z.strictObject({
  path: Text,
  exists: z.boolean(),
  modifiedAt: Name.nullable(),
});
export type CatalogInfo = z.infer<typeof CatalogInfo>;

export const Artifacts = z.strictObject({
  manifest: ManifestInfo.nullable(),
  catalog: CatalogInfo.nullable(),
});
export type Artifacts = z.infer<typeof Artifacts>;

export const Counts = z.strictObject({
  nodes: Count,
  columns: Count,
  edges: Count,
  unresolved: Count,
  nodeKinds: z.record(z.string(), Count),
});
export type Counts = z.infer<typeof Counts>;

export const ColumnStates = z.strictObject({
  resolved: Count,
  partial: Count,
  unknown: Count,
});
export type ColumnStates = z.infer<typeof ColumnStates>;

/** Source files changed after the manifest was written. */
export const Stale = z.strictObject({
  count: Count,
  examples: z.array(Text).max(5),
});
export type Stale = z.infer<typeof Stale>;

/** The conformance rules behind the diagnostics. In Restricted Mode only the built-in rules run. */
export const RulesStatus = z.strictObject({
  pack: Text.nullable(),
  packLoaded: z.boolean(),
  restricted: z.boolean(),
  problems: z.array(Text).max(50),
  findings: Count,
  score: z.number().min(0).max(100).nullable(),
  coverage: z.number().min(0).max(100).nullable(),
});
export type RulesStatus = z.infer<typeof RulesStatus>;

export const Status = z.strictObject({
  protocolVersion: z.int().nullable(),
  server: ServerInfo,
  project: ProjectInfo,
  trusted: z.boolean(),
  state: IndexState,
  indexing: z.boolean(),
  generation: Count,
  columnLineage: ColumnStage,
  message: Text.nullable(),
  errors: z.array(IndexError).max(50),
  errorCount: Count,
  artifacts: Artifacts,
  counts: Counts.nullable(),
  columnStates: ColumnStates.nullable(),
  stale: Stale,
  fromCache: z.boolean(),
  indexedAt: Name.nullable(),
  rules: RulesStatus,
  airflow: AirflowStatus,
});
export type Status = z.infer<typeof Status>;

export const requests = {
  'lineage/status': { params: EmptyParams, result: Status },
  'lineage/refresh': { params: EmptyParams, result: Status },
  'lineage/graph': { params: GraphParams, result: GraphResult },
  'lineage/impact': { params: ImpactParams, result: ImpactResult },
  'lineage/export': { params: ExportParams, result: ExportResult },
  'lineage/nodesForFile': { params: NodesForFileParams, result: NodesForFileResult },
  'lineage/node': { params: NodeParams, result: NodeResult },
  'lineage/reviewChanges': { params: ReviewParams, result: ReviewResult },
  'lineage/airflow': { params: AirflowParams, result: AirflowResult },
} as const;
export type RequestMethod = keyof typeof requests;

export const notifications = {
  'lineage/indexUpdated': Status,
} as const;
export type NotificationMethod = keyof typeof notifications;
