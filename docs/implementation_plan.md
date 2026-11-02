# VS Code Extension Implementation Plan

**Status**: Direction approved 1 November 2026. Tracked in beads (see Tracking).
**Date**: 1 November 2026
**Companion**: [design.md](design.md)

Estimates assume one full-time engineer who knows the codebase. They are ranges, not commitments.

Each phase ends in exit evidence. Two phases end in go/no-go **gates** that decide whether investment continues.

---

## Timeline

| Phase | Focus | Estimate | Ends with |
|---|---|---|---|
| 0 | Spikes and customer discovery | 2 weeks | **Gate G0**: proceed or stop |
| 1 | `lineagepilot_core` library | about 4 weeks (S2 added parser fixes and task 1.9) | Core passes fixtures and performance budgets |
| 2 | Language server and extension shell | 2–3 weeks | Integration tests green on 3 operating systems |
| 3 | Lineage graph webview | 2 weeks | Interactive graph from the ported components |
| 4 | Change Impact Review (the wedge) | 2–3 weeks | Acceptance scenario passes (dbt part) |
| 5 | Airflow linkage | 2–3 weeks | Full acceptance scenario passes |
| 6 | Beta and release | 2 weeks | **Gate G1**: continue, pivot or stop |

- **One engineer**: about 16–19 weeks, after the Phase 0 findings. Task 5.5's corpus labelling runs in parallel.
- **Two engineers** (Python and TypeScript): about 11–13 weeks. After Phase 2, Phase 3 can run in parallel with Phases 4 and 5.

The **legacy wind-down** runs alongside and is not on the critical path. Rotating the Snowflake key cannot wait for any phase.

---

## Phase 0: Spikes and discovery (2 weeks)

| ID | Task | Done when |
|---|---|---|
| S1 | **Packaging spike.** Build a universal VSIX with pure-Python libraries in `bundled/libs`, and launch a minimal pygls server through an interpreter found via `@vscode/python-extension`. | The server starts and returns a CodeLens on macOS, Windows and Linux, with Python 3.10 and 3.13. Every runtime dependency installs with `--implementation py`. |
| S2 | **Parse and performance spike.** Import the existing `dbt_parser` without the database layer, and index real dbt Core 1.8+ and Fusion manifests into a throwaway in-memory graph. | Design §11 budgets are measured on 2,000- and 5,000-model manifests. The supported manifest versions are pinned. |
| S3 | **Static DAG spike.** Use the AST to detect Cosmos `DbtDag`/`DbtTaskGroup` and `BashOperator` dbt commands. | Correct task-to-model links on 3 public example DAG repositories. Misses are listed. |
| S4 | ~~**Customer discovery.** Interview 10 teams running dbt plus Airflow. Show a clickable mock of the Change Impact Review.~~ **Waived** on 1 November 2026 by the product owner, who takes the outcome as given from their experience as a data engineer, data engineering lead and data architect (LP-1ay.4). | n/a |

**Gate G0**: proceed only if all of the following hold.
1. S1 and S2 meet their criteria, or show a credible path to meeting them.
2. ~~At least 5 of the 10 interviewees say the cross-tool review would change how they review pull requests.~~ Met by product-owner judgment (S4 waived). Market validation therefore rests on the Phase 6 beta and Gate G1.
3. Spikes S1 and S2 confirm decision D-2 (Python sidecar). Decisions D-1, D-3 and D-4 were made on 1 November 2026.

If G0 fails, stop before Phase 1. Little beyond the spikes has been built at that point.

---

## Phase 1: `lineagepilot_core` library (about 4 weeks)

| ID | Task |
|---|---|
| 1.1 | Create `packages/core` with uv and `uv.lock`. Add an `import-linter` contract banning `sqlalchemy`, `fastapi` and `pydantic`. Add a CI job. |
| 1.2 | Move the parsers (`base`, `dbt_parser`, `dbt_config`, `sql_parser`, `data_flow_annotations`, `dbt_manifest_resolver`) and their unit tests. Rewrite imports, and add the stdlib `json` fallback for `orjson`. Then fix what S2 found: <br>• the per-column lineage loop; <br>• `has_tests` derived from `test.*` nodes; <br>• exposures; <br>• source file paths and `patch_path`; <br>• `unique_id` identity; <br>• the v12 schema gate; <br>• no `print()`. <br>Test against real manifests, not hand-written ones. |
| 1.3 | `MetadataGraph`: canonical IDs, logical-to-physical edges, evidence records and `Unresolved` records (design §5). |
| 1.4 | Traversal: upstream and downstream at model and column level, depth caps, cycle safety, and an impact query. |
| 1.5 | Rules engine: five result states, a coverage-aware score, the 12 carried and 6 remapped rules, and `.lineagepilot/rules.yml` packs with length-capped, time-limited regexes. |
| 1.6 | PII indicators that trace every upstream path, not just the first, labelled as indicators rather than verified exposure. |
| 1.7 | Exporters (JSON, Mermaid, DOT, GraphML, Cypher, CSV) with per-format escaping, plus hypothesis property tests. |
| 1.8 | Snapshot cache keyed by manifest hash, and a headless entry point: `lineagepilot index` and `lineagepilot impact`. |
| 1.9 | Column lineage with `sqlglot.lineage` and a schema from `catalog.json`, replacing `sql_column_parser` (S2). Models without a catalog, or that fail to parse, are `unknown`. Must be done before task 1.4's column-level traversal. |

**Exit**:
- Coverage of at least 80%.
- The golden fixtures pass.
- The §11 budgets are met on the synthetic 5,000-model manifest.
- Exporter property tests find no injection.

---

## Phase 2: Language server and extension shell (2–3 weeks)

| ID | Task |
|---|---|
| 2.1 | pygls server: discover workspace folders, index on start, watch `target/`, DAG folders and rule packs, and re-index with a debounce. |
| 2.2 | LSP features: `textDocument/codeLens`, `textDocument/hover`, `publishDiagnostics`. |
| 2.3 | `lineage/*` custom methods, with one shared JSON Schema generating zod (TypeScript) and Python validators. Add protocol version negotiation. |
| 2.4 | TypeScript client: activation events, interpreter resolution chain, server start/restart/crash backoff, output channel and status bar. |
| 2.5 | Security wiring: Workspace Trust (`limited`), `restrictedConfigurations`, machine-scoped settings, and a guarded "Refresh dbt artifacts" command. |
| 2.6 | Tree views: **Lineage** and **Airflow** (empty until Phase 5). |
| 2.7 | Build pipeline: `scripts/bundle-libs` with `--require-hashes`, a `vsce package` size budget, and Open VSX packaging. |

**Exit**:
- `@vscode/test-cli` integration tests on the fixture workspace pass on all 3 operating systems.
- The restricted-mode tests prove dbt cannot run and workspace settings cannot change the interpreter.
- Extension coverage is at least 75%.

---

## Phase 3: Lineage graph webview (2 weeks)

| ID | Task |
|---|---|
| 3.1 | Webview host: CSP with a nonce, `localResourceRoots`, and a zod-validated message union. |
| 3.2 | Port `CapsuleNode`, `ColumnNode`, `LineageGraph`, `ColumnLineageGraph`, `ColumnDetailPanel`, `ImpactAnalysis` and `SettingsPanel`. Replace `@/lib/api/*` with the transport, and `next/navigation` with `openFile`. |
| 3.3 | Model and column modes, a depth control, filters, auto-layout and lazy expansion. |
| 3.4 | VS Code theme variables, and keyboard navigation with accessible labels. |

**Exit**:
- React Testing Library coverage of at least 75%.
- A 500-node graph stays interactive.
- Clicking a node opens the right file and range.

---

## Phase 4: Change Impact Review (2–3 weeks)

This is the differentiating feature, so it comes before Airflow linkage, which then extends it.

| ID | Task |
|---|---|
| 4.1 | Git integration through the `vscode.git` API: the merge-base with the default branch, and changed and untracked files. |
| 4.2 | Map files to nodes, and diff columns against the baseline in priority order (baseline manifest → `catalog.json` → model level only). |
| 4.3 | Impact aggregation: grouped results with evidence and owners, plus an **Unknowns** section. |
| 4.4 | Review panel UI and deterministic "Copy as Markdown". |
| 4.5 | `lineagepilot review`, producing output identical to the panel, ready for CI later. |

**Exit**:
- The dbt half of the acceptance scenario (design §14) passes.
- Golden-file tests confirm the Markdown output is deterministic.

---

## Phase 5: Airflow linkage (2–3 weeks)

| ID | Task |
|---|---|
| 5.1 | Grow the S3 spike into `dag_static.py`. It covers Cosmos, `BashOperator`/`KubernetesPodOperator`/`DockerOperator` dbt commands, SQL operators, and task dependencies. Add the resolution S3 showed is needed (design section 6.2): <br>• workspace-module constants; <br>• `Path(__file__)` and environment-variable defaults; <br>• one-level helper inlining; <br>• `cwd`; <br>• Airflow file discovery; <br>• dbt-loom exclusion; <br>• version-pinned selectors with a `dbt ls` conformance test. |
| 5.2 | Load annotation files through the reused `data_flow_annotations.py`, including a container path map. Add suffix matching for `/usr/local/airflow`-style paths. |
| 5.3 | Feed `orchestrates` edges into CodeLens, the Airflow tree view and the Change Impact Review. Show heuristic links by default, labelled. |
| 5.4 | Emit `unknown` records for dynamic DAGs and unresolved selectors, using the named categories from S3, at the call site. |
| 5.5 | Held-out corpus: 10 or more unseen dbt+Airflow repositories, labelled before any Phase 5 tuning. It can start straight after G0 (LP-56q.5). |

**Exit**:
- The full acceptance scenario passes.
- With the scanner frozen, the miss rate on the 5.5 held-out corpus is measured and reported, not only the hits. The S3 repositories are in-sample and do not count.

---

## Phase 6: Beta and release (2 weeks)

| ID | Task |
|---|---|
| 6.1 | README, a `walkthroughs` contribution for first-run setup, a sample project and troubleshooting docs. |
| 6.2 | Release pipeline that publishes to the Marketplace and Open VSX from tagged CI builds only. The publisher account is protected with 2FA. |
| 6.3 | Closed beta with dbt-plus-Airflow teams recruited by the product owner. There is no interviewee pool, because S4 was waived. Feedback goes through GitHub issues. Decide whether opt-in telemetry is wanted. |

**Gate G1** (four weeks into the beta): continue if at least half of the active beta users open the review weekly, at least 3 teams have pasted reviews into real pull requests, and at least one team reports it changed a decision. Otherwise pivot, for example to a CI-only product using the same core, or stop.

---

## Legacy wind-down (parallel, in the data-capsule repository)

These tasks change data-capsule, not this repository. They are tracked in data-capsule's beads, under epic DCS-4sz.

1. **Now, independent of every gate**: rotate the leaked Snowflake key and purge it from both remotes (DCS-hov).
2. **Now**: stop any running deployment of the hosted server. Its critical issues are closed as won't-fix, so a running instance stays exposed. The beads issues were closed or re-targeted on 1 November 2026 (design decision D-1).
3. Any time: add `backend/LEGACY.md` and `frontend/LEGACY.md` in data-capsule.
4. After G1 (LP-7er.4 here): delete or archive data-capsule's `backend/` and `frontend/` (DCS-4sz.3). Beads cannot link issues across repositories, so DCS-4sz.3 is marked blocked by hand.

---

## After the MVP (backlog, not scheduled)

- A CI GitHub Action that runs `lineagepilot review` and comments on pull requests, possibly as the paid tier.
- Live Airflow REST mode with machine-scoped connections and SecretStorage.
- Snowflake catalog and access-history lineage, once its SQL construction is fixed.
- Navigation features (go-to-definition, previews), only if beta users ask for them.
- Neovim and JetBrains support through the same language server.
- BI exposure connectors, chosen by pilot demand.

---

## Definition of done (every task)

- Unit tests, plus integration tests wherever a boundary is crossed. The coverage gates hold.
- No new `unknown` or `unsupported` path is silently converted into a pass.
- Security checklist reviewed: trust mode, setting scope, input limits, escaping.
- User-facing behaviour is documented in the README or the walkthrough.

## Tracking

Filed in beads on 1 November 2026. Later that day it moved to this repository's own beads, under the prefix `LP`. IDs kept their suffixes, so DCS-58r.2 became LP-58r.2. Each task blocks on the previous phase's epic, so `bd ready` only shows work that can start.

| Phase | Epic | Gate |
|---|---|---|
| 0 Spikes and discovery | LP-1ay (tasks .1–.4) | G0 = LP-1ay.5 |
| 1 `lineagepilot_core` | LP-58r. Task 1.7 is LP-auw, LP-aml, LP-l1s | |
| 2 Language server and shell | LP-243 | |
| 3 Lineage webview | LP-b7z | |
| 4 Change Impact Review | LP-zod | |
| 5 Airflow linkage | LP-56q | |
| 6 Beta and release | LP-7er | G1 = LP-7er.4 |

The legacy wind-down stays in data-capsule's beads as DCS-4sz, which includes DCS-hov (the key rotation). Any `DCS-…` ID in these documents refers to a data-capsule issue.

Find the next task with `bd ready`.
