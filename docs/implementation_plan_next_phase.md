# Implementation plan: phases 7–11, beyond dbt and Airflow

**Status**: Draft, 15 November 2026. It implements [design_next_phase.md](design_next_phase.md), which is still a proposal. It becomes the plan when the product owner approves decisions D-8 to D-14 (see [Decisions and questions](#decisions-and-questions-and-when-each-is-needed)).
**Date**: 15 November 2026
**Companions**: [design_next_phase.md](design_next_phase.md) (what and why), [implementation_plan.md](implementation_plan.md) (phases 0–6, whose conventions this plan keeps)

Estimates assume one full-time engineer who knows the codebase. They are ranges, not commitments. A bare section reference such as §6.4 points into design_next_phase.md; references into the MVP design say "design.md §…".

---

## Timeline

| Phase | Focus | Estimate | Ends with |
|---|---|---|---|
| Entry | Close the Airflow gaps, measure the scanner again, fit the server's memory budget | 2–3 weeks | The entry conditions of §6.2 are met |
| 7 | A multi-tool graph, the reader contract, the dbt semantic layer | 3–4 weeks | Airflow unchanged through the contract; metrics in the review; release 0.3.0 |
| 8 | Dagster | 3–4 weeks | Acceptance scenario with Dagster; held-out miss rate; release 0.4.0 |
| 9 | BI: Looker, Lightdash, Power BI | 4–6 weeks | Acceptance scenario with consumers; a miss rate per tool; release 0.5.0 |
| G2 | Four weeks of use | 4 weeks (calendar) | **Gate G2**: keep widening, deepen, or reorder |
| 10 | Databricks Asset Bundles with Spark Declarative Pipelines; GitHub Actions | 3–4 weeks | Acceptance scenario with Databricks jobs; release 0.6.0 |
| 11 | Upstream: Airflow 3 assets, dlt, Meltano, OpenLineage files | 3–4 weeks | A changed ingestion file reviewed through to the dbt models below it; release 0.7.0 |

- **One engineer**: phases 7–11 take the design's 16–22 weeks of building. Before them, the entry work takes 2–3 weeks, and the G2 window adds 4 calendar weeks between phases 9 and 10. The window is not idle ([Inside the G2 window](#inside-the-g2-window)).
- **Corpus labelling.** The design's estimate assumes labelling runs in parallel. If the same engineer labels, each corpus costs about 2–4 days, roughly 3 weeks over the plan. A second person labelling is faster, and keeps the labels blinder.
- **Two engineers** (Python and TypeScript, as in phases 0–6): Phase 7 splits between the core and the extension. After it, phases 8 and 9 run side by side, and so do phases 10 and 11. Expect about 10–14 weeks of building, plus the G2 window, which then follows both 8 and 9.
- **Order.** After Phase 7, phases 8–11 do not depend on each other (§6.1). What teams say they run (question Q1) can reorder them. G2 still comes after the first release that has Dagster and a BI tool in it.
- **Releases** (proposed): one minor version per phase, from the tag pipeline, to the Marketplace and Open VSX. Version numbers in this plan are proposals.

---

## How every reader phase runs

Phases 8–11 each add readers through the contract built in Phase 7, so they share one shape. It is written down once here, which keeps each phase's table to what is specific to its tools.

1. **Before the phase starts**, its held-out corpus has been labelled blind, during the previous phase ([Corpus timeline](#corpus-timeline)). The tool versions the reader supports are chosen and pinned.
2. **Days 1–3: a mapping note.** How the tool's concepts become nodes, edges, IDs, origins and unknown categories. It is appended to §7.1 and reviewed before any code, because IDs end up in golden files and saved views.
3. **Week 1: a heavy fixture and the scan budget**, measured with `packages/lsp/scripts/server_budgets.py`. The reference is the DAG scan, at 5.1–6.6 s cold (§8.2).
4. **Build** discovery, reading, linking and unknowns. Each pattern gets a fixture. Each unknown category gets a fixture that shows it at the line that caused it, with the annotation in `.lineagepilot/lineage.yml` that answers it.
5. **Hostile fixtures**: oversized files, deep nesting, YAML alias bombs, paths that escape the workspace, and symlinks. For a reader of Python, a file that writes a marker if it is ever run (design.md §10).
6. **The review, the tree views and CodeLens** show the tool. After Phase 7 these are generic, so this step is mostly golden files.
7. **Freeze, then score.** The reader's hash is recorded, the corpus is scored once, and the first result is reported as it is: misses, unknowns, and false positives.
8. **Release**: CHANGELOG, a README section with the supported versions and the reader's known boundary, and the walkthrough if setup changed.

Proposed: a reader with more than 2% false positives, the G2 ceiling, does not ship turned on by default until that is fixed. Whether to drop a reader with a poor miss rate (§12) is the product owner's decision at the phase exit.

---

## Entry (2–3 weeks)

These are the conditions of §6.2, as beads issues. Condition 1 is met: Gate G1 passed on 13 November 2026 (design.md §15), and 0.2.0 is public.

| ID | Task | Done when |
|---|---|---|
| E.1 | **LP-0o1**: an environment variable with no default, such as `os.environ["AIRFLOW_HOME"]`, is matched by the tail of the path, as `heuristic`, as container paths already are. | A fixture per form (subscript, `.get`, f-string, `os.path.join`) links. 35 of the 89 misses had this cause. |
| E.2 | **LP-z2x**: a helper imported from another workspace module is inlined one level, as syntax and inside the workspace only, as local helpers already are. | A cross-file helper links. An import that leaves the workspace, or a helper two levels deep, is still a named unknown. The marker-file test still passes. |
| E.3 | **LP-c05**: a container path that matches no project, such as `/app`, uses a mount that names the host folder, or else the only project in the workspace, as `heuristic`. | Both cases link. With two or more projects, the path stays unknown. |
| E.4 | **LP-aax**: a fresh held-out corpus of 10 or more repositories. They are labelled blind while E.1–E.3 are built, and are not used to test those fixes. The scanner is frozen, its hash recorded, and the corpus scored once. | The new miss rate and false positives are in `corpus/README.md`, beside the 84.0%. |
| E.5 | **LP-dtt**: server memory after a cold index at 5,000 models comes within 500 MB. | `server_budgets.py` shows it on the synthetic manifest. |
| E.6 | **LP-qy0.10**: verify on Windows that the server's memory watch stops a child process past its limit. Added by this plan: every reader relies on that limit (§7.2). | Verified on a Windows machine, or the gap is fixed. |
| E.7 | **Owner**: approve D-8 to D-14 and record them in design.md §15. Answer Q3. | Recorded. |
| E.8 | **Owner**: publish without the global Azure DevOps PAT, by setting up the Marketplace's trusted publishing and revoking the PAT. Global PATs are being retired, so this blocks release 0.3.0. | A dry-run release job authenticates without `VSCE_PAT`. |

**Exit**: E.1–E.6 are closed and the fresh miss rate is reported. The design sets no threshold, so the product owner reads the result before Phase 7 starts. If the miss rate barely moves, that is a reason to keep working on Airflow, not to widen (§6.2).

**Status, 16 November 2026** (merged to `main` in PR #5, `7c5cbd4`). E.1–E.6 are closed and the fresh miss rate is reported, so the exit's own conditions are met; the owner's reading of E.4 remains:
- **E.1–E.3 done** (`fd686c8`): LP-0o1, LP-z2x and LP-c05 closed, each with its fixtures.
- **E.4 done**: the fresh corpus scores 15 of 35, a miss rate of 57.1%, with no false positives. **The 0.2.0 scanner scores the same 15**: the sample holds almost none of the patterns E.1–E.3 fix, so it does not measure them. On the first corpus, now in-sample, they take 17 to 55 of 106. See [corpus/README.md](../corpus/README.md#a-second-corpus-after-the-entry-fixes-lp-aax); follow-ups LP-244 to LP-249.
- **E.5 done**: snapshot format 3, read a batch at a time, takes the server at 5,000 models from 608 to 388 MB cold and 511 to 384 MB warm on macOS, with the warm start at 0.90 s ([design.md §11](design.md#11-performance-budgets)).
- **E.6 done**: on Windows, the server's memory watch stopped a child past its limit, in CI (`lineagepilot-lsp`, job `windows-memory-watch`, runs 43894714635 and 43899691674). LP-qy0.10 closed. The rest of the server's unit suite still hangs on Windows (LP-7t7), so the job runs the limits' tests alone.
- **E.7, E.8**: the owner's. E.8's code is on `main` (`68826b9`, Trusted Publishing); what remains is the publisher's policy and revoking the PAT.
- **Rollback to 0.2.0**: [release.md](release.md#rolling-back-to-an-earlier-release).

These scanner issues are not entry conditions. They stay in the backlog as candidates for the G2 window: LP-2cd, LP-x29, LP-4aw, LP-nqk, LP-ykj and LP-o59.

---

## Phase 7: a multi-tool graph, and the dbt semantic layer (3–4 weeks)

Order: 7.1 and 7.2 first, then 7.3. After that, 7.4–7.9 can run in any order, and the extension work (7.6) can go to a second engineer.

| ID | Task | Done when |
|---|---|---|
| 7.1 | **Graph model (D-9).** In `graph/model.py`: `NodeKind.PIPELINE`, `TASK` and `CONSUMER`; `tool` and `subkind` fields on `Node`; `EdgeKind.LOADS`, `CONSUMES` and `PART_OF`. Each reader's origin is added in the phase that adds the reader. `MetadataGraph.airflow` (an `AirflowSummary`) becomes one summary per reader, keyed by tool. Airflow DAGs and tasks become `pipeline` and `task` nodes with `tool="airflow"`, and their IDs stay `airflow:<dag>` and `airflow:<dag>.<task>`. Update everything that branches on the old kinds: `AIRFLOW_KINDS` in `airflow/link.py`, `KIND_ORDER` in `analysis/review.py`, traversal, and the exporters in `export/graph.py`. | The exporters' property tests pass with the new kinds and edges. A test shows that no rule or PII indicator applies to a pipeline or consumer node: they stay limited to `DBT_KINDS`. `SNAPSHOT_FORMAT` is bumped and `tests/test_snapshot_format.py` records the new model, so a server rolled back to 0.2.0 ignores the new snapshots ([release.md](release.md#rolling-back-to-an-earlier-release)). |
| 7.2 | **A shared reading package.** Move `safe_io`, `values`, `shell`, `selector`, `workspace` and `categories` out of `lineagepilot_core/airflow/` into `lineagepilot_core/readers/common/`. Move the scanner itself to `lineagepilot_core/readers/airflow/`. `UnknownCategory` becomes shared, and its annotation hint names `lineage.yml`. | The Airflow tests move with only their import lines changed, and the diff of the test files shows nothing else. |
| 7.3 | **The reader contract (§7.2).** A `Reader` protocol in `readers/contract.py` covering discovery, reading, linking, output and lifecycle. In the server, `airflow_scan.py` and `worker.py` become one runner for any reader, with a time limit, a memory limit, and the result released after linking. The DAG watch in `watch.py` becomes a watch on each reader's globs. Airflow is the first reader, and `lineagePilot.airflow.dagFolders` stays its setting. | The Phase 5 acceptance scenario and the server's stdio tests pass. Their golden files change only by the renames agreed in D-9. |
| 7.4 | **The annotation file (§7.3, D-14).** Read `.lineagepilot/lineage.yml` through `parsers/data_flow_annotations.py`, and keep reading `airflow.yml`. `pipelines[].tool` defaults to `airflow`. A new `consumers` section adds consumer nodes, with origin `annotation` and confidence `declared`, optionally down to columns. Proposed: when both files exist, `lineage.yml` wins, and the status says `airflow.yml` was ignored. | Fixtures cover each file alone, both together, and a consumer on a column. A change to either file starts a new scan. |
| 7.5 | **The protocol (§7.4).** In `protocol/lineage-protocol.schema.json`, add `lineage/pipelines` (with `nodes` and an optional `tools`), `lineage/consumers`, an entry for each reader in `lineage/status`, and `pipelines` and `consumers` in review results. `lineage/airflow` and the review's `airflow` field stay for one protocol version, through the negotiation (design.md §9). Regenerate the zod and Python validators. | The contract examples validate on both sides. An older client still gets `lineage/airflow`. |
| 7.6 | **The extension.** `views/airflowTree.ts` becomes the **Pipelines** view, grouped by tool. Add a **Consumers** view. CodeLens shows counts by tool (`features.py`). In the graph webview, labels, node styles and filters cover kind and tool (`labels.ts`, `ModelNode.tsx`, `ImpactPanel.tsx`), with tool names in plain text and no logos. The review page gets the new sections (`ReviewApp.tsx`). | Unit, React Testing Library and integration tests pass, with coverage at 75% or more. Restricted Mode shows both views. |
| 7.7 | **The review's Markdown (§7.5).** Sections in the design's order. Each section is cut on its own within `MAX_CHARS` in `export/review_markdown.py`, with a count, so one tool cannot push another out. | A golden file pins the order. A test fills one section to its limit and the others still appear. |
| 7.8 | **The dbt semantic layer (task 7.5 of §6.3).** Add `semantic_model.`, `metric.` and `saved_query.` to `INDEXED_PREFIXES` in `parsers/dbt_parser.py`, as consumer nodes with `tool="dbt_semantic"`. Edges: `consumes` from a semantic model to its model; its measures, dimensions and entities become columns under it, each consuming the model columns its `expr`, or its name, refers to (expressions go through sqlglot, and anything else is an unknown); a metric consumes the measures or metrics it uses; a saved query consumes its metrics and group-by dimensions, and lists its exports. `make_fixture.py` adds to `fixtures/impact-shop`, with real dbt, a semantic model on the mart, a metric whose measure uses the renamed column, and a saved query with an export. If dbt needs a time spine model to parse metrics, the fixture adds one. | Renaming the mart column lists the metric and the saved query, with their owners. |
| 7.9 | **The corpus scorer (task 7.6 of §6.3).** `corpus/score.py` scores any reader, with one folder per tool (`corpus/<tool>/`, holding `repos.json`, `labels/` and `results/`) and a reader hash per tool. Today `SCANNER_FILES` hashes `airflow/*.py`, which 7.2 moves. The Airflow corpus moves to `corpus/airflow/`. | Scoring the current scanner gives the same numbers as before the move. |
| 7.10 | **A budget harness for all readers.** `server_budgets.py` gets a section for each reader. A benchmark checks that a review of 100 changed models with every reader turned on stays within 300 ms (§8.2). In Phase 7 it covers Airflow and the semantic layer, and each later phase adds its reader. | The design.md §11 budgets hold, measured. |
| 7.11 | **Docs and release 0.3.0.** design.md §15 records D-8 to D-14. Update the README, the CHANGELOG and the annotation file's documentation. | 0.3.0 is on both registries. |

**Exit**:
- The Phase 5 acceptance scenario passes. Its golden files change only by the renames agreed in D-9.
- Renaming a mart column lists the metric and the saved query that use it.
- The design.md §11 budgets hold.

**Prepared during Phase 7**: the Dagster corpus is labelled, and Q1 is asked.

---

## Phase 8: Dagster (3–4 weeks)

| ID | Task |
|---|---|
| 8.0 | **The corpus**, done before the phase: 10 or more public Dagster and dbt repositories, at pinned commits, labelled blind into `corpus/dagster/`. |
| 8.1 | **The mapping note**, with pinned `dagster` and `dagster-dbt` versions. Proposed: a `@dbt_assets` definition or a dbt component is a `task` (subkind `asset`) that `orchestrates` the models it selects; a job and a schedule are `pipeline` nodes (subkinds `job` and `schedule`); a non-dbt asset is a `task` (subkind `asset`) downstream of the dbt asset whose key it names. One thing must be settled here: an asset can belong to several jobs, so asset IDs cannot nest under one job the way Airflow tasks nest under their DAG. |
| 8.2 | **Discovery.** `lineagePilot.dagster.paths`. By default it finds `defs.yaml` files, a `pyproject.toml` with `[tool.dg]`, and Python files that import `dagster`, checked as text before parsing, as the DAG scan does. When nothing is found, it says where it looked. |
| 8.3 | **Components.** `defs.yaml` files with `type: dagster_dbt.DbtProjectComponent`, read with `safe_yaml`: the project path, the selection, and the attributes that set asset keys. A templated value is an unknown. |
| 8.4 | **Python definitions.** `@dbt_assets(manifest=..., select=..., exclude=...)` and `DbtProject(project_dir=...)`, read with the shared safe evaluator. Selections go through the shared selector engine. The manifest path decides which dbt project it is. |
| 8.5 | **Asset keys.** The default translator's keys, `meta.dagster.asset_key`, and literal prefix options. A custom `DagsterDbtTranslator` subclass is a named unknown. A conformance script, `scripts/dagster_key_conformance.py`, runs real dagster-dbt on `fixtures/impact-shop`'s manifest and compares its keys with the reader's. It runs in CI, in a job of its own, as `selector_conformance.py` does. |
| 8.6 | **Jobs and schedules.** `define_asset_job(selection=...)`, with a bounded subset of `AssetSelection`: keys, groups, `build_dbt_asset_selection`, upstream and downstream, union, intersection and difference. Plus `ScheduleDefinition` and `@schedule`. Anything outside the subset is an unknown. |
| 8.7 | **Non-dbt assets downstream.** `@asset(deps=[...])`, `AssetIn` and `AssetSpec(deps=...)`, when they name a dbt asset key. |
| 8.8 | **Unknowns.** The categories in §6.4, folded into the existing ones where they fit (wrapper, loop or factory, not literal). Annotations take `tool: dagster`. |
| 8.9 | **The review, fixtures and release.** Dagster rows grouped by code location and job. Fixtures in `fixtures/impact-shop/dagster/`: one component, one definitions file, and one pattern that cannot be read. Then freeze, score, and release 0.4.0. |

**Exit** (§6.4):
- The acceptance scenario on `fixtures/impact-shop` passes with a Dagster component and a Python definitions file.
- The held-out miss rate and false positives are reported.

**Prepared during Phase 8**: the Looker, Lightdash and Power BI corpora are labelled. The Power BI Project fixtures are saved. Q2 is answered, and the method for scoring G2 is chosen.

---

## Phase 9: consumers in BI (4–6 weeks)

The order is Looker, then Lightdash, then Power BI. Lightdash is small because it reuses dbt's `meta`. Power BI is last, so it is what moves if time runs short or Q2 removes it.

| ID | Task |
|---|---|
| 9.0 | **Done before the phase.** Q2 answered (Power BI in or out). A corpus labelled for each tool. Power BI Project fixtures saved with Power BI Desktop and committed (§9). This needs a Windows machine with Power BI Desktop, so who makes them, and when, is settled early in Phase 8. |
| 9.1 | **The LookML parser.** Check `lkml` against the bundler's checks (pure Python, MIT, pinned by hash) and against caps on file size, nesting depth and string length, inside the reader's child-process limits. Decide in the first three days whether to ship it or write a bounded parser. Hostile fixtures either way. |
| 9.2 | **LookML structure.** `include:` (inside the workspace only), `extends` and refinements (`+view`). `@{constant}` from `manifest.lkml` is resolved where it is literal. Liquid is an unknown. |
| 9.3 | **A view's table.** `sql_table_name`, or `derived_table: sql:` through sqlglot. Proposed dialect: the dbt manifest's `metadata.adapter_type`, because the Looker connection's dialect is not in the files; a setting can override it. Matching to dbt relations: a full name is `inferred`, and a partial name is `heuristic` only when exactly one relation ends with it. |
| 9.4 | **LookML fields and content.** `${TABLE}.column` in a field's `sql:` consumes the model's column. `${view.field}` references are followed within LookML. Explores' joins and `.dashboard.lookml` elements give `part_of` edges. Every review with Looker rows says that Looks and dashboards saved in the instance are not visible. |
| 9.5 | **Lightdash.** Dimensions and metrics come from dbt `meta` in the manifest. Charts and dashboards come from the YAML that `lightdash download` writes. |
| 9.6 | **The Power BI model.** A line-oriented TMDL reader, with caps, for tables, columns with their `sourceColumn`, and partitions. M navigation steps are recognised for `Snowflake.Databases`, `Databricks.Catalogs`, `GoogleBigQuery.Database` and `Sql.Database`. The text of `Value.NativeQuery` goes to sqlglot. DAX references (`'Table'[Column]`) give edges inside the model. M is never evaluated. |
| 9.7 | **The Power BI report.** PBIR (`*.Report/definition/`) and the legacy `report.json`: which visuals, on which pages, use which fields. Unknowns: M built from parameters or functions, dataflows, a model built on another model, and a `.pbix` file, which the review says to save as a Power BI Project. The PBIR version read is pinned, and a version it does not know is an unknown (§12). |
| 9.8 | **Exposures.** A dashboard that a BI reader finds and a dbt exposure names, matched by URL, becomes one row. |
| 9.9 | **The review, fixtures and release.** The Consumers view, consumer counts in CodeLens, and the review's Consumers section at column level, with golden files. The extended acceptance fixture is in `fixtures/impact-shop`. Then freeze, score each tool (giving the sample size when it is small), and release 0.5.0. |

**Exit** (§6.5):
- The acceptance scenario passes with a LookML view and explore, a Power BI Project model and report, and a Lightdash chart, all on the mart. Renaming the mart column lists the dimension, the Power BI column and page, and the chart.
- A Liquid-templated view and a parameterised M query appear as unknowns.
- Each tool's held-out miss rate is reported.

---

## Gate G2 (after Phase 9)

Continue widening, into phases 10 and 11, only if all three criteria of §10 hold: measured quality (2% false positives at most), use (3 or more teams pasting reviews over four weeks), and value (one decision changed by a row from a tool other than dbt or Airflow). Two things the design leaves open must be settled first:

1. **How G2 is measured.** The weekly check-in form and the usage counts were removed on 13 November 2026 (design.md §15, D-7 revised), and G1 had no check-ins filed. Criteria 2 and 3 therefore need a channel. Proposed: the product owner asks the teams directly and records the answers in the G2 beads issue. A GitHub issue form, "a review that changed a decision", can add to that. Neither counts anything automatically, so the no-telemetry promise holds. Decide this before 0.5.0 ships.
2. **When the four weeks start.** At the release of 0.5.0. Teams that use Dagster can start counting at 0.4.0.

### Inside the G2 window

The window is work that pays off whichever way G2 goes:
- the remaining Airflow scanner issues (LP-2cd, LP-x29, LP-4aw, LP-nqk, LP-ykj, LP-o59);
- the first export-file route, the Looker content export, which covers the dashboards saved in a Looker instance ([The export-file route](#the-export-file-route-by-demand));
- labelling the Phase 10 corpus, which is cheap to throw away if G2 says stop.

**Outcomes**: continue to Phase 10; or deepen what exists (miss rates, column-level consumers) instead; or reorder phases 10 and 11 by what the teams said they run.

---

## Phase 10: declarative orchestrators (3–4 weeks)

| ID | Task |
|---|---|
| 10.0 | **Corpora**, labelled during Phase 9 or the G2 window: Databricks bundles that run dbt, and GitHub workflows that run dbt. |
| 10.1 | **The bundle reader.** `databricks.yml` and the files its `include:` names, inside the workspace only. `${var.x}` is resolved from the defaults and the default target, where literal, and the review names which target that was. Fixture bundles are validated in CI against Databricks' published bundle schema, pinned in the repository. |
| 10.2 | **Jobs.** `resources.jobs.<job>.tasks[]`. `dbt_task.commands` go through the shared shell parser, with `project_directory` giving the project. `depends_on` gives `task_depends_on` edges. `sql_task` is parsed with sqlglot. `notebook_task` is an unknown unless the notebook is SQL. `pipeline_task` links to the pipeline resource it names. |
| 10.3 | **Spark Declarative Pipelines in SQL.** `CREATE MATERIALIZED VIEW` and `CREATE STREAMING TABLE … AS SELECT`, from the pipeline resources' source files, through sqlglot in the Databricks dialect. |
| 10.4 | **Spark Declarative Pipelines in Python.** `@dp.table` and `@dlt.table` functions with literal table reads, through the shared AST evaluator. The marker-file test applies. |
| 10.5 | **Pipelines in the graph.** A pipeline table that dbt reads as a source gets a `loads` edge, upstream. A pipeline that reads a dbt model's relation is downstream of it. |
| 10.6 | **GitHub Actions.** `.github/workflows/*.yml`: a step whose `run:` invokes dbt is read by the shell parser. `${{ }}` expressions are unknowns, as Jinja is. `on.schedule` cron entries become the pipeline's schedule. |
| 10.7 | **The review, fixtures and release.** Then freeze, score, and release 0.6.0. |

**Exit** (§6.6): the acceptance scenario passes with Databricks jobs, and the held-out miss rate is reported.

Not scheduled, but cheap once 10.2 exists: Kestra flows, and dbt Cloud jobs defined in Terraform (§6.6). Both wait for demand.

---

## Phase 11: upstream (3–4 weeks)

| ID | Task |
|---|---|
| 11.0 | **Corpora**, labelled during Phase 10: dlt, Meltano, and Airflow 3 repositories that use assets. Public OpenLineage files are rare, so that sample may be fixtures only. If so, the result says so. |
| 11.1 | **D-11.** Add `Confidence.OBSERVED` to the graph, the protocol and the labels in the interface. No rule and no traversal treats it as `declared`. |
| 11.2 | **Airflow 3 assets**, in the existing scanner, first because it is cheapest. `outlets=[Asset(...)]` and `inlets` on tasks already scanned, and Airflow 2's `Dataset`. An asset that names a dbt source's table gives a `loads` edge from the task. |
| 11.3 | **dlt.** `dlt.pipeline(pipeline_name=..., destination=..., dataset_name=...)`, and the `name` or `table_name` of its resources, through the shared evaluator. Tables are matched to dbt sources by schema and identifier, with `loads` edges. The marker-file test applies. |
| 11.4 | **Meltano.** In `meltano.yml`, the extractors and their selected streams, and each loader's schema setting. |
| 11.5 | **OpenLineage files.** Run events (`observed`) and the static job and dataset events. Jobs become pipeline and task nodes. Datasets map to dbt relations by OpenLineage's naming, for Snowflake, Databricks and BigQuery first (§4.5). The origin is `openlineage`. Each file has a size cap, and URLs in facets are never followed. The mapping note decides whether these files are found in the workspace or through the export route. |
| 11.6 | **The review's Upstream section.** A changed source definition shows what loads it. A changed dlt or Meltano file maps to its pipeline, and from there through `loads` into the dbt graph. Today such files appear only as "other changed files". |
| 11.7 | **Fixtures and release.** Then freeze, score, and release 0.7.0. |

**Exit** (§6.7): a changed ingestion file is reviewed through to the dbt models below it, and each reader's held-out miss rate is reported.

---

## The export-file route (by demand)

The route of §6.8, built per tool and starting in the G2 window:
- a documented command or query that the user runs with their own credentials;
- a machine-scoped setting, `lineagePilot.exports.<tool>`, listed in `restrictedConfigurations`;
- the file read as untrusted data, with the usual caps, and origin `export`;
- the export's date shown in the review, so a stale export is visible.

The first is the Looker content export. After that, demand decides. A Snowflake `ACCESS_HISTORY` export needs D-11 first, because its facts are `observed`.

---

## Corpus timeline

Each corpus is labelled before its reader is written, so the labels are blind by construction.

| Corpus | Labelled during | Scored at |
|---|---|---|
| Airflow, fresh (LP-aax) | Entry | The end of Entry |
| Dagster | Phase 7 | The end of Phase 8 |
| Looker, Lightdash, Power BI | Phase 8 | The end of Phase 9 |
| Databricks bundles, GitHub Actions | Phase 9 or the G2 window | The end of Phase 10 |
| Airflow 3 assets, dlt, Meltano, OpenLineage | Phase 10 | The end of Phase 11 |

If phases 8–11 are reordered, the labelling order follows.

---

## Decisions and questions, and when each is needed

| Item | Needed before | Why |
|---|---|---|
| D-8 (how tools are chosen), D-13 (no second framework) | Phase 7 | They set the scope of the whole plan. |
| D-9 (the graph model), D-14 (the annotation file) | Phase 7 | Tasks 7.1 and 7.4 build them. |
| D-10 (tools in a service: export files only) | The first export route, in the G2 window | It decides the route's settings. |
| D-11 (the confidence `observed`) | Phase 11 | Task 11.1 builds it. |
| D-12 (live connectors) | Nothing in this plan | It stays open. Q4 asks the same thing. |
| Q1: which tools teams run | The end of Phase 7 | It can reorder phases 8–11. The weekly check-in form it names was removed on 13 November 2026, so ask the teams directly, or add an issue form. |
| Q2: Power BI, in or out | The middle of Phase 8 | The Power BI corpus and fixtures are made in Phase 8. |
| Q3: the CI GitHub Action | Phase 7 | It competes for the same engineer. Every phase reaches `lineagepilot review`, so an Action built early carries each new reader into pull requests. |

---

## Definition of done

Every task meets the definition of done for phases 0–6 ([implementation_plan.md](implementation_plan.md#definition-of-done-every-task)). A reader is also done only when all of these hold:
- It appears in the review, with evidence on every row, and every unknown at the line that caused it, with the annotation that answers it.
- Its hostile fixtures pass. A reader of Python passes the marker-file test. Restricted Mode reads its files and runs nothing.
- Its scan budget is measured, and the design.md §11 budgets hold with every reader turned on.
- The tool versions it supports are pinned and documented. A format version it does not know is an unknown, not a guess.
- Text from its files that reaches the review follows the review's Markdown rules (design.md §8).
- Its `paths` setting is resource-scoped and bounded. Export paths are machine-scoped and listed in `restrictedConfigurations`.
- Any new dependency is pure Python, pinned by hash, checked by the bundler, and listed in `THIRD_PARTY_NOTICES.md`.

---

## Tracking

Not yet filed. When D-8 to D-14 are approved, file in beads under the prefix `LP`: one epic per phase, and G2 as a gate task, as G1 was. Phase 7 blocks on the Entry epic. Phases 8 and 9 block only on Phase 7, so they can be reordered. G2 blocks on Phase 9, and phases 10 and 11 block on G2. `bd ready` then shows only work that can start.

| Phase | Epic | Gate |
|---|---|---|
| Entry | To be filed. It gathers LP-0o1, LP-z2x, LP-c05, LP-aax, LP-dtt and LP-qy0.10 | |
| 7 A multi-tool graph and the semantic layer | To be filed | |
| 8 Dagster | To be filed | |
| 9 BI | To be filed | G2 |
| 10 Declarative orchestrators | To be filed | |
| 11 Upstream | To be filed | |
