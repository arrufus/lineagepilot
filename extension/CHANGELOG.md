# Changelog

The versions of LineagePilot, newest first. Each release's entry is its release notes.

## 0.4.0

What a dbt change reaches now includes Dagster: the dbt asset definitions, jobs and schedules that run your models, and the assets that read them. LineagePilot is no longer marked **Preview**.

- **Dagster linkage.** Dagster definitions are read as syntax, never imported or run, so Dagster need not be installed. The rules followed are dagster 1.13.29's and dagster-dbt 0.29.29's. LineagePilot reads:
  - dbt components: `DbtProjectComponent` in a `defs.yaml`, with its project, selection and `translation`;
  - `@dbt_assets`, with `DbtProject`, and the older `load_assets_from_dbt_project` and `load_assets_from_dbt_manifest`, also inside a `@repository` function;
  - asset jobs (`define_asset_job`), with `AssetSelection`, `build_dbt_asset_selection` and selection strings such as `group:marts+`;
  - schedules: `ScheduleDefinition`, `@schedule` and `build_schedule_from_dbt_selection`;
  - assets that read a dbt asset, through `deps`, `AssetIn`, `AssetSpec` or `get_asset_key_for_model`;
  - the code location each is in, from a `dg` project, `workspace.yaml`, `dagster_cloud.yaml`, `[tool.dagster]` or the `Definitions` call that loads it.

  Asset keys and groups follow dagster-dbt's defaults, `meta.dagster` and a component's `translation`, and were checked against dagster-dbt itself.
- **Where Dagster shows.** Dagster is in the **Pipelines** view, CodeLens, the lineage graph and **Review Changes**. In the review, Dagster's rows are grouped by code location: the dbt asset definitions first, then each job followed by its schedules, then the assets that read dbt. A schedule's row names its job and cron.
- **What cannot be read is an unknown, at the line that caused it:**
  - a custom `DagsterDbtTranslator`;
  - keys worked out at run time;
  - assets or jobs made in a loop or a factory;
  - a decorator of your own that makes dbt assets;
  - assets loaded from outside the workspace;
  - Python that runs dbt itself, through `subprocess`, dbt's Python API, `DbtCliResource.cli` or Dagster Pipes.

  What is built on an unknown, such as a job over a definition that cannot be read, is an unknown too, not left out. `.lineagepilot/lineage.yml` answers Dagster's unknowns with `tool: dagster`.
- **`lineagePilot.dagster.paths`** sets the folders to look in. The default is the whole workspace.
- **The command line**: `lineagepilot review` and `lineagepilot impact` read Dagster too. `--dagster-path` sets the folders, and `--no-dagster` leaves Dagster out.
- **The sample project** has two Dagster code locations beside its DAG files, and **Review Changes** lists what they run.
- **Airflow**: a DAG folder whose `__init__.py` imports its own modules (`from . import x`) no longer makes the scan stop with "expression nesting too deep".

**What to expect from the Dagster linkage.** It was measured on 110 links in 12 public repositories it had never seen. It linked 16 correctly, gave no wrong link, and made no link where there was none. Most of the rest it could not read. The repositories it does well on are those that use dagster-dbt's own APIs; one repository with its own decorator over dbt holds half the links, and none of them can be read. That run also found the reader silent on 69 links, with no link and no unknown, which this release fixes: on the same repositories, it now links 18 and lists all but 6 of the rest as unknown, at the line that caused each one. That second figure is no longer a measure on repositories it had never seen ([corpus/dagster/README.md](https://github.com/arrufus/lineagepilot/blob/main/corpus/dagster/README.md)).

**If you read LineagePilot's output in a script:**
- Dagster's nodes have `"tool": "dagster"`. A code location, a job and a schedule are kind `pipeline`, and a dbt asset definition and an asset that reads dbt are kind `task`.
- The review's JSON gives a schedule's job and cron, as `job` and `cron`, on its row.

**The first start after upgrading indexes each project again**, because the index cache's format changed.

## 0.3.0

What a dbt change reaches now goes past dbt's models and Airflow's tasks, to what reads your models: the dbt semantic layer, and anything else you declare. It is still a **Preview**.

- **The dbt semantic layer.** Semantic models, metrics and saved queries, read from `manifest.json`, are in CodeLens, the lineage graph, the new **Consumers** view and **Review Changes**. What each one reads is followed down to the column: rename a mart's column, and the review lists the metrics and saved queries that read it, with their owners. A saved query also lists the tables its exports write. Both ways of writing the semantic layer work: with measures, and dbt 1.12's newer form without them. dbt Fusion leaves the semantic layer out of its manifest (as of Fusion 2.0.9), so with Fusion there is none to show.
- **Declared consumers.** `.lineagepilot/lineage.yml` can say what reads your models from outside dbt, such as a dashboard, down to the column. Declared consumers are shown wherever the semantic layer is. A name that matches nothing is an unknown, not dropped.
- **`.lineagepilot/lineage.yml`** is the annotation file's new name, for every tool, and `airflow.yml` is still read. When both exist, `lineage.yml` is read, and the status says the other was ignored.
- **Pipelines view.** It replaces the Airflow view, and groups pipelines by tool. Airflow is the only tool so far.
- **The review is organised by section:**
  1. what changed;
  2. the downstream impact, with the semantic layer in a table of its own;
  3. pipelines and consumers, each by tool;
  4. owners;
  5. unknowns, by tool.

  When the Markdown is too long, each section is cut on its own and says how many rows it left out, so one section cannot push the others out.
- **Counts by kind and tool.** CodeLens and the review's headline count the semantic layer by what each node is, as in `↓ 1 model · 2 semantic models · 4 metrics · 1 saved query`, and count tasks by tool. A semantic model, metric or saved query has its own CodeLens in its YAML file.
- **Airflow linkage reads more:**
  - a path built on an environment variable with no default, such as `$AIRFLOW_HOME/dbt`, matched by the path after the variable;
  - a helper function imported from another file in the workspace, one level deep;
  - dbt in a container: a path that matches no project goes through the task's bind mounts, or else to the workspace's only dbt project.

  The links these make are `heuristic`. Two cases that used to be left out are now unknowns at their line: a `DockerOperator` whose image runs dbt, given only dbt's arguments, and a task inside an `if` decided by an environment variable's default.
- **Less memory on large projects.** At 5,000 models with Airflow on, the language server on macOS holds about 390 MB, where it held 510–610 MB.

**What to expect from the Airflow linkage.** It was measured again on 35 task links in 12 repositories it had never seen. It linked 15 correctly and listed 19 of the other 20 as unknown, at the line that caused each one. One was wrong, and it made no link where there was none. These repositories happen to use few of the patterns that 0.3.0 adds, and 0.2.0 links the same 15 ([corpus/airflow/README.md](https://github.com/arrufus/lineagepilot/blob/main/corpus/airflow/README.md)).

**If you read LineagePilot's output in a script:**
- `lineagepilot impact --json`, `lineagepilot review --json` and **Export Lineage Graph** call an Airflow task's kind `task` and a DAG's `pipeline`, not `airflow_task` and `airflow_dag`.
- The review's JSON rows give the tool beside the kind: `"tool": "airflow"`.
- In a Cypher export the labels are `Task` and `Pipeline`, not `AirflowTask` and `AirflowDag`.
- The review's Markdown has the sections above, in that order.

**The first start after upgrading indexes each project again**, because the index cache's format changed.

## 0.2.0

The first public release, on the VS Code Marketplace and Open VSX. It is a **Preview**: what it shows is accurate as far as it goes, and it says plainly where it could not tell.

LineagePilot shows what a dbt change reaches, from the artifacts already in your workspace. It needs no server, no account and no warehouse connection, and makes no network connections.

- **Lineage as you edit**: CodeLens, hovers, the Problems panel, the Lineage view and the lineage graph, at model and column level, from `manifest.json`, compiled SQL and `catalog.json`.
- **Change Impact Review**: what your branch and working tree change, what that reaches across models, exposures, Airflow tasks and owners, and what could not be checked. **Copy as Markdown** puts it in your pull request; `lineagepilot review` prints the same text.
- **Airflow linkage**: tasks linked to the models they run, from DAG files read as syntax and never run, with `.lineagepilot/airflow.yml` for what cannot be read.
- **Get Started walkthrough** and a **sample project** that needs neither dbt nor Airflow.
- **Restricted Mode**: only reads artifacts, never runs dbt or an interpreter from the workspace.

**What to expect from the Airflow linkage.** On 106 task links in 17 repositories it had never seen, it linked 17 correctly and listed most of the rest as unknown, at the line that caused each one. Two were wrong, and it made no link where there was none. Expect the review's Airflow section to list tasks it could not read, and use `.lineagepilot/airflow.yml` to answer them.

**Removed**: usage counts and **Copy Usage Counts**, which existed only for the closed beta's weekly check-in. Anything they counted on your machine is deleted when 0.2.0 starts. A `lineagePilot.usageCounts.enabled` line left in your user settings does nothing, and you can delete it.

## 0.1.1

The first beta, for teams invited to the closed beta.

- **Lineage from your dbt artifacts**, in a local language server: CodeLens, hovers, the Problems panel, the Lineage view and the lineage graph, at model and column level, from `manifest.json`, compiled SQL and `catalog.json`.
- **Change Impact Review**: what your branch and working tree change, what that reaches across models, exposures, Airflow tasks and owners, and what could not be checked. **Copy as Markdown** for the pull request; `lineagepilot review` prints the same text.
- **Airflow linkage**: tasks linked to the models they run, from DAG files read as syntax and never run, with `.lineagepilot/airflow.yml` for what cannot be read.
- **Get Started walkthrough** and a **sample project** that needs neither dbt nor Airflow.
- **Usage counts** for the beta's weekly check-in: off unless you turn them on, kept on your machine, never sent.
- Restricted Mode: only reads artifacts, never runs dbt or an interpreter from the workspace.
