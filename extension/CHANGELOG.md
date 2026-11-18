# Changelog

The versions of LineagePilot, newest first. Each release's entry is its release notes.

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
