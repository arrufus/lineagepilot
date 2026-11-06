# LineagePilot

See what is upstream and downstream of the model you are editing, and what a change to it would reach, from the dbt artifacts already in your workspace. LineagePilot runs locally: no server, no account, no warehouse connection.

> **Preview.** This is the Phase 5 build: lineage, CodeLens, hovers, diagnostics, the Lineage view, the lineage graph, the Change Impact Review and Airflow linkage.

## What you get

- **CodeLens** on each model file and on each entry in a YAML properties file: everything downstream, for example `↓ 14 models · 3 Airflow tasks · 1 exposure · PII`. Click it to see the downstream in the Lineage view.
- **Hovers** on `ref()` and `source()` calls, and on the columns of the model you are editing: the direct upstream and downstream, with how each link is known (declared in the manifest, inferred from compiled SQL) and the column-lineage state.
- **Problems**: conformance and PII rules, each linked to its documentation. Results LineagePilot could not check are shown as "Not checked", never as passes. PII findings are *indicators*, from tags, names and lineage, not verified exposure.
- **Lineage view** in the activity bar: the upstream and downstream of the open file, one level at a time. Click a node to open it.
- **Airflow view** in the activity bar: the DAGs and tasks that run or read the open model, or all of them, each with how sure the link is and why, then what could not be read. Click one to open its DAG file at the task. DAG files are only ever parsed, never imported or run. See [Airflow](#airflow).
- **Lineage graph** (**Show Lineage Graph**, or the graph icon on a node in the Lineage view): the model, or one of its columns, with everything around it to the depth you choose. Filter by kind, highlight by name, and press **+** on a node at the edge to see further. Click a node for its details, its columns and its links, with a preview of its file beside the graph; double-click to open it, or **Impact** to see everything a change could reach. Solid edges are declared in the manifest, dashed ones inferred from SQL. The graph follows your theme and works from the keyboard: Tab to a node, the arrow keys to follow its edges (the view moves along with them), Enter for details, O to open.
- **Change Impact Review** (**Review Changes**, or the compare icon on the Lineage view): what your branch and working tree change, against the merge-base with the default branch or a base you choose. It shows each changed model and its columns as a diff, then what that reaches, grouped by models, exposures, Airflow tasks and owners, each with how sure it is and what says so, then an **Unknowns** section for everything that could not be checked. **Copy as Markdown** puts it on the clipboard for your pull request; `lineagepilot review` prints the same text for the same inputs. See [Reviewing a change](#reviewing-a-change).
- **Status bar**: node count and manifest age, or a warning when model files are newer than the manifest.
- **Commands** (LineagePilot: …): Show Lineage Graph, Analyze Impact of Current Model, Review Changes, Refresh dbt Artifacts, Re-index Lineage, Export Lineage Graph (JSON, Mermaid, DOT, GraphML, Cypher), Export PII Inventory (Markdown, CSV), Show Status, Restart Language Server, Show Output.

## Requirements

- A dbt project with `target/manifest.json`, from `dbt parse`, `dbt compile` or `dbt docs generate`. Manifest schema v12 (dbt 1.8 to 1.12, and Fusion) is supported.
- For column lineage: compiled SQL (`dbt compile`) and `target/catalog.json` (`dbt docs generate`). Without them, column lineage shows as *unknown* and impact falls back to whole models.
- **Python 3.10 or later.** LineagePilot looks, in order, at the `lineagePilot.python.interpreterPath` setting, the Python extension's selected interpreter, `python3` on `PATH` (`py -3` on Windows), and Pythons installed by [uv](https://docs.astral.sh/uv/). If it finds none and uv is installed, it offers to have uv download one; nothing is downloaded without your consent.

Large projects index in the background: the model-level lineage is ready in seconds, and column lineage follows. The index is cached, so reopening a project is fast.

## Reviewing a change

A review compares what a changed model's compiled SQL selects with a baseline, so **run `dbt compile` after your edit**. Until you do, the review says the file is newer than the manifest, marks its columns unknown and lists everything below it at model level. **Run dbt compile** on the review does it for you.

The baseline is, in order:

1. the manifest in `lineagePilot.baseline.manifestPath`, typically the production one you use for dbt's `--state`. This is the one to set if you can: it knows what is deployed, and works with no `catalog.json`.
2. `target/catalog.json`, which lists what the warehouse held when `dbt docs generate` last ran. If you built the changed model before that, a rename no longer shows as a change.
3. nothing: the model's columns are marked unknown.

What the rows mean:

- **Column** rows are traced from a changed column through compiled SQL.
- **Model level** rows depend on a changed model, but no changed column is traced to them. They are listed because comparing column names cannot rule out a change in how an unchanged column is computed.
- **Confidence** uses the graph's line styles: solid when an artifact declares the link, dashed when parsing inferred it, dotted when a pattern suggested it.

What goes under **Unknowns**, rather than being left out: a changed macro or `dbt_project.yml` (which models they affect is not worked out), a new file the manifest has not seen, a downstream model whose SQL cannot be parsed, and a change to a properties file, which is listed but not followed downstream because it documents the data rather than changing it.

## Workspace Trust

A cloned repository can contain anything, so in **Restricted Mode** LineagePilot only reads the project's artifacts:

- it never runs dbt (**Refresh dbt Artifacts** is unavailable);
- it never runs a Python interpreter from inside the workspace, not even to check its version;
- it uses only its built-in rules, not `.lineagepilot/rules.yml`;
- it cannot review changes, because VS Code turns its Git extension off, and the review says so rather than reporting that nothing changed.

The interpreter and dbt executable settings are machine-scoped: a workspace's `.vscode/settings.json` cannot set them. VS Code also ignores a workspace's artifact size limit and baseline manifest in Restricted Mode.

## Settings

| Setting | Default | |
|---|---|---|
| `lineagePilot.python.interpreterPath` | empty | Python for the language server. Machine-scoped. |
| `lineagePilot.dbt.executablePath` | empty (`dbt` on `PATH`) | The dbt that **Refresh dbt Artifacts** runs. Machine-scoped. |
| `lineagePilot.index.columnLineage` | `true` | Trace lineage column by column. |
| `lineagePilot.index.maxArtifactMB` | `512` | Larger artifacts are not indexed. |
| `lineagePilot.baseline.manifestPath` | empty | The manifest **Review Changes** compares columns with. Absolute, or relative to the dbt project. |
| `lineagePilot.airflow.dagFolders` | `["**/dags"]` | Folders in the workspace that hold Airflow DAG files. `**/dags` finds any folder called `dags`, up to six levels down. |
| `lineagePilot.trace.server` | `off` | Log the messages to and from the language server. |

## Airflow

LineagePilot reads your DAG files as syntax, to work out which tasks run which models. It never imports or runs them, in any workspace, so airflow and cosmos need not be installed. It understands:

- Astronomer Cosmos `DbtDag` and `DbtTaskGroup`, and its single operators. A group is one link, with the task ids Cosmos should render for each model worked out from the manifest. This mapping is checked for Cosmos 1.11 and later.
- dbt commands in `BashOperator`, `KubernetesPodOperator`, `DockerOperator` and `SSHOperator`, including `cd x && dbt run`, several commands in one string, and `docker exec`.
- SQL operators: which dbt models' tables their SQL reads or writes.
- Task order, from `>>`, `<<`, `set_upstream`, `set_downstream` and `chain()`.
- Values from constants in other workspace files, `Path(__file__)`, environment variable defaults and local helper functions.

Links are labelled by how sure they are. **Most are `heuristic`**, because a path in a DAG file is usually a path inside a container, or depends on an environment variable's default. They are shown anyway, with the reason, because hiding them would hide most of the real ones.

What cannot be read is listed as an unknown, at the line that caused it, rather than guessed: tasks built in a loop or a factory, from a `manifest.json` read when Airflow parses the file, by a wrapper class or a function imported from another file; selectors that need dbt's state; Jinja or shell variables; a remote manifest; a generated project; a DAG file that cannot load. `corpus/README.md` in the repository says how often the scanner gets it right on repositories it had never seen.

To answer an unknown, or to tell LineagePilot where a container path really is, put `.lineagepilot/airflow.yml` at the root of the dbt project:

```yaml
path_map:
  /usr/local/airflow/dbt: .       # a path in a DAG file, and where that is, relative to the project
pipelines:
  - pipeline_id: finance_gl       # a DAG
    owner: finance-eng
    tasks:
      - task_id: load_gl
        consumes: [model.finance.stg_accounts, analytics.staging.raw_transactions]
        produces: [analytics.facts.gl]
acknowledged:                     # unknowns you have answered
  - file: dags/factory.py
```

`lineagepilot airflow` prints the same scan on the command line.

## Custom rules

Put a rule pack in `.lineagepilot/rules.yml` at the root of the dbt project to add rules, turn built-in ones off or change their severity. Problems in the pack show in the Problems panel on the pack file. The built-in rules are listed in [rules.md](https://github.com/arrufus/lineagepilot/blob/main/docs/rules.md).

## Troubleshooting

- **Lineage: no Python**: install Python 3.10+, or set `lineagePilot.python.interpreterPath`. **LineagePilot: Show Output** lists every interpreter it tried and why each was skipped.
- **Lineage unknown**: the manifest is missing, too large or not a dbt manifest. Run `dbt parse`.
- **Files newer than the manifest**: run **Refresh dbt Artifacts** (or `dbt parse`) so the lineage matches your edits.
- **Review: columns unknown**: the review says why. Most often the model changed after the manifest was written (run `dbt compile`), or nothing can say what it selected before (set `lineagePilot.baseline.manifestPath`, or run `dbt docs generate`).
- **Review: no default branch**: it tries `origin/HEAD`, `origin/main`, `origin/master`, `main` and `master`. Use **Change base** to pick another; a shallow clone needs fetching first.
