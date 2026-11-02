# LineagePilot

See what is upstream and downstream of the model you are editing, and what a change to it would reach, from the dbt artifacts already in your workspace. LineagePilot runs locally: no server, no account, no warehouse connection.

> **Preview.** This is the Phase 3 build: lineage, CodeLens, hovers, diagnostics, the Lineage view and the lineage graph. The change-impact review of your branch and Airflow linkage come in later versions.

## What you get

- **CodeLens** on each model file and on each entry in a YAML properties file: everything downstream, for example `↓ 14 models · 1 exposure · PII`. Click it to see the downstream in the Lineage view.
- **Hovers** on `ref()` and `source()` calls, and on the columns of the model you are editing: the direct upstream and downstream, with how each link is known (declared in the manifest, inferred from compiled SQL) and the column-lineage state.
- **Problems**: conformance and PII rules, each linked to its documentation. Results LineagePilot could not check are shown as "Not checked", never as passes. PII findings are *indicators*, from tags, names and lineage, not verified exposure.
- **Lineage view** in the activity bar: the upstream and downstream of the open file, one level at a time. Click a node to open it.
- **Lineage graph** (**Show Lineage Graph**, or the graph icon on a node in the Lineage view): the model, or one of its columns, with everything around it to the depth you choose. Filter by kind, highlight by name, and press **+** on a node at the edge to see further. Click a node for its details, its columns and its links, with a preview of its file beside the graph; double-click to open it, or **Impact** to see everything a change could reach. Solid edges are declared in the manifest, dashed ones inferred from SQL. The graph follows your theme and works from the keyboard: Tab to a node, the arrow keys to follow its edges, Enter for details, O to open.
- **Status bar**: node count and manifest age, or a warning when model files are newer than the manifest.
- **Commands** (LineagePilot: …): Show Lineage Graph, Analyze Impact of Current Model, Refresh dbt Artifacts, Re-index Lineage, Export Lineage Graph (JSON, Mermaid, DOT, GraphML, Cypher), Export PII Inventory (Markdown, CSV), Show Status, Restart Language Server, Show Output.

## Requirements

- A dbt project with `target/manifest.json`, from `dbt parse`, `dbt compile` or `dbt docs generate`. Manifest schema v12 (dbt 1.8 to 1.12, and Fusion) is supported.
- For column lineage: compiled SQL (`dbt compile`) and `target/catalog.json` (`dbt docs generate`). Without them, column lineage shows as *unknown* and impact falls back to whole models.
- **Python 3.10 or later.** LineagePilot looks, in order, at the `lineagePilot.python.interpreterPath` setting, the Python extension's selected interpreter, `python3` on `PATH` (`py -3` on Windows), and Pythons installed by [uv](https://docs.astral.sh/uv/). If it finds none and uv is installed, it offers to have uv download one; nothing is downloaded without your consent.

Large projects index in the background: the model-level lineage is ready in seconds, and column lineage follows. The index is cached, so reopening a project is fast.

## Workspace Trust

A cloned repository can contain anything, so in **Restricted Mode** LineagePilot only reads the project's artifacts:

- it never runs dbt (**Refresh dbt Artifacts** is unavailable);
- it never runs a Python interpreter from inside the workspace, not even to check its version;
- it uses only its built-in rules, not `.lineagepilot/rules.yml`.

The interpreter and dbt executable settings are machine-scoped: a workspace's `.vscode/settings.json` cannot set them.

## Settings

| Setting | Default | |
|---|---|---|
| `lineagePilot.python.interpreterPath` | empty | Python for the language server. Machine-scoped. |
| `lineagePilot.dbt.executablePath` | empty (`dbt` on `PATH`) | The dbt that **Refresh dbt Artifacts** runs. Machine-scoped. |
| `lineagePilot.index.columnLineage` | `true` | Trace lineage column by column. |
| `lineagePilot.index.maxArtifactMB` | `512` | Larger artifacts are not indexed. |
| `lineagePilot.airflow.dagFolders` | `["dags"]` | Where Airflow DAG files live (used from the Airflow release). |
| `lineagePilot.trace.server` | `off` | Log the messages to and from the language server. |

## Custom rules

Put a rule pack in `.lineagepilot/rules.yml` at the root of the dbt project to add rules, turn built-in ones off or change their severity. Problems in the pack show in the Problems panel on the pack file. The built-in rules are listed in [rules.md](https://github.com/arrufus/lineagepilot/blob/main/docs/rules.md).

## Troubleshooting

- **Lineage: no Python**: install Python 3.10+, or set `lineagePilot.python.interpreterPath`. **LineagePilot: Show Output** lists every interpreter it tried and why each was skipped.
- **Lineage unknown**: the manifest is missing, too large or not a dbt manifest. Run `dbt parse`.
- **Files newer than the manifest**: run **Refresh dbt Artifacts** (or `dbt parse`) so the lineage matches your edits.
