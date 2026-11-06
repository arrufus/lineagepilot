# lineagepilot_core

The editor-agnostic core of LineagePilot: parsers, the metadata graph, analysis (lineage, impact, rules, PII) and exporters. The language server (`packages/lsp`) and the headless CLI both run on it. See [design section 4.1](../../docs/design.md#41-components).

## Modules

| Module | What it does |
|---|---|
| `parsers/dbt_parser.py` | Reads `manifest.json` and `catalog.json` into nodes (models, seeds, snapshots, sources, exposures), columns, edges and unresolved references, all keyed by dbt `unique_id`. Gates on manifest schema v12. |
| `parsers/dbt_config.py` | PII name patterns and layer inference rules. |
| `parsers/sql_parser.py` | Tables read and written by one SQL statement (sqlglot). |
| `parsers/data_flow_annotations.py` | Declared data flow for Airflow tasks, from YAML or JSON. |
| `parsers/dbt_manifest_resolver.py` | dbt selectors to models, simplified. Nothing uses it: `airflow/selector.py` took over in Phase 5. |
| `parsers/sql_lineage.py` | Column lineage of one compiled SQL statement with `sqlglot.lineage` over a `catalog.json` schema; `unknown`, with a reason, whenever a wrong answer is possible. |
| `graph/ids.py` | Canonical IDs (design section 5.1): dbt `unique_id`, `relation:<db>.<schema>.<name>`, `<node>#<column>` and `airflow:<dag>.<task>`, folded to the adapter's case rules. |
| `graph/model.py` | `MetadataGraph`: nodes, columns, edges that point upstream to downstream and carry evidence (origin, confidence, artifact version, location), and `Unresolved` records. |
| `graph/build.py` | Builds the graph from a dbt parse, with explicit `materialises` edges to physical relations. Refuses a parse that has errors. |
| `graph/column_lineage.py` | Adds `derives` edges per model, with a `resolved` / `partial` / `unknown` status and `Unresolved` records. Run after the model-level index: about 3 ms a model. |
| `analysis/traversal.py` | Breadth-first upstream and downstream traversal at model or column level, with depth and size caps; steps through physical tables from builders to the sources that read them. |
| `analysis/impact.py` | Downstream impact of changed nodes or columns: column-precise where column lineage is known, model level (marked `inferred`) where it is not, with exposures, Airflow tasks, confidence and unknowns. |
| `analysis/pii.py` | PII indicators: declared (tag), suggested by a column name (pattern), or inherited along every lineage path (lineage), with masking stopping the flow. An exposure report for consumption layers and a per-column trace. Labelled indicators, not verified exposure. |
| `analysis/review.py` | The Change Impact Review (design section 8): changed files to nodes, each changed model's columns against a baseline, impact with evidence and owners, and every unknown met on the way. Returns the JSON the panel and the CLI share. |
| `analysis/baseline.py` | What a review needs from a baseline manifest, read only for the changed nodes. |
| `rules/` | Conformance rules with five results (`pass`, `fail`, `not_applicable`, `unsupported`, `unknown`) and a score that leaves the last two out, with coverage beside it. 18 built-in rules (design section 12). Custom rules come from `.lineagepilot/rules.yml`, a size-capped, validated file whose regexes are checked for ReDoS shapes before use. |
| `export/` | Graph exports (JSON, Mermaid, DOT, GraphML, Cypher) and the PII inventory (Markdown, CSV). Every value is escaped for its format; Mermaid and DOT IDs are generated; Cypher labels and relationship types come from fixed lists. Hypothesis property tests re-parse every format. `review_markdown.py` renders the review for a pull request: workspace text only reaches the page in code spans or with its punctuation escaped, and the text stays under GitHub's comment limit. |
| `airflow/dag_static.py` | The AST scanner (design section 6.2): a small abstract interpreter over a DAG file's syntax, its discovery rules, SQL operators. DAG files are never imported or executed. `values.py` holds what it computes, each with a confidence; `shell.py` finds dbt in commands; `cosmos.py` reads `DbtDag` and `DbtTaskGroup` and derives their task ids; `selector.py` is dbt's node selection, following the manifest's dbt version; `manifest.py` is a light view of a manifest; `standin.py` builds an approximate one from project files; `projects.py` maps a path in a DAG file to a project; `categories.py` names the reasons a file cannot be read; `safe_io.py` reads files that may be anything. |
| `airflow/workspace.py` | One scan for one project: its annotation file, then its DAG files. |
| `airflow/link.py` | Puts a scan into the graph, and takes it out again. A scan is never part of a snapshot. |
| `airflow/view.py` | The DAGs and tasks touching some nodes. |
| `gitdiff.py` | What changed since the merge-base, from git, for `lineagepilot review`. The extension asks the same questions through `vscode.git`. |
| `project.py` | `dbt_project.yml`, read as untrusted input: where the artifacts and source folders are. |
| `snapshot.py` | JSON snapshots of the whole graph, keyed by the artifacts' sha256, the snapshot format and the core version; atomic writes; a bad snapshot is rebuilt, never trusted. |
| `indexer.py` | One pipeline for the CLI and the language server's indexer: snapshot or parse, graph, column lineage, snapshot. It can hand over the model-level graph before column lineage (`on_model_graph`), and refuses artifacts over a size cap. |
| `cli.py` | `lineagepilot index`, `impact`, `review` and `airflow`. |

The parsers were ported from data-capsule at commit `5e57fcc`. Each module's docstring lists what changed and why; the changes come from spike S2 ([findings](../../spikes/s2-parse-perf/README.md#findings-that-should-change-the-design-or-the-phase-1-plan)).

Test fixtures under `tests/fixtures/dbt/` are real dbt artifacts, as are those of `fixtures/impact-shop`, the Change Impact Review's project, from before and after a column rename. `tests/fixtures/review/` holds its golden Markdown. `tests/fixtures/airflow/` holds DAG files, one per pattern, and dbt projects written for spike S3, with real `dbt ls` answers from dbt 1.9 and 1.12: `scripts/selector_conformance.py` records and checks them. [PROVENANCE.json](tests/fixtures/dbt/PROVENANCE.json) records where each came from and its hash.

## Develop

```sh
cd packages/core
export UV_PYTHON_PREFERENCE=only-managed   # pyproject.toml also sets this
uv sync --locked
uv run pytest                              # tests, the import contract and the 80% coverage gate
uv run ruff check . && uv run ruff format --check .
```

To run the tests on Python 3.10 as well:

```sh
UV_PROJECT_ENVIRONMENT=work/venv-py310 uv run --python 3.10 pytest
```

CI runs the same commands on Linux, macOS and Windows with Python 3.10 and 3.13 ([workflow](../../.github/workflows/lineagepilot-core.yml)).

## Performance budgets

Design section 11, measured on spike S2's 5,000-model synthetic project (dbt 1.12, with catalog) on an Apple M4 Max, 2 November 2026. Reproduce with `uv run python scripts/budgets.py <dir with manifest.json and catalog.json>`.

| Budget | Python 3.13 | Python 3.10 |
|---|---|---|
| Cold index, model level (parse and graph; ≤ 5 s at 2,000 models) | 2.53 s | 2.78 s |
| Column lineage for every model, after the index | 21.4 s | 29.4 s |
| Indexer peak memory (a child process) | 768 MB | 861 MB |
| Warm start from the snapshot (≤ 1 s) | 0.53 s | 0.56 s |
| Server memory after a warm start (≤ 500 MB) | 387 MB | 405 MB |
| CodeLens: impact counts, p95 (≤ 100 ms) | 0.08 ms | 0.13 ms |
| Model-level impact from the source with the largest closure, 651 nodes (≤ 300 ms) | 3.1 ms | 5.0 ms |
| Column lineage for one model, worst of the 10 longest (≤ 2 s) | 0.008 s | 0.011 s |

The server freezes its heap after loading (`gc.collect(); gc.freeze()`); without that, Python 3.10's garbage collection paused queries for up to 285 ms.

## Command line

```sh
uv run lineagepilot index --project-dir path/to/dbt/project       # or --json
uv run lineagepilot impact stg_orders#order_total --project-dir ...  # a model, column, file or unique_id
uv run lineagepilot review --project-dir ...                      # Markdown for a pull request, or --json
uv run lineagepilot review --base origin/develop --baseline-manifest prod/manifest.json --project-dir ...
```

`review` prints exactly what the panel's **Copy as Markdown** copies for the same inputs. It compares the working tree with the merge-base of `HEAD` and `--base`, by default the first of `origin/HEAD`, `origin/main`, `origin/master`, `main` and `master`, and needs `git` on `PATH` and enough history for a merge-base.

Snapshots go to `<project>/target/.lineagepilot` unless `--cache-dir` or `--no-cache` says otherwise. Exit codes: 0 success, 1 unknown index or target, or git cannot say what changed, 2 usage.

## Rules this package keeps

- **No server stack.** An `import-linter` contract in `pyproject.toml` forbids `sqlalchemy`, `fastapi` and `pydantic` (design section 13). `tests/test_package.py` runs the contract. It also imports every module in a fresh interpreter and checks that nothing from design section 4.4's dropped list gets loaded.
- **Locked with hashes.** `uv.lock` pins every package by sha256. CI runs `uv sync --locked`, which fails when the lock is stale, and audits the locked set with `pip-audit --require-hashes`.
- **Releases at least two weeks old.** `exclude-newer` in `pyproject.toml` stops the resolver picking anything newer than a fixed date. To take a newer release, move that date forward in its own commit, then re-run `uv lock` and the audit.
- **One sqlglot major.** sqlglot is pinned to `>=30.23,<31`, because its lineage API changes between majors (S2 finding 9).
- **No `print()`.** The language server talks over stdio, so any stray output corrupts the protocol. Ruff rule T20 enforces this.
