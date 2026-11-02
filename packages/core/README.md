# lineagepilot_core

The editor-agnostic core of LineagePilot: parsers, the metadata graph, analysis (lineage, impact, rules, PII) and exporters. The language server (`packages/lsp`) and the headless CLI both run on it. See [design section 4.1](../../docs/design.md#41-components).

## Modules

| Module | What it does |
|---|---|
| `parsers/dbt_parser.py` | Reads `manifest.json` and `catalog.json` into nodes (models, seeds, snapshots, sources, exposures), columns, edges and unresolved references, all keyed by dbt `unique_id`. Gates on manifest schema v12. |
| `parsers/dbt_config.py` | PII name patterns and layer inference rules. |
| `parsers/sql_parser.py` | Tables read and written by one SQL statement (sqlglot). |
| `parsers/data_flow_annotations.py` | Declared data flow for Airflow tasks, from YAML or JSON. |
| `parsers/dbt_manifest_resolver.py` | dbt selectors to models. Phase 5 replaces its selector rules. |
| `parsers/sql_lineage.py` | Column lineage of one compiled SQL statement with `sqlglot.lineage` over a `catalog.json` schema; `unknown`, with a reason, whenever a wrong answer is possible. |
| `graph/ids.py` | Canonical IDs (design section 5.1): dbt `unique_id`, `relation:<db>.<schema>.<name>`, `<node>#<column>` and `airflow:<dag>.<task>`, folded to the adapter's case rules. |
| `graph/model.py` | `MetadataGraph`: nodes, columns, edges that point upstream to downstream and carry evidence (origin, confidence, artifact version, location), and `Unresolved` records. |
| `graph/build.py` | Builds the graph from a dbt parse, with explicit `materialises` edges to physical relations. Refuses a parse that has errors. |
| `graph/column_lineage.py` | Adds `derives` edges per model, with a `resolved` / `partial` / `unknown` status and `Unresolved` records. Run after the model-level index: about 3 ms a model. |
| `analysis/traversal.py` | Breadth-first upstream and downstream traversal at model or column level, with depth and size caps; steps through physical tables from builders to the sources that read them. |
| `analysis/impact.py` | Downstream impact of changed nodes or columns: column-precise where column lineage is known, model level (marked `inferred`) where it is not, with exposures, Airflow tasks, confidence and unknowns. |

The parsers were ported from data-capsule at commit `5e57fcc`. Each module's docstring lists what changed and why; the changes come from spike S2 ([findings](../../spikes/s2-parse-perf/README.md#findings-that-should-change-the-design-or-the-phase-1-plan)).

Test fixtures under `tests/fixtures/dbt/` are real dbt artifacts. [PROVENANCE.json](tests/fixtures/dbt/PROVENANCE.json) records where each came from and its hash.

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

## Rules this package keeps

- **No server stack.** An `import-linter` contract in `pyproject.toml` forbids `sqlalchemy`, `fastapi` and `pydantic` (design section 13). `tests/test_package.py` runs the contract. It also imports every module in a fresh interpreter and checks that nothing from design section 4.4's dropped list gets loaded.
- **Locked with hashes.** `uv.lock` pins every package by sha256. CI runs `uv sync --locked`, which fails when the lock is stale, and audits the locked set with `pip-audit --require-hashes`.
- **Releases at least two weeks old.** `exclude-newer` in `pyproject.toml` stops the resolver picking anything newer than a fixed date. To take a newer release, move that date forward in its own commit, then re-run `uv lock` and the audit.
- **One sqlglot major.** sqlglot is pinned to `>=30.23,<31`, because its lineage API changes between majors (S2 finding 9).
- **No `print()`.** The language server talks over stdio, so any stray output corrupts the protocol. Ruff rule T20 enforces this.
