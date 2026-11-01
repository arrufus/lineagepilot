# Spike S2: parse and performance

**Phase 0, task S2.** Run on 1 November 2026, on an Apple M4 Max (16 cores, 64 GB) with native arm64 CPython 3.10.17 and 3.13.3 (uv-managed).

## The question

Can the **existing** `DbtParser`, with no database layer, index real dbt Core 1.8+ manifests (and Fusion manifests) within the design's section 11 budgets? Which manifest schema versions should LineagePilot pin as supported?

## Short answer

The **as-is parser is not fit to ship**. One small change to the parser, plus moving indexing into a child process, gets every budget met.

- The parser runs without the server stack. httpx, snowflake, sqlalchemy, fastapi and pydantic never load. It needs `orjson`, because it imports it with no fallback.
- It reads every real manifest correctly at model level. That covers dbt Core 1.8 to 1.12 and Fusion 2.0.9. Edge counts match `depends_on` exactly, with zero `ParseError`s.
- Cold index of 2,000 models meets the 5 s budget as-is: 1.4 s on 3.13 and 3.1 s on 3.10. At 5,000 models it misses: 6.0 s and 16.5 s. The cause is the O(columns × capsules) loop in `_build_lineage`. With a dict lookup, 5,000 models index in 1.9 s and 2.2 s.
- **Memory is the one budget the as-is design misses.** Indexing 5,000 models in one process peaks at about 1.0 GB, against a 500 MB budget. A server that loads the snapshot instead uses about 106 MB. So indexing must run in a child process.
- Warm start, CodeLens and impact all pass with orders of magnitude to spare.
- Column lineage is fast, but the output is useless on dbt-style SQL. 86–100% of models return a single `*` mapping, and almost no source column names a real relation. `sqlglot.lineage`, given a schema from `catalog.json`, resolves 91–99% of columns in at most 0.35 s per model.
- `has_tests` and `test_count` are **always False and 0** on real manifests.
- Every manifest tested reports `metadata.dbt_schema_version` **v12**, Fusion 2.0.9 included. Pin v12.

D-2 (Python sidecar) holds, provided the Phase 1 changes listed under "Findings" are made.

## Results

The criterion uses the **as-is** parser unless the row says otherwise. "Max" is the worst of 5 fresh-process runs for cold index and warm start, of 30 runs for impact, and of 1,000 queries for CodeLens. The catalog is included in cold index runs.

### Status by Python version and size

| Criterion (budget) | 3.10, 2,000 | 3.10, 5,000 | 3.13, 2,000 | 3.13, 5,000 |
|---|---|---|---|---|
| Cold index ≤ 5 s (budget set for 2,000; 5,000 is what Phase 1 must meet) | 3.07 s **met** | 16.47 s **not met** | 1.37 s **met** | 6.04 s **not met** |
| Same, optimised variant (dict lookup) | 0.89 s | 2.16 s | 0.74 s | 1.95 s |
| Warm start ≤ 1 s (JSON snapshot, includes interpreter start) | 0.10 s **met** | 0.18 s **met** | 0.10 s **met** | 0.17 s **met** |
| Warm start, SQLite snapshot | 0.12 s **met** | 0.22 s **met** | 0.12 s **met** | 0.22 s **met** |
| CodeLens/hover ≤ 100 ms p95 | 0.0016 ms **met** | 0.0017 ms **met** | 0.0011 ms **met** | 0.0013 ms **met** |
| Model-level impact ≤ 300 ms (high-fan-out source) | 0.16 ms **met** | 0.36 ms **met** | 0.12 ms **met** | 0.38 ms **met** |
| Model-level impact (median model) | 0.001 ms **met** | 0.001 ms **met** | 0.001 ms **met** | 0.001 ms **met** |
| Column lineage ≤ 2 s (10 most complex models, max) | 0.003 s **met on time; output unusable** | 0.003 s **met on time; output unusable** | 0.002 s **met on time; output unusable** | 0.002 s **met on time; output unusable** |
| Peak RSS ≤ 500 MB (budget set for 5,000) | 446 MB (reference) | 1,044 MB **not met** | 430 MB (reference) | 1,009 MB **not met** |
| RSS of a server that loads the snapshot (indexing in a child) | 62 MB | 105 MB **met** | 62 MB | 106 MB **met** |
| Graph webview, 500 nodes | not measured (Phase 3) | | | |

jaffle-shop (13 models, 6 sources) takes 0.04–0.05 s to cold-index on both versions, and every query is under 0.01 ms.

### Where cold-index time goes (as-is, one run, seconds)

| Phase | 3.10, 2,000 | 3.10, 5,000 | 3.13, 2,000 | 3.13, 5,000 |
|---|---|---|---|---|
| Import parser modules | 0.04 | 0.04 | 0.04 | 0.03 |
| `_load_json` (orjson, manifest and catalog) | 0.19 | 0.51 | 0.12 | 0.29 |
| `_process_models` (columns, PII regexes, layer and domain) | 0.48 | 1.15 | 0.39 | 1.03 |
| `_process_sources` | 0.14 | 0.33 | 0.11 | 0.42 |
| `_build_lineage` | **2.09** | **14.34** | **0.63** | **4.18** |
| Graph build | 0.01 | 0.04 | 0.01 | 0.03 |

### Memory: peak RSS of the indexing process (optimised parser, with catalog, max of 3)

| JSON loader | 3.10, 2,000 | 3.10, 5,000 | 3.13, 2,000 | 3.13, 5,000 | Cold index, 3.13, 5,000 |
|---|---|---|---|---|---|
| orjson (as-is) | 447 MB | 1,059 MB | 432 MB | 1,011 MB | 1.87 s |
| stdlib `json` (labelled shim) | 386 MB | 882 MB | 348 MB | 783 MB | 2.18 s |
| stdlib with an `object_hook` that drops tests, macros, docs and SQL text while decoding (labelled) | 316 MB | 710 MB | 289 MB | 659 MB | 1.98 s |

All three give an identical graph. Even the trimmed loader misses 500 MB at 5,000 models. Two things remain: the raw bytes and decoded text during the load, and the `ParseResult` itself, which holds 127,000 `RawColumn` dataclasses. With the manifest freed, the process still holds about 400 MB after parse. A snapshot-loaded graph of the same project is 106 MB.

### Other measurements (5,000 models)

| Measure | 3.10 | 3.13 |
|---|---|---|
| `json.loads` vs `orjson.loads`, manifest and catalog (p50) | 0.68 s vs 0.50 s | 0.50 s vs 0.30 s |
| Snapshot size, JSON / SQLite | 5.9 MB / 5.3 MB | 5.9 MB / 5.3 MB |
| Snapshot write, JSON / SQLite | 0.03 s / 0.13 s | 0.03 s / 0.12 s |
| SHA-256 of the 132 MB manifest (snapshot key), inside warm start | 0.055 s | 0.055 s |
| asyncio loop blocked by `await DbtParser().parse()` (longest gap) | 2,173 ms | 1,826 ms |
| Same with `asyncio.to_thread` (longest gap, GIL contention) | 524 ms | 232 ms |
| CodeLens with the *full* downstream count instead of direct (reference), p95 | 0.004 ms | 0.003 ms |
| Labelled `sqlglot.lineage` with a catalog schema, 10 most complex models: max time, columns reaching a table | 0.29 s, 191 of 210 | 0.35 s, 191 of 210 |

For context, producing these manifests takes dbt itself much longer. `dbt parse` of 5,000 models takes 40–54 s on Core and 12 s on Fusion.

The detailed tables, including p50/p95/max for every row, are in `results/bench_results.json`. `uv run python -m spike.report` renders them as Markdown.

### Cold index per dbt version (compile manifests, no catalog, one run each, 3.13)

| Project | dbt | Manifest | Capsules | Edges | As-is | Optimised | Same graph |
|---|---|---|---|---|---|---|---|
| synth-2000 | 1.8.10 | 46 MB | 2,603 | 3,630 | 1.80 s in the results file; three reruns gave 1.09 s each, so this was noise | 0.58 s | yes |
| synth-2000 | 1.10.23 | 51 MB | 2,603 | 3,630 | 1.12 s | 0.60 s | yes |
| synth-2000 | 1.12.6 | 53 MB | 2,603 | 3,630 | 1.12 s | 0.63 s | yes |
| synth-2000 | Fusion 2.0.9 | 48 MB | 2,603 | 3,630 | 1.12 s | 0.60 s | yes |
| synth-5000 | 1.8.10 | 116 MB | 6,503 | 9,080 | 4.73 s | 1.50 s | yes |
| synth-5000 | 1.10.23 | 126 MB | 6,503 | 9,080 | 4.85 s | 1.59 s | yes |
| synth-5000 | 1.12.6 | 132 MB | 6,503 | 9,080 | 4.90 s | 1.61 s | yes |
| synth-5000 | Fusion 2.0.9 | 119 MB | 6,503 | 9,080 | 4.90 s | 1.63 s | yes |

On 3.10 the same manifests take 2.4–2.5 s at 2,000 models and 12.7–13.0 s at 5,000 as-is, and 0.72–0.79 s and 1.82–1.96 s optimised. Manifest size grows a little with each dbt minor, and performance does not change meaningfully.

## Versions to pin

### dbt version to manifest schema

| dbt version | Adapter | `metadata.dbt_schema_version` |
|---|---|---|
| 1.8.10 | dbt-duckdb 1.8.4 | `https://schemas.getdbt.com/dbt/manifest/v12.json` |
| 1.9.11 | dbt-duckdb 1.9.6 | v12 |
| 1.10.23 | dbt-duckdb 1.10.1 | v12 |
| 1.11.16 | dbt-duckdb 1.11.1 | v12 |
| 1.12.6 | dbt-duckdb 1.11.1 (no 1.12 exists) | v12 |
| 2.0.9 (Fusion, "dbt v2") | built-in DuckDB driver; dummy Snowflake profile also tried | v12 |

The official manifest page (docs.getdbt.com/reference/artifacts/manifest-json, fetched 1 November 2026) lists v12 for dbt v1.8 through v2.0. One third-party summary in a web search claimed Fusion writes "v20". The real Fusion 2.0.9 manifests here say v12, and so does the official page.

### Field differences that matter to the parser

| Field | 1.8 | 1.9–1.12 | Fusion 2.0.9 | Effect on `DbtParser` |
|---|---|---|---|---|
| Generic tests | separate `test.*` nodes with `attached_node`, `column_name`, `test_metadata` | same | same, plus `depends_on.nodes_with_ref_location` | Parser looks for `data_tests` on nodes and columns. It is never there, so `has_tests` is always False |
| Tests on sources | `attached_node` is null; the source is in `depends_on.nodes` | same | same | Must be joined through `depends_on`, not `attached_node` |
| Column `data_type` | present, null unless declared in YAML | same | **key absent** | Types come only from `catalog.json` in practice |
| Column `config`, `doc_blocks`, `granularity` | absent | added in 1.9/1.10 | partly absent | Not read |
| `meta.owner` | in both `meta` and `config.meta` | same | same | Read through `config` |
| Model `docs`, `group`, `created_at`, `build_path` | present | present | **absent** | Not read |
| Source `patch_path`, `source_meta`, `quoting` | present | present | **absent** | Not read |
| Model `patch_path` (schema.yml) | `project://models/x.yml` | same | same | **Not carried into `RawCapsule`**, so the YAML file cannot be mapped to its models |
| Source `original_file_path` | the sources YAML | same | same | **Dropped by `_process_sources`** |
| `compiled_code` | `compile` and `docs generate` only | same | `compile` only | Not read by `DbtParser`; needed for column lineage |
| Top-level `functions` | absent | added in 1.11 | present | Not read |
| Versioned models (`version`, `latest_version`) | present | same | same | Both versions get **one URN**, see findings |

### Recommendation

- **Support manifest schema v12.** That is dbt Core 1.8 to 1.12 and Fusion 2.0.x, all tested here.
- **Gate on `dbt_schema_version`, not `dbt_version`.** Fusion reports `dbt_version` 2.0.9 with schema v12.
- **Newer than v12 (unknown):** warn in the status bar and the Problems panel, and parse best-effort. Also run structural checks: models present, `depends_on` present, and edge count consistent with `parent_map`. If a check fails, mark the index `unknown` rather than showing an empty graph. This matters because the parser never reads the schema version. A future manifest that renamed `depends_on` gives zero edges and zero errors (`tests/test_versions.py`).
- **Older than v12 (dbt 1.7 and earlier, end of life):** warn that the version is untested, and parse best-effort. Test v11 in Phase 1 only if beta users need it.
- **Record `dbt_version`, `dbt_schema_version` and `generated_at`** in each evidence record (design section 5.2).

## Suspicions

### 1. `_build_lineage` is O(columns × capsules) — **confirmed**

Lines 711–720 scan every capsule for every column. At 5,000 models that is 127,000 columns × 6,503 capsules. It costs 4.2 s on 3.13 and 14.3 s on 3.10: 70% and 87% of the cold index. That loop alone breaks the 5 s budget at 5,000 models. A `capsule_urn → capsule` dict (`spike/optimised.py`) cuts it to 0.04–0.05 s. The output is identical, checked on every fixture and every real manifest (`tests/test_optimised.py`, "Same graph" column).

### 2. `has_tests` / `test_count` are always False / 0 — **confirmed**

The parser counted zero tested models on all 42 real manifests. The test nodes show 9 of 13 jaffle-shop models and 1,999 of 2,000 / 4,999 of 5,000 synthetic models have tests. The backend fixture passes only because it is hand-written with `data_tests` lists on manifest columns. Real manifests never have that shape. DBT_001–004 and the "has tests" remaps would fail every model. Phase 1 must derive tests from test nodes: `attached_node` and `column_name` for models, and `depends_on.nodes` for sources, with `test_metadata.name` giving `unique`, `not_null` or `relationships`.

### 3. `parse()` is `async` but blocks — **confirmed**

It does synchronous file IO and CPU work with no `await` points. Under `await` in an asyncio loop (pygls runs one), the loop stalled for 0.7–0.8 s at 2,000 models and 1.8–2.2 s at 5,000. No hover, cancel or progress message is served during that time. `asyncio.to_thread` helps, but the GIL still causes stalls of 100–520 ms. **Index in a child process.** That also fixes memory: the child's 0.66–1.06 GB peak is released when it exits, and the server keeps a 106 MB graph.

### 4. Other problems found

| # | Finding | Evidence |
|---|---|---|
| a | `sql_column_parser` reads only the outermost `SELECT`. dbt style ends in `select * from final`, so it returns one `*` mapping with no source columns. | All 13 jaffle-shop models. 1,711 of 2,000 and 4,314 of 5,000 synthetic models. In the 686 models with an explicit final select, 1 of 12,836 source columns names a real relation. The rest name the final CTE. |
| b | `sql_column_parser` reports failures with `print()`. On a stdio language server, stdout is the JSON-RPC channel, so one bad SQL statement corrupts the protocol stream. | `tests/test_loader.py::test_column_parser_prints_to_stdout_on_failure` |
| c | `_build_column_lineage` reads `depends_on.columns`, `catalog_col_lineage` and `columns[].depends_on`. No real Core or Fusion manifest has any of them. It never produces an edge. | 0 column edges on all 42 manifests |
| d | Versioned models collide. The URN is `prefix:model:project.schema:name`, so `model.x.v1` and `model.x.v2` share one. One edge is silently dropped as a duplicate, and edges into a version are ambiguous. | 1 collision in every synthetic manifest (`graph.urn_collisions`) |
| e | No exposure handling at all, and `include_tests` in `DbtParserConfig` is never read. Exposures are needed for the CodeLens text and for impact. | `spike/graph.add_manifest_extras` adds them from the manifest, labelled |
| f | `dbt_parser.py` imports `orjson` with no fallback. Without it, the import fails. | `test_as_is_parser_hard_requires_orjson` |
| g | The backend declares `requires-python >=3.11`, but the five modules run and pass on 3.10. The as-is hot loop is 3.3–3.4× slower on 3.10 than on 3.13. After the fix, 3.10 is 14–22% slower. | Both test runs; cold index table |
| h | The backend's `sqlglot>=20.0.0` resolves to 30.27.0. sqlglot 30 made `schema` a required positional argument of `sqlglot.lineage.lineage()`. The `sql_column_parser` API surface still works. | Found while building the labelled alternative |
| i | The parser does not read `dbt_schema_version`. Unknown manifests fail silently. | `test_unknown_schema_version_fails_silently` |
| j | PII name patterns flag geographic columns (`city`, `country`, `state`) as `address`. That gave 14% of columns in the synthetic project, so expect noisy PII indicators. | 15,682 of 111,781 columns (synth-5000, manifest only) |

## Findings that should change the design or the Phase 1 plan

1. **Task 1.2 is not "move unchanged except for imports".** Fix `_build_lineage` (suspicion 1) and test derivation (suspicion 2). Add exposures, source file paths and `patch_path` to the parse output. Replace the parser's URN as identity with `unique_id`, which design section 5.1 already intends.
2. **Indexing must run in a child process.** It should not be an `await` or a thread. The server should hold only a compact graph and load it from the snapshot. Section 10 already says "Parsing runs in a worker"; make that a process. Then state the section 11 memory budget for the server process (met at 106 MB), and record the indexer's transient peak separately: 0.66–1.06 GB at 5,000 models today, depending on the JSON loader. If a lower peak is needed, Phase 1 should replace `ParseResult`'s per-column dataclasses with a leaner structure. Without that, the trimmed loader still peaks at 659–710 MB.
3. **Column lineage needs new code, not reuse.** Design section 6.1 says to reuse `sql_column_parser`; on dbt SQL it produces nothing usable. Use `sqlglot.lineage` over `compiled_code`, with a schema built from `catalog.json`. It resolved 91–99% of output columns to upstream tables in at most 0.35 s per model. Without a catalog, `select *` cannot be expanded, so those columns are `unknown`, which design section 6.1 already allows. Add this as a Phase 1 task. The `print()` calls must go regardless.
4. **Add a manifest version gate** with the structural checks above (Phase 1, task 1.2).
5. **Keep stdlib `json` as the fallback.** At 5,000 models it adds 0.1–0.4 s to the cold index and lowers peak RSS by 180–230 MB. orjson buys little here.
6. **Snapshot format: JSON is enough.** A stdlib-JSON snapshot loads faster than SQLite (0.10 s vs 0.15 s in-process at 5,000 models), and both are far inside budget. Use SQLite only if Phase 4 baselines need partial queries.
7. **Query budgets have huge headroom.** CodeLens and impact take microseconds, so no precomputed closures or caches are needed in Phase 1.
8. **"Refresh dbt artifacts" must be asynchronous with progress.** dbt itself takes 40–54 s (Core) or 12 s (Fusion) to parse 5,000 models.
9. **Pin sqlglot to a major version** in `uv.lock`. The lineage API changed between majors.
10. **Pin fixture repositories by SHA.** jaffle-shop's default branch has required dbt `>=2.0.0` (Fusion) since 15 September 2026. Earlier, Core-compatible commits are the only way to test Core.

## Fusion outcome: measured

- **Install.** dbt v2 (Fusion engine) 2.0.9 was installed with the official script (`public.cdn.getdbt.com/fs/install/install.sh`, sha256 `7ef1ab8d…724e`). It ran only inside a Docker container (`debian:bookworm-slim`, linux/aarch64, non-root user). No Fusion code ran on the host.
- **jaffle-shop.** At `5beb145b` (the dbt v2 commit), `deps`, `parse`, `seed` and `compile` all succeeded with a DuckDB profile. Fusion downloads its DuckDB driver on first use.
- **No connection needed for parse.** `dbt parse` with a dummy Snowflake profile (fake account and password) succeeded. `--static-analysis` is not a `parse` option in 2.0.9; `compile --static-analysis off` worked.
- **Synthetic projects.** 2,000 and 5,000 models parsed in 5.4 s and 12.4 s, and compiled in 8.3 s and 26.2 s. Fusion is stricter about YAML. It rejects top-level source `freshness` and `loaded_at_field` (they belong under `config:`) and wants generic-test `arguments:`. The generator's `--yaml-style v2` produces that form, which dbt Core 1.12 also accepts.
- **Schema.** Every Fusion manifest reports `dbt_schema_version` v12 and `dbt_version` 2.0.9.
- **Parser results.** `DbtParser` reads them with the same counts as Core. The field differences are in the table above: most notably, columns carry no `data_type`.

## How to reproduce

All commands run from `lineagepilot/spikes/s2-parse-perf/`. Generated data goes to `work/`, which is gitignored.

```sh
export UV_PYTHON_PREFERENCE=only-managed     # pyproject.toml also sets python-preference
uv python install 3.10 3.13                   # native arm64 builds; Homebrew ones here are x86_64
uv sync

# Tests: offline, small checked-in fixtures, both Python versions
uv run pytest
UV_PROJECT_ENVIRONMENT=work/venv-py310 uv run --python 3.10 pytest

# Real manifests (network: PyPI, GitHub, dbt hub). About 25 minutes.
uv run python scripts/setup_dbt_envs.py       # dbt-core 1.8.10 / 1.9.11 / 1.10.23 / 1.11.16 / 1.12.6
uv run python scripts/build_manifests.py      # jaffle-shop @ 7be2c58, synth-2000, synth-5000
docker build -t s2-fusion:latest scripts/fusion
uv run python scripts/fusion_attempt.py       # Fusion, in Docker only
uv run python scripts/make_fixtures.py        # optional: regenerates tests/fixtures/

# Benchmarks and analysis (about 15 minutes in total)
for v in 3.13 3.10; do
  E=work/venv-py${v/./}
  UV_PROJECT_ENVIRONMENT=$E uv run --python $v python -m spike.bench run
  UV_PROJECT_ENVIRONMENT=$E uv run --python $v python -m spike.bench memory
  UV_PROJECT_ENVIRONMENT=$E uv run --python $v python -m spike.bench quality
done
uv run python -m spike.versions               # results/manifest_versions.json
uv run python -m spike.report                 # Markdown tables from the results
```

`spike.bench` refuses to time on a non-arm64 interpreter on macOS. Every results entry records `platform.machine()`, the Python version, sqlglot and orjson.

## Inputs

| Input | Detail |
|---|---|
| jaffle-shop (Core) | `github.com/dbt-labs/jaffle-shop` @ `7be2c5838dbdeca8e915d4e46db70e910753d7f6` (2026-01-21). The last commit with `require-dbt-version >=1.5.0` and flat generic-test arguments. Resolved packages: dbt_utils 1.3.3, dbt_date 0.17.1, audit_helper @ `71817f0a`. Catalog from `seed`, `run`, `docs generate` on DuckDB. |
| jaffle-shop (Fusion) | Same repository @ `5beb145b00f5465ec759cfcdd9745e858818cf95` (2026-09-15, "Update jaffle-shop for dbt v2"). |
| Synthetic projects | `scripts/gen_project.py`, seed 7. Layers sources → staging (30%) → intermediate (45%) → marts (25%). 10–30 documented columns per model. `unique`, `not_null`, `relationships` and `accepted_values` tests in YAML (7,785 tests at 2,000 models, 19,429 at 5,000). Fan-in 1–4 with Pareto-skewed parents. 20 / 50 exposures, `meta.owner` on about half the models, one versioned model, a disabled model, 2 seeds and 1 snapshot. The SQL is valid DuckDB: `dbt run` builds every model, and `docs generate` writes a real catalog. |
| dbt runs | Each minor has its own venv under `work/dbt-envs/` (uv-managed CPython 3.11 arm64). Dummy `profiles.yml` under `work/profiles/`: DuckDB files only, no credentials. |
| Checked-in fixtures | `tests/fixtures/`: a 24-model synthetic project built with dbt 1.12, jaffle-shop from dbt 1.12, and jaffle-shop from Fusion 2.0.9. Each is under 1 MB with `macros` emptied (the parser never reads macros). Provenance is in `PROVENANCE.json`. |

## Limits of this spike

- The synthetic graph is broad and shallow. The median model has a downstream closure of 1. The largest closure is 651 nodes at 5,000 models, reached from a source through its staging model. Each source has only one direct child. Deeper real projects cost more per impact query, but BFS here runs at about 0.25 µs per node, so even a closure of the whole graph (6,553 nodes) would take about 2 ms.
- Timings come from one fast machine with a warm page cache. A laptop two or three times slower would still meet every budget with the fixes. It would leave the as-is 2,000-model cold index on 3.10 (3.1 s) close to the 5 s line.
- macOS only. Linux and Windows are not measured; S1 covers those platforms.
- Synthetic projects were compiled only on dbt 1.8, 1.10, 1.12 and Fusion; on 1.9 and 1.11 they were only parsed. jaffle-shop was compiled on all five Core minors.
- Fusion's own `compile` used `--static-analysis off` for the synthetic projects.
- "Graph webview, 500 nodes" belongs to Phase 3 and is not measured.

## Files

| Path | Purpose |
|---|---|
| `spike/loader.py` | Loads the five backend modules by file path under their `src.parsers.*` names, without `__init__.py`. Also holds the labelled stdlib and trimming JSON shims |
| `spike/graph.py` | Throwaway graph (nodes, file index, forward and reverse adjacency), queries, JSON and SQLite snapshots |
| `spike/optimised.py` | Labelled variant: overrides `_build_lineage` only |
| `spike/bench.py` | Benchmark functions, subprocess workers, orchestrator (`run`, `memory`, `quality`) |
| `spike/versions.py` | Per-manifest version facts and correctness checks |
| `spike/report.py` | Renders the result tables |
| `scripts/setup_dbt_envs.py`, `build_manifests.py`, `gen_project.py`, `fusion_attempt.py`, `fusion/Dockerfile`, `make_fixtures.py` | Reproduce every input |
| `results/bench_results.json` | All timings, keyed by Python version |
| `results/manifest_versions.json` | Version table, key differences, correctness checks; includes the Fusion attempt |
| `results/fusion_attempt.json` | Fusion commands, exit codes, schema versions |
| `tests/` | 52 tests: loader (no forbidden modules), generator shape, graph builder, optimised equivalence, benchmark harness, versions |
