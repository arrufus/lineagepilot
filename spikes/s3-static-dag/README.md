# Spike S3: static DAG analysis

## The question

Does AST-only scanning give correct task-to-dbt-model links on 3 public example DAG repositories? Every miss is listed, with its reason.

## Short answer

Yes, with one caveat about how much the result proves.

- On the 3 repositories, the scanner found all 84 expected links. 75 were resolvable, and all 75 were correct. The other 9 were legitimately `unknown`. There were **no misses and no false positives**.
- That result is **in-sample**. I built the scanner while reading these repositories, so it is an upper bound.
- A **held-out check** used 4 more repositories (9 links). They were labelled blind and scanned with the code frozen. The result was also 0 misses and 0 false positives. But 2 of the 5 resolvable links were right only through a fallback heuristic.
- A plain single-file scanner that only follows literals gets **56 of 75** links correct. That is a **22.6% miss rate**, and it gets **0 of 9** in cosmos-demo. Four features close that gap:
  - cross-module constants;
  - container-path remapping;
  - following helper calls;
  - a stand-in manifest.

  None of these four features is in design section 6.2.
- 71 of the 75 correct links have confidence `heuristic`, not `declared` or `inferred`.
- The selector engine agrees with real `dbt ls` (dbt-core 1.12.6) on **42 of 42** supported expressions. The 2 unsupported expressions come back as `unknown`.

## How to reproduce

All commands run from this folder, with uv-managed CPython.

```sh
cd spikes/s3-static-dag
export UV_PYTHON_PREFERENCE=only-managed

uv sync                                         # dev tools only (pytest, pytest-cov); runtime is stdlib-only
uv run pytest --cov                             # 132 tests, offline, no dbt needed; 88% coverage
UV_PROJECT_ENVIRONMENT=work/venv310 uv run --python 3.10 pytest   # same suite on Python 3.10

uv run python scripts/run_s3.py fetch           # shallow fetch of the pinned SHAs into work/ (data only)
uv run python scripts/run_s3.py run             # scan, score against ground truth, write results/

# the CLI on one repository
uv run python -m lineagepilot_spike_dag scan work/repos/astronomer-cosmos/dev/dags --workspace work/repos/astronomer-cosmos
uv run python -m lineagepilot_spike_dag select tests/fixtures/dbt/shop/target/manifest.json -s +customer_ltv

# selector engine against real dbt, on our own fixture project only
uv sync --group dbt
uv run --group dbt python scripts/validate_selectors.py   # dbt parse + 44 x dbt ls; rewrites the fixture manifest
uv run python scripts/validate_selectors.py --offline     # same comparison from the recorded dbt answers
```

`work/` is gitignored. It holds the fetched repositories and the Python 3.10 venv.

### Safety rules that were followed

- Third-party repositories are data. Fetching is `git fetch --depth 1` of a pinned SHA, with hooks off, no submodules and no LFS.
- No DAG file, script or requirement from them was imported, run or installed.
- No dbt command ran on their projects.
- The scanner reads each file as bytes, with a 2 MB cap, and calls `ast.parse`. A `SyntaxError` becomes an `Unresolved` record.
- A test proves DAG files are never imported. A fixture DAG writes a marker file on import. The test checks that the marker really appears when the file is executed. It then scans the file with `exec`, `eval`, `runpy.run_path` and `importlib.import_module` patched to fail, and an import spy installed. The marker never appears.
- dbt-core and dbt-duckdb are a dev dependency group. They ran only on `tests/fixtures/dbt/shop`, a project written for this spike, using a `:memory:` duckdb profile.
- Nothing third-party was copied into `tests/fixtures`. All fixtures are written here.

## Repositories

| Repository | SHA | Licence | Scanned |
|---|---|---|---|
| astronomer/astronomer-cosmos | `c92d0b23ff8cfc4c96f8b0bf2ec13f2f496d4fc2` | Apache-2.0 | `dev/dags` |
| astronomer/cosmos-demo | `169707b8b725fb3b852f7eb58dc6edff818c9a57` | Apache-2.0 | `dags` |
| astronomer/airflow-dbt-demo | `8da816d0c7db1a9bd012c051bbde21a5d0a612f5` | none in repo | `dags` |

All three suggested repositories exist and fit. I added a held-out set of 4 repositories, found with GitHub code search for `"dbt run" BashOperator`:

| Held-out repository | SHA | Licence | Why |
|---|---|---|---|
| konosp/dbt-airflow-docker-compose | `25f03315cd35228b3211fb46e28d763a997ca952` | Apache-2.0 | manifest-driven BashOperator DAGs |
| danielvdende/data-testing-with-airflow | `86600ee5dcffe54adb2e4b620f404ff2c13cd887` | Apache-2.0 | `BashOperator(cwd=...)` |
| GokuMohandas/data-engineering | `1d346983ad79575f1c9729ae6ec8574a6de37de7` | none in repo | `@dag`, `cd x && dbt run && dbt test` |
| saastoolset/dbt-core-airflow-template | `d4cd1d2414c3313fd94456fc63f6c84cff32bc1e` | GPL-3.0 | airflow-dbt-python operator, a broken Bash DAG |

Its Cosmos DAGs are copies of cosmos-demo, so only its two distinct files are held out.

## What the prototype does

The package is `src/lineagepilot_spike_dag`. It is stdlib-only at runtime.

| Module | Role |
|---|---|
| `scanner.py` | The AST walker. It is a small abstract interpreter over the AST: scopes, branch merging, DAG and task-group contexts, operators and dependencies. |
| `values.py` | The values it computes, each with a confidence: `declared`, `inferred` or `heuristic`. |
| `cosmos.py` | Reads DbtDag/DbtTaskGroup configuration and predicts Cosmos task ids. |
| `shell.py` | Finds dbt invocations in shell strings and argv lists. |
| `selector.py` | A subset of dbt node selection over a manifest. |
| `manifest.py` | Reads `manifest.json` as JSON. |
| `standin.py`, `miniyaml.py` | Build an approximate manifest from project files when none is committed. |
| `projects.py` | Discovers dbt projects, remaps container paths and picks a manifest. |
| `evaluate.py` | Scores a scan against the ground truth. |

### What it detects

- **DAGs**:
  - `with DAG(...)`, `x = DAG(...)`, and positional or keyword `dag_id`;
  - `@dag` functions, where the dag_id defaults to the function name;
  - `DbtDag`, including `with DbtDag(...) as dag`;
  - tasks attached with `dag=`.
- **Cosmos**:
  - `DbtDag` and `DbtTaskGroup`, with `group_id` positional, keyword or the default `dbt_task_group`;
  - `ProjectConfig(path | dbt_project_path= | manifest_path=, project_name=)`, positional or keyword;
  - `RenderConfig(select, exclude, selector, load_method, test_behavior, dbt_project_path, dbt_ls_path, group_nodes_by_folder, normalize_task_id, seed_rendering_behavior, source_rendering_behavior, should_detach_multiple_parents_tests)`;
  - `ExecutionConfig(execution_mode, dbt_project_path)`;
  - `operator_args`;
  - the Cosmos 0.x keywords `dbt_project_name`, `dbt_root_path` and dict-style `select`.
- **Load modes**:
  - `DBT_LS_FILE` reads the dbt ls file;
  - a local `manifest_path` is read, with container paths remapped;
  - a remote one (`s3://`, `gs://`, `abfs://`) gives `unknown`.
- **Cosmos single operators**:
  - every `Dbt<Command><Mode>Operator`, with `select=`, `models=`, `exclude=`, `selector=` and `dbt_cmd_flags=[...]`;
  - `.partial(...).expand(select=[...])` over a literal list;
  - the airflow-dbt and airflow-dbt-python operators.
- **Shell-style dbt calls**:
  - `BashOperator(bash_command=..., cwd=..., env={"DBT_PROJECT_DIR": ...})`, `SSHOperator`, `KubernetesPodOperator(cmds=..., arguments=...)`, `DockerOperator(command=str | list)` and `@task.bash`;
  - the parser handles:
    - `cd a && cd b && dbt ...`;
    - several dbt commands in one string;
    - global flags before the sub-command;
    - env prefixes, `uv run`, `python -m dbt`, `bash -c`;
    - `--select/-s`, `--models/-m`, `--exclude`, `--selector`, `--project-dir`, `--profiles-dir` and `--target`, in both `--x=y` and `--x y` forms;
  - Jinja in a selector or path makes that field `unknown`.
- **Dependencies**:
  - `>>` and `<<` with lists on either side;
  - `set_upstream` and `set_downstream`;
  - `chain(...)`, pairwise between equal-length lists;
  - `cross_downstream`.

  Operands must resolve to known operator, task-group or `@task` calls.
- **Constants**:
  - module and function-local names;
  - f-strings, `%` formatting and `.format()`;
  - `+` on strings and lists;
  - `str.join` and `.replace`;
  - `os.path.join`, `dirname` and `realpath`;
  - `Path(a, b) / "x"`, `.parent`, `.parents[n]`, `.resolve()` and `.joinpath()`;
  - `Path(__file__)` resolved against the file's real path;
  - dict literals with `**` spreads, and subscripts;
  - `if`/`try`/`match` branches merged, with values that differ becoming unknown;
  - `os.getenv("X", default)` and `os.environ.get(...)`, which use the default with confidence `heuristic` and a note (`Variable.get(..., default_var=)` is treated the same way);
  - `if __name__ == "__main__":` bodies are skipped.
- **Beyond the brief**, each one needed by the sample:
  - constants imported from other workspace modules (`from include.constants import X`, `from include import constants`, relative imports), read as AST only;
  - container-path remapping;
  - one-level inlining of local helper functions called with literals;
  - Airflow's file discovery rules: the safe-mode heuristic, `.airflowignore` (regexp) and skipping dbt project folders.
- **Dynamic patterns become `unknown` records with a reason**:
  - loops, while-loops and comprehensions that create tasks;
  - DAG factories;
  - functions never called at module level;
  - Jinja in selectors;
  - reading `manifest.json` at parse time;
  - workspace wrapper classes that build operators, and unknown calls with `dbt_*` keyword arguments;
  - `subprocess`/`dbtRunner` calls of dbt inside `@task` code;
  - shell scripts;
  - unsupported selector methods;
  - projects with no model files;
  - calls the scanner does not follow.

### Output

Each link mirrors design section 5.2:

```json
{"origin": "dag_static", "kind": "cosmos_task_group", "confidence": "heuristic",
 "location": {"file": "dev/dags/basic_cosmos_task_group.py", "line": 50, "col": 16},
 "dag_id": "basic_cosmos_task_group", "task_id": null, "group_id": "customers",
 "dbt_command": "cosmos_render", "project_path": "dev/dags/dbt/jaffle_shop",
 "manifest": {"kind": "committed", "dbt_version": "1.11.6", "generated_at": "...", "approximate": false},
 "select": ["path:seeds/raw_customers.csv"], "exclude": [], "selector": null,
 "resolved_models": [], "resolved_nodes": ["seed.jaffle_shop.raw_customers"],
 "task_id_pattern": "customers.<name>.run + customers.<name>.test (node has tests) | customers.<name>_run (no tests)",
 "tasks": [{"unique_id": "seed.jaffle_shop.raw_customers", "task_ids": ["customers.raw_customers_seed"], "operator": "dbt"}],
 "status": "resolved", "unknown_reasons": [], "notes": ["environment variable 'DBT_ROOT_PATH' assumed unset; used its default"]}
```

Unresolved records are `{file, line, col, reason, category, ...}`. A link with status `unknown` never carries `resolved_models`. Its partial set, if any, goes to `partial_models`. Dependencies are `task_depends_on` records between `airflow:<dag_id>.<task_id>` ids.

- **`resolved_models`** holds models only.
- **`resolved_nodes`** also holds the seeds and snapshots a command runs.
- **For `dbt test`**, the models are the selected models that have at least one test.

### Where model sets come from

1. A manifest the DAG declares (`ProjectConfig.manifest_path`), or a `--manifest` given to the CLI.
2. The project's committed `target/manifest.json`. A cheap drift check notes model files that are not in it.
3. Otherwise, an **approximate stand-in** built from the project files:
   - model, seed and snapshot files;
   - literal `ref()` and `source()` calls;
   - `config()` calls, `schema.yml` and `dbt_project.yml` folder configs;
   - versions, using dbt's rule that the latest version may live in the unsuffixed file;
   - tests.

   It is labelled `approximate`. On our fixture project, the stand-in equals the real manifest for every model, seed, snapshot and source: the same edges, tags and materialisations. It also agrees with `dbt ls` on 38 of 38 non-YAML-selector expressions.

dbt-loom nodes have no file of their own. They are external and never linked.

## The Cosmos task-id mapping

I read this from Cosmos source as text, at the pinned SHA:
- `cosmos/airflow/graph.py`: `_get_task_id_and_args`, `create_task_metadata`, `generate_task_or_group`, `_add_test_tasks`, `create_task_groups_based_on_folder`, `calculate_detached_node_name`;
- the defaults in `cosmos/config.py` (`RenderConfig`) and `cosmos/airflow/task_group.py`.

I then checked it against the ids Cosmos's own `tests/airflow/test_graph.py` asserts, and against 7 hand-labelled links. All 7 matched.

`<name>` is the node's resource name from its unique_id, with `.` replaced by `_`. So the versioned `model.p.orders.v2` becomes `orders_v2`. `P` is the prefix:
- `""` in a `DbtDag`;
- `<group_id>.` in a `DbtTaskGroup`, plus the prefixes of any enclosing `TaskGroup`.

The default `group_id` is `dbt_task_group`.

| RenderConfig | Model with at least one non-detached test | Model without tests | Seed | Snapshot | Extra tasks |
|---|---|---|---|---|---|
| default `test_behavior=AFTER_EACH` | TaskGroup `P<name>` holding `P<name>.run` and `P<name>.test` | `P<name>_run` | `P<name>_seed` (or `.seed`/`.test` if it has tests) | `P<name>_snapshot` | none |
| `TestBehavior.BUILD` | `P<name>_model_build` | `P<name>_model_build` | `P<name>_seed_build` | `P<name>_snapshot_build` | none |
| `TestBehavior.AFTER_ALL` | `P<name>_run` | `P<name>_run` | `P<name>_seed` | `P<name>_snapshot` | `P<project_name>_test` after the leaves |
| `TestBehavior.NONE` | `P<name>_run` | `P<name>_run` | `P<name>_seed` | `P<name>_snapshot` | none |

Modifiers:
- **Folder grouping.** `group_nodes_by_folder=True` adds folder task groups from `original_file_path`, for example `models.staging.stg_x_run`. With Cosmos's default naming (`enable_hierarchical_naming_for_group_nodes_by_folder=False`), groups are cached by the **bare folder name**. Same-named folders collapse into the first group created, so the ids depend on node order. Cosmos's own test expects `gen2.models.child_run` for `gen3/models/child.sql`.
- **Ephemeral models** render as an `EmptyOperator` with the same id (`ephemeral_models_as_empty_operator=True`).
- **Seed rendering.** `seed_rendering_behavior=NONE` drops seed tasks. `RENDER_ONLY` makes them `EmptyOperator`s.
- **Detached tests.** `should_detach_multiple_parents_tests=True` (with AFTER_EACH or BUILD) moves tests with more than one parent to `P<test_name>_test`. The parent then counts as tested only if it has other tests.
- **`normalize_task_id`** is a callable, so the ids cannot be predicted. The models stay resolved, and the per-task ids are reported as `null`.
- **Watcher execution modes** add `P dbt_producer_watcher`, which runs `dbt build` over the whole selection. The per-model tasks watch it.
- **Commands.** Each model task runs `dbt run --select fqn:<fqn>` (Cosmos 1.11+, or `--models` with `AIRFLOW__COSMOS__PRE_DBT_FUSION`).
- **"Has tests"** comes from the manifest's test nodes. Tests removed by `exclude` do not count. So task ids need test information, not just models.

## Results

### Per repository (in-sample)

| Repository | DAG files scanned | Expected links | Resolvable | Expected unknown | Found | Correct | Misses | Miss rate | Legit unknowns | False positives |
|---|---|---|---|---|---|---|---|---|---|---|
| astronomer-cosmos | 41 | 68 | 61 | 7 | 68 | 61 | 0 | 0% | 7 | 0 |
| cosmos-demo | 9 | 9 | 9 | 0 | 9 | 9 | 0 | 0% | 0 | 0 |
| airflow-dbt-demo | 3 | 7 | 5 | 2 | 7 | 5 | 0 | 0% | 2 | 0 |
| **Total** | **53** | **84** | **75** | **9** | **84** | **75** | **0** | **0%** | **9** | **0** |

"Correct" means four things are right: the DAG, the task or group id, the dbt project, and a model set equal to the hand-labelled set. The labels are in `results/ground_truth/<repo>.json`, with one line of reasoning per link.

Out-of-scope dbt tasks were seen but not counted: 3 docs tasks, 1 Kubernetes docs task, 1 clone and a run-operation loop. The scan of astronomer-cosmos (41 files) takes about 80 ms.

The in-sample links by kind:

| Kind | Links |
|---|---|
| `DbtDag` | 33 |
| `DbtTaskGroup` | 30 |
| Cosmos single operators | 14 |
| `BashOperator` | 5 |

No sampled repository calls dbt through a raw `KubernetesPodOperator` or `DockerOperator`. Those paths are tested only on fixtures.

Confidence of the 75 correct links:

| Confidence | Links | Why |
|---|---|---|
| `inferred` | 4 | literal paths and a committed or declared manifest |
| `heuristic` | 71 | `os.getenv("DBT_ROOT_PATH", default)` in nearly every Cosmos example, container-path remapping, or a stand-in manifest |

### Held out (scanner frozen before reading the repositories)

| Repository | DAG files | Expected | Resolvable | Correct | Legit unknowns | Misses | False positives |
|---|---|---|---|---|---|---|---|
| konosp/dbt-airflow-docker-compose | 2 | 3 | 0 | 0 | 3 | 0 | 0 |
| danielvdende/data-testing-with-airflow | 1 | 2 | 2 | 2 (by fallback) | 0 | 0 | 0 |
| GokuMohandas/data-engineering | 2 | 2 | 2 | 2 | 0 | 0 | 0 |
| saastoolset/dbt-core-airflow-template (2 files) | 2 | 2 | 1 | 1 | 1 | 0 | 0 |
| **Total** | **7** | **9** | **5** | **5** | **4** | **0** | **0** |

The frozen scanner's source hash is recorded in `results/heldout_frozen_run.json`. Two caveats, both reported as near-misses below:
- The 2 danielvdende links were right only because the repository has a single dbt project. The frozen scanner ignored `BashOperator(cwd=...)`. I fixed that after the hold-out.
- The broken saastoolset DAG came out `unknown` for an imprecise reason.

The hold-out has only 9 links, so it is weak evidence on its own.

### What each feature buys (ablation, in-sample)

| Scanner variant | Correct (of 75) | Misses (of 84) | Miss rate |
|---|---|---|---|
| Full prototype | 75 | 0 | 0% |
| Without cross-module constant imports | 66 | 9 | 10.7% |
| Without container-path remapping | 60 | 15 | 17.9% |
| Without helper-call inlining | 71 | 4 | 4.8% |
| Single-file literals only (all three off) | 56 | 19 | 22.6% |

Per repository, the literals-only variant gets:

| Repository | Correct |
|---|---|
| astronomer-cosmos | 56 of 61 |
| cosmos-demo | 0 of 9 |
| airflow-dbt-demo | 0 of 5 |

## Selector agreement with dbt

I compared the engine with `dbt ls --resource-type model --output json` on `tests/fixtures/dbt/shop`. That project has:
- 16 model nodes in 16 files, including a Python model and a two-version model;
- 2 seeds, 1 snapshot, 3 sources, tests including a relationships test, an exposure and 4 YAML selectors;
- fan-in and fan-out, ephemeral, incremental, view and table models.

The tools were dbt-core 1.12.6 and dbt-duckdb 1.11.1.

| Expressions | Count |
|---|---|
| Total | 44 |
| Supported, agree with dbt | **42** |
| Supported, disagree | 0 |
| Supported, engine said unknown | 0 |
| Unsupported (`exposure:`, `test_name:`), engine said unknown | 2 of 2 |
| Same expressions over the stand-in manifest (non-YAML) | 38 of 38 agree |

The cases cover:
- bare names, folder names (unscoped fqn), full fqn, `fqn:` and `*x*`/`x_*` wildcards;
- `+`, `n+`, `+n`, `n+x+m` and `@`, including `@` through a relationships test;
- `tag:` with wildcards, `path:` (folder, file, glob), default path and file methods, `file:`, `config.materialized:` and `resource_type:`;
- `source:x+` and `source:x.y+`, a source tag, `package:`, and versioned names in both forms;
- union (in one argument and across arguments), intersection, `--exclude` (one and two);
- 4 YAML selectors.

All per-case results are in `results/selector_agreement.json`.

Two dbt details had to come from dbt-core's source, which I read as text:
- since dbt 1.10, a bare wildcard such as `stg_*` also matches the node's leaf name;
- `@` follows test edges, so `@fct_orders` pulls in the other parent of a relationships test.

## The full miss list

**In-sample, full scanner:** none.

**Held-out, frozen scanner:** none. There are two near-misses:

1. `danielvdende … awesome_dag.dbt_run` and `dbt_test` were correct only through the single-project fallback. `bash_command='dbt run'` relies on `BashOperator(cwd=DBT_DIRECTORY)`, which the frozen scanner did not read. In a repository with two dbt projects this would have been `unknown`, which is not wrong, but it is not a hit either. Fixed after the hold-out.
2. `saastoolset … dbt_bash.dbt_run` came out `unknown` because the scanner does not model `str + Path`. The DAG really fails at import, with a TypeError. The outcome is right, but the reason does not name the TypeError. A static scanner can link tasks of DAGs that never load.

**Misses of simpler scanners**, which are the misses the design as written would have. All 19 are in the ablation above:

| Cause | Links missed | Where |
|---|---|---|
| Project path comes from another workspace module (`from include.constants import jaffle_shop_path`) | 9 | every cosmos-demo DAG |
| Project path is a container path (`/usr/local/airflow/dbt[/jaffle_shop]`) or an in-image relative path (`dags/dbt/jaffle_shop`) | 15 | all 9 cosmos-demo, all 5 airflow-dbt-demo, `jaffle_shop_kubernetes.load_seeds` |
| Task groups built by a local helper called with literals (`_task_group(...)`) | 4 | `cross_project_bidirectional_dag` |

Three more misses were found and fixed while building. Each would have been a wrong model set:

| Problem | Where | Fix |
|---|---|---|
| Versioned models: the latest version lives in the unsuffixed file | `example_model_version` | rule taken from dbt-core's schema parser |
| A project with no model files gave an empty set silently | `performance_dag`, whose models are generated by a test | now `unknown` |
| The bare-wildcard leaf rule was missing | `*customers*` | rule from dbt-core |

## Unknowns

There are 21 unresolved records across the 7 repositories. Each was reviewed by hand, and all 21 are legitimate. The reviews are in `unresolved_review` in each ground-truth file and in `results/s3_results.json`.

| Cause | Records | Where |
|---|---|---|
| Remote Cosmos manifest (`s3://`, `gs://`, `abfs://`) | 6 | `cosmos_manifest_example`, `cosmos_manifest_selectors_example` |
| Tasks built in loops over `manifest.json` read at parse time | 9 | airflow-dbt-demo `dbt_advanced` (4), konosp (4), `example_tasks_map` (1, a loop over a runtime Cosmos object) |
| Wrapper class that builds tasks from the manifest, and the edges to its groups | 2 | `dbt_advanced_utility` |
| run-operation tasks created in a loop (out of scope) | 1 | `cosmos_seed_dag` |
| Models generated at test time | 1 | `performance_dag` |
| Edge to a task that exists only if a provider imports | 1 | `cosmos_manifest_example` |
| DAG that fails at import | 1 | saastoolset `dbt_bash` |

## Known limits

These would cause misses in other repositories.

- **Selector semantics.** The engine follows dbt's semantics. In `LoadMode.DBT_MANIFEST`, Cosmos uses its own selector (`cosmos/dbt/selector.py`), which may differ from dbt for some expressions. That was not measured.
- **Inputs dbt sees but a static read does not.** `LoadMode.AUTOMATIC` without a manifest runs `dbt ls` at parse time, with the project's `vars`, env vars and installed packages. A static read ignores `ProjectConfig(dbt_vars=...)`, `enabled` configs driven by Jinja, and package models in stand-in manifests.
- **Unsupported selector methods** give `unknown`: `state`, `result`, `source_status`, `exposure`, `metric`, `group`, `access`, `version`, `semantic_model`, `saved_query`, `unit_test`, `test_name` and `test_type`.
- **Not followed:**
  - functions imported from other modules;
  - more than one level of helper calls into `include/`;
  - loops, even over literal lists;
  - `.expand()` over non-literals;
  - `normalize_task_id`.
- **Environment variables** are assumed unset, and their defaults are used. That is right for these examples, but it is a guess.
- **Stand-in manifests** miss refs made inside test macros. For example, `example_multiple_parents_test`'s custom test refs `model_a`, so its detached test task is not predicted.
- **`.airflowignore`** is read in regexp syntax only. Airflow 3's default is glob syntax.
- **Raw `KubernetesPodOperator`/`DockerOperator` dbt calls** are validated only on fixtures. The entrypoint is assumed to be dbt when `cmds` is absent.

## Findings that should change the design (section 6.2) or the Phase 5 plan

1. **Literal-only operands are not enough.** Design 6.2 says "where the operands are literals". In the sample, a literal-only scanner misses 22.6% of links, and 100% in two of the three repositories. The MVP scanner needs three things:
   - constants imported from workspace modules, read as AST only and kept inside the workspace;
   - `Path(__file__)` arithmetic and env-var defaults;
   - one-level inlining of local helpers called with literals.

   Add these to task 5.1.
2. **Container-path remapping is a first-class requirement.** Astro images put the project at `/usr/local/airflow`, and Kubernetes DAGs use in-image relative paths. The scanner should keep the suffix-match heuristic and mark the link `heuristic`. It should also accept an explicit path map in the annotation file (task 5.2), such as `/usr/local/airflow -> .`. This mapping, not selector logic, was the largest single cause of misses.
3. **Expect "heuristic" to be the normal confidence.** 71 of 75 correct links are `heuristic`. The Change Impact Review must show heuristic links by default and say why. Hiding them would hide most real links.
4. **Cosmos links need the manifest's tests, not only its models.** The model → task-id mapping depends on several things:
   - whether a node has tests;
   - `test_behavior`;
   - folder grouping, which is order-dependent with Cosmos's default naming;
   - `normalize_task_id`, which cannot be predicted;
   - watcher producers, `AFTER_ALL` extra tasks and ephemeral `EmptyOperator`s.

   Store the group-level `orchestrates` edge (DbtDag or DbtTaskGroup → models) as the primary evidence. Derive per-task ids from it, and mark them version-specific: Cosmos 1.11+ selects with `fqn:`. Pin the mapping to a Cosmos version range, and re-check it against Cosmos's own `tests/airflow/test_graph.py` when Cosmos is upgraded.
5. **Exclude dbt-loom external nodes.** Downstream manifests contain injected upstream nodes with no file. Linking them would produce false positives. Treat them as external.
6. **Pin selector semantics to the manifest's dbt version, and keep a `dbt ls` conformance test in CI.** dbt changed wildcard matching in 1.10. This spike's fixture and recorded answers can seed that test.
7. **Name the common unknown categories in task 5.4:**
   - remote manifest;
   - tasks generated from a manifest at parse time (the classic "explode the manifest" BashOperator DAG, common in older repositories);
   - workspace wrapper class;
   - loop or factory;
   - generated project;
   - unsupported selector;
   - Jinja;
   - DAG not loadable.

   Each needs a one-line explanation in the UI and an annotation escape hatch.
8. **Report unfollowed calls at the call site.** The ablation showed that a call the scanner does not follow can leave nothing at the place a user looks. The prototype now emits a `call_not_followed` record when the function builds operators. Keep that rule in 5.1.
9. **Match Airflow's own file discovery:** the safe-mode heuristic, `.airflowignore`, and skipping dbt project folders. Without it, dbt Python models and include modules would be scanned as DAGs.
10. **The S3 gate evidence is in-sample.** The Phase 5 exit criterion ("miss rate measured on the S3 repositories") will be biased in the same way. Add a held-out corpus to Phase 5 and label it before tuning: 10 or more repositories, with more BashOperator, KubernetesPodOperator and DockerOperator DAGs than this sample has.
11. **Define `dbt test` linking precisely.** This spike links a test task to the selected models that have tests. dbt's eager indirect selection also runs tests that belong to other models, such as relationships tests. Decide what the impact review should show.
12. **Prefer committed manifests, and label stand-ins approximate.** The stand-in matched dbt exactly on our fixture. It still cannot see packages, vars or Jinja-generated configs, and it reported 0 models for a generated project. That last case now gives `unknown` rather than an empty set.

## Files

```
pyproject.toml, uv.lock          uv project; runtime deps: none
src/lineagepilot_spike_dag/      the prototype (CLI: python -m lineagepilot_spike_dag scan|select)
tests/                           132 tests; fixtures written for this spike:
  fixtures/dags/                 one DAG file per pattern, a side-effect DAG, a syntax error, .airflowignore
  fixtures/include/              workspace modules for cross-module constants and a wrapper class
  fixtures/dbt/shop/             dbt project + generated target/manifest.json (macros/docs trimmed, paths scrubbed)
  fixtures/dbt/shop_dbt_ls.json  recorded dbt ls answers for 44 expressions
  fixtures/dbt/mini/             project without a manifest (stand-in features)
scripts/run_s3.py                fetch (pinned SHAs) and run (scan + score + ablations)
scripts/validate_selectors.py    dbt parse/ls on our fixture; engine and stand-in agreement
results/s3_results.json          metrics per repo, misses, false positives, unknowns with review, ablations, held-out
results/ground_truth/*.json      hand labels, one reason per link; heldout/ for the held-out set
results/heldout_frozen_run.json  held-out result of the frozen scanner, with its source hash
results/selector_agreement.json  engine vs dbt ls, per expression
results/scans/*.json             raw scanner output per repository
```
