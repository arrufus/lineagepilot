# Dagster held-out corpus

Twelve public repositories that run dbt from Dagster, labelled by hand with the links a careful person expects. The Dagster reader was measured on them once, after it was frozen, on 22 November 2026 (task 8.9).

| | |
|---|---|
| Repositories | 12, three from each of four searches |
| Links | 110: 109 `resolved`, 1 `unknown` |
| Marked `UNSURE:` | 2 |
| Candidates turned down | 16 |

The selection record is [repos.json](repos.json). The labels are in [labels/](labels/), one file per repository.

## The result

**On 110 links in 12 unseen repositories, the frozen reader got 16 right with its default setting: a miss rate of 85.5%. In the folders the labels name, it got 8 right: 92.7%. It made no false positives.**

It was never wrong. But for most links it said nothing at all:

| What the reader did | Default setting (the whole workspace) | | The labels' folders | |
|---|---|---|---|---|
| Correct: right unit, project and model set | 16 | 14.5% | 8 | 7.3% |
| Said "unknown", at the line that caused it | 25 | 22.7% | 18 | 16.4% |
| Said nothing at all | 69 | 62.7% | 84 | 76.4% |
| Wrong (a model set, a project, or a unit's identity) | 0 | | 0 | |
| False positives: a link where there was none | 0 | | 0 | |
| Unknowns no label expects | 14 | | 14 | |

So the reader would not mislead a reviewer: every link it gave was right. On repositories like these it would rarely help one, and, unlike the Airflow scanner on its first corpus, it mostly does not say what it could not read. The silent links break the definition of done ("every unknown at the line that caused it").

| Reader | Correct | Miss rate | Sample |
|---|---|---|---|
| **Phase 8, frozen, default setting** | **16 of 110** | **85.5%** | **12 repositories, labelled blind** |
| Phase 8, frozen, the labels' folders | 8 of 110 | 92.7% | The same, with `lineagePilot.dagster.paths` set to each label's folders |
| Default setting, without the 2 labels marked `UNSURE:` | 16 of 108 | 85.2% | |
| Default setting, without `edwinweber/dbt_duckdb_demo_public` | 16 of 57 | 71.9% | One repository holds 53 of the 110 links, and none of them is right |
| Airflow, Phase 5, for comparison | 17 of 106 | 84.0% | [../airflow/](../airflow/README.md) |
| Airflow, the Entry phase, for comparison | 15 of 35 | 57.1% | [../airflow/fresh/](../airflow/README.md#a-second-corpus-after-the-entry-fixes-lp-aax) |

By unit, with the default setting:

| Unit | Links | Correct | Miss rate |
|---|---|---|---|
| `dbt_assets` | 17 | 8 | 52.9% |
| `job` | 28 | 3 | 89.3% |
| `schedule` | 9 | 2 | 77.8% |
| `asset` (downstream of dbt) | 52 | 3 | 94.2% |
| `dbt_cli` | 4 | 0 | 100% |

Five repositories are fully right with the default setting: `srdp-hub/srdp` (4 of 4), `shawnmartin-py/plex-ingest` (5 of 5), and three with one link each. All 16 correct links are `heuristic`: no repository commits a dbt manifest, so each project is read through the stand-in manifest. In the default scan, one correct link has a different set of seeds or snapshots from its label.

## Why it misses

Every link's outcome is in [results/rescored.json](results/rescored.json). With the default setting, the 94 misses have these causes:

| Links | Cause | Reported as | Follow-up |
|---|---|---|---|
| 48 | Jobs (10), schedules (2) and downstream assets (36) over dbt assets the reader could not read: `edwinweber`'s, below | **Nothing** | LP-ios |
| 12 | An asset built by a factory imported from another workspace file, `build_dc_export_asset(...)` | **Nothing** (11), unknown under another name (1) | LP-9zx |
| 7 | Jobs made in a `for` loop by a factory | Unknown (`loop_or_factory`) | LP-8yt |
| 5 | Jobs that run dbt only through an asset that calls the dbt CLI, and their schedules | **Nothing** | LP-3d5 |
| 5 | dbt assets from a repository decorator that wraps `@multi_asset(specs=build_dbt_specs(...))` | Unknown (`python_dbt_call`, on the code location) | LP-9kf |
| 4 | Python that runs dbt itself, with a literal command (`dbt_cli`) | Unknown (`python_dbt_call`), by design (task 8.8) | LP-8yt |
| 4 | Two jobs that select a component's assets by group or key, where its `translation` sets the group with a Jinja `{% if %}`, and their schedules | Unknown (`custom_translator`; `not_literal` for the schedules) | LP-8yt |
| 4 | `@dbt_assets` whose manifest path comes from a helper, `path_config.DBT_DIR.joinpath(...)`, and the job and schedule over them | Unknown (`not_literal`) | LP-8yt |
| 3 | `load_assets_from_dbt_project` inside a `@repository` function, and the job and schedule over it | **Nothing** | LP-xpx |
| 1 | A short module trips "expression nesting too deep for the static evaluator", reported as a problem at line 0 | **Nothing** at the line | LP-odu |
| 1 | Expected unknown: a custom component type whose `AssetSpec` deps name dbt assets | **Nothing** | LP-3d5 |

**The labels' folders do worse than the default** by 8 links (LP-ms8). Set to a package folder such as `src/plex_ingest`, `lineagePilot.dagster.paths` leaves out the `pyproject.toml` with `[tool.dg]` and the `workspace.yaml` at the repository's root. Then `{{ project_root }}` cannot be filled, code locations are named after files, and jobs lose their location's dbt assets. A user who narrows the setting gets the worse figure.

**The unknowns no label expects** are 13 assets upstream of dbt in `DevelopmentFinanceObservatory/pipelines`, whose deps name keys that no asset is known to make, and one job in `srdp-hub/srdp` (LP-8yt).

### How to read the number

- **No false positives** keeps the reader under G2's ceiling of 2%, so by the plan's rule it may ship turned on. Whether to keep a reader with this miss rate is the product owner's decision at the phase exit (design_next_phase §12: "Drop a reader whose corpus result is poor rather than carry it").
- **One repository is half the corpus.** `edwinweber/dbt_duckdb_demo_public` has 53 of the 110 links, and 48 of them miss for one reason. Without it, the default setting gets 16 of 57.
- **The labels are demanding.** They count what a careful person can work out from the whole repository: factories read by hand, loops expanded, and a dbt CLI command's selection. The reader reads definitions as syntax, by design, and a factory or a loop is an unknown.
- **Only 1 link expected unknown,** so this says little about how the reader handles links it should not resolve.

## How it was run

1. **The labels came first.** They were committed unread by the reader's author (`8f0441f`, PR #22), before any Dagster reader code.
2. **Frozen, then read.** The reader's hash was recorded in [tool.py](tool.py) and committed (`111c47f`) before the scorer was written and before anyone on the scanning side opened a label or a repository:

   | | |
   |---|---|
   | Reader | `readers/dagster/`, `readers/common/`, and the YAML, JSON and annotation parsers, at `be8453e` |
   | `reader_sha256` | `ecb8dfdaebd77dd79e6a8c21e914b08614e5a466bd91240abaaa9961882d3136` |
   | Scorer | [tool.py](tool.py), written from the label format above (`861f586`) |
   | Run | [results/first.json](results/first.json), the first and only run of the reader on that hash |

3. **A scorer bug, found reading the run.** The scorer had left out the unknowns the reader puts on a whole code location, where it found no asset to put them on. They are still at a line, so they answer a label at that place. The fix (`ee91e2b`) changed only miss reasons and unknown counts: 5 links in each scan went from "not found" to "only an unknown at its place", and no link's outcome changed between correct and miss. [results/rescored.json](results/rescored.json) is the same reader on the same clones, scored with the fix, and is the result reported here.
4. **The reader was not changed.** Both runs record the frozen hash.

To reproduce, check out `ee91e2b` and:

```sh
export UV_PYTHON_PREFERENCE=only-managed
uv run --project packages/core python corpus/score.py fetch dagster
uv run --project packages/core python corpus/score.py hash dagster      # ecb8dfda…3136
uv run --project packages/core python corpus/score.py run dagster my-run
```

## After the freeze

The plan's definition of done forbids silence, so the four causes that left links silent were fixed before 0.4.0 (LP-ios, LP-9zx, LP-xpx, LP-odu, PR #36). Each pattern is now read, or an unknown at the line that causes it:

- **A decorator of the workspace's own that makes dbt assets** (`edwinweber`): a function under a decorator the reader does not know, whose body runs dbt through a dbt resource's `cli`, is a dbt asset definition that cannot be read (`wrapper`). The jobs, schedules and assets built on it, or that select every asset, or by key or by group, in its code location, are unknowns, not left out.
- **A factory imported from another file** (`DevelopmentFinanceObservatory`): each call is a definition of its own, in the calling file's code location. All of one factory's assets had one key, and all but one were dropped as duplicates.
- **A `@repository` function** (`flyanakin`): it is read as Dagster runs it, and what it returns is loaded. A legacy loader wrapped in `with_resources` takes its variable's name. A legacy loader's default groups changed between dagster-dbt versions, so a job that selects by group over one is an unknown when it matches none.
- **A package that imports from itself** (`lelouvincx`): `from . import x` in an `__init__.py` is the submodule `x`. The shared evaluator recursed until Python stopped it. The DAG scan's output is unchanged on both Airflow corpora but for 83 spurious records of this in a virtualenv one repository commits, and both score as before.

With the default setting:

| | Frozen | After |
|---|---|---|
| Correct | 16 | 18: `flyanakin`'s and `lelouvincx`'s definitions, which were silent |
| Unknown, at the line | 25 | 86 |
| Silent | 69 | 6: LP-3d5's two patterns, filed as P2 |
| Misses | 94 (85.5%) | 92 (83.6%) |
| False positives | 0 | 0 |
| Unknowns no label expects | 14 | 44 |

In the labels' folders, 10 are correct (90.9% missed) and 22 silent: LP-3d5's 6, and 16 jobs and schedules in `Odunpo`, `vbalalian`, `Cam0505`, `plex-ingest` and `flyanakin`. A narrowed setting leaves out the `workspace.yaml` or the `dg` project's `pyproject.toml` that names their code location, so each is put in a location of its own file, where there is no dbt asset for it to select (LP-ms8).

The extra unknowns no label expects, 30 with the default setting, are the cost of not being silent. In the labels' folders, where each one is recorded, there are 25 more (39, from 14):
- 11 are assets that `DevelopmentFinanceObservatory`'s factories make, whose dependencies cannot be read, which the duplicate keys had hidden;
- 10 are jobs in `edwinweber` that select by group, in a code location whose dbt asset definitions cannot be read, so they may run them, and 2 are its assets;
- 2 are elsewhere: an asset in `Odunpo` and a schedule in `flyanakin`.

None is a false positive.

[results/after-silent-fixes.json](results/after-silent-fixes.json) has this run, on the reader `efa4a424…27f2` at `046aa6f`. **It is not a new out-of-sample result:** the fixes were made after reading this corpus, so a reader tuned on it needs a fresh corpus to measure it, as the Airflow reader had. Nothing else was tuned: the other causes in [Why it misses](#why-it-misses) are filed, not fixed.

## How it was chosen

1. Four GitHub code searches, run on 2026-11-20 in this order, 50 hits each:
   1. `"from dagster_dbt import" dbt_assets language:Python`
   2. `DbtProjectComponent` in files named `defs.yaml`
   3. `load_assets_from_dbt_project language:Python` (the older dagster-dbt API)
   4. `dagster_dbt define_asset_job language:Python`
2. Hits were taken in the order returned. None was skipped for being easy or hard.
3. A repository was taken only if it uses Dagster with dagster-dbt (or runs dbt from Dagster code), holds its dbt project itself, is not in the `dagster-io` organisation, is not a fork, and is not a copy of a Dagster or dbt Labs tutorial or example.
4. As in the Airflow corpora, a repository whose files total over 200 MB at HEAD was turned down as too large to fetch for scoring.
5. Each search stopped at its third repository. The stopping points, the hits passed over and the 16 candidates turned down, with reasons, are in [repos.json](repos.json).
6. Each repository is pinned to the HEAD commit of its default branch when it was chosen.
7. The repositories were fetched as data, the way [../score.py](../score.py) `fetch` does. The sandbox refused `git -c` options, so fetch and checkout ran in fresh repositories with only git's `.sample` hooks and no global hooks path, and LFS smudging was turned off with `GIT_LFS_SKIP_SMUDGE=1`. Nothing from a repository was imported, executed or installed.

## How it was labelled

1. **Blind.** The labeller read only the corpus layout, the scorer, the Airflow corpora's method and format, and the repositories' own files. It did not read the reader, its design or its tests.
2. **Every unit in the Dagster code.** Each dbt asset definition, job, schedule, downstream asset and dbt CLI call found in the Dagster code is a link, registered or not. Units made in a loop are one link each.
3. **Dagster's rules, from Dagster's source.** dagster-dbt's defaults were read from its source at the pinned versions: the default select `fqn:*`, seeds and snapshots as assets, no asset for an ephemeral model, the default asset key `[schema, name]`, the old default group `fqn[1]`, the default schedule name `<job>_schedule`.
4. **dbt's rules, from the project files.** Model sets were worked out with a script of the labeller's own. It reads `dbt_project.yml`, SQL `config()` blocks, YAML properties and `ref()`/`source()` calls, and applies dbt's selection syntax. Every set was then checked by eye.
5. **Unknown only when the files cannot tell.** One link is `unknown`: its assets come from a service's state outside the repository.
6. The conventions for names, lines, factories and registration are listed in [repos.json](repos.json).

## Label format

One JSON file per repository, `labels/<owner>_<name>.json`:

```json
{
  "repo": "owner/name",
  "url": "https://github.com/owner/name",
  "sha": "<40 hex>",
  "license": "<SPDX id, or 'none found (read as data only)'>",
  "scan": {"paths": ["<folders holding the Dagster code, relative to the root>"], "workspace": "."},
  "labelled_by": "blind: read the repository's files only; no reader, no execution",
  "dagster_pins": {"dagster": "<as written, or null>", "dagster-dbt": "<as written, or null>"},
  "dbt_projects": ["<folders holding a dbt_project.yml>"],
  "out_of_scope": [{"file": "...", "what": "..."}],
  "links": [
    {
      "id": "<unit>:<name>, unique in this repository",
      "unit": "dbt_assets | job | schedule | asset | dbt_cli",
      "api": "<the construct, e.g. @dbt_assets, DbtProjectComponent, load_assets_from_dbt_project, define_asset_job, ScheduleDefinition, @asset deps>",
      "name": "<function, job, schedule or asset name, or the defs.yaml's folder>",
      "file": "<path>", "line": 0,
      "registered_in": "<the file whose Definitions (or repository) includes it, or null if it is not registered>",
      "project_path": "<the dbt project folder>", "package": "<the dbt project's name>",
      "selection_as_written": "<select/exclude, the job's selection or the command, verbatim; or null>",
      "deps_as_written": ["<for unit asset: its deps as written>"],
      "expect": "resolved | unknown",
      "unknown_reason": null,
      "models": ["<model names, not unique IDs>"],
      "other_nodes": [],
      "reason": "<how it was worked out; starts with UNSURE: when unsure>"
    }
  ]
}
```

The units:

| `unit` | What it is | `models` means |
|---|---|---|
| `dbt_assets` | A definition of dbt assets: an `@dbt_assets` function, a `DbtProjectComponent` in a `defs.yaml`, `load_assets_from_dbt_project`, or another dagster-dbt construct | the models whose assets it defines, after its select and exclude |
| `job` | A job that runs dbt: `define_asset_job` over dbt assets, or one that runs a `dbt_cli` asset | the models it materialises |
| `schedule` | A schedule whose job runs dbt | the models it runs |
| `asset` | A non-dbt asset that depends directly on a dbt asset | the models it depends on directly |
| `dbt_cli` | An op or asset that runs the dbt CLI itself, outside dagster-dbt's asset definitions | the models its command selects |

Seeds and snapshots go in `other_nodes`, as `seed.<name>` and `snapshot.<name>`. Sources are never models. A link with `"expect": "unknown"` has an `unknown_reason` and empty `models`. Sensors, automation conditions, asset checks, assets upstream of dbt and Dagster code unrelated to dbt are listed in `out_of_scope`, not in `links`.

Counts by unit: 17 `dbt_assets`, 28 `job`, 9 `schedule`, 52 `asset`, 4 `dbt_cli`. Four links are not registered in any `Definitions`.

## Limits

- **One labeller, not two.** Nobody checked the labels independently. 2 are marked `UNSURE:`.
- **Model sets are stand-ins.** No repository commits a dbt manifest, so every set comes from the project files through the labeller's script. Where a model's `ref()` sits inside a macro, the script cannot see it; no label here depends on such an edge.
- **One repository dominates.** `edwinweber/dbt_duckdb_demo_public` holds 53 of the 110 links, 36 of them assets made in loops by two factories. Read pooled results per repository too.
- **Almost no rightly unknown links.** Only 1 link expects `unknown`, so the corpus says little about how the reader handles links it should not resolve.
- **Some repositories pin nothing.** Two list `dagster` and `dagster-dbt` without versions, and two others pin different versions in different files. Their labels use the rules of the versions read; for these projects the versions give the same sets.
- **The fetch differed slightly** from `score.py` (hooks and LFS, above). The files checked out are the same.
- **Factories were resolved by reading them.** Labels for units built by repository helpers (export assets, job factories, model lists derived from constants) come from reading those helpers and the data they read in the repository, not from running them.
