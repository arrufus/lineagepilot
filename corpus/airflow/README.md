# Held-out DAG corpus (task 5.5, the Phase 5 exit)

How well the Airflow scanner does on repositories it had never seen. Spike S3's 0% miss rate was in-sample: the scanner was built while reading those repositories, and its own held-out check had only 9 links. This measures it properly.

There are two corpora. The first, in this folder, measured the Phase 5 scanner. The second, in [fresh/](fresh/), was labelled after the Entry phase's fixes (LP-aax), and is reported in [A second corpus](#a-second-corpus-after-the-entry-fixes-lp-aax).

## The result

**On 106 links in 17 unseen repositories, the frozen scanner got 17 right: a miss rate of 84.0%.**

It was rarely wrong, and it usually said so when it could not tell:

| What the scanner did | Links | |
|---|---|---|
| Correct: right DAG, task, project and model set | 17 | 16.0% |
| Said "unknown", at the line that caused it | 78 | 73.6% |
| Said nothing at all | 9 | 8.5% |
| Wrong (one model set, one task's DAG) | 2 | 1.9% |
| False positives: a link where there was none | 0 | |

So on repositories like these, the review's Airflow section is mostly a list of tasks it could not read, not a list of tasks it linked. Nothing suggests it would mislead a reviewer. It would often fail to help one.

The 9 silent links broke the plan's definition of done, so they were fixed afterwards (see [After the freeze](#after-the-freeze)). Everything else stands as measured.

**This corpus is now in-sample.** It has been read, and the Entry fixes were aimed at its three largest causes, so a figure on it no longer counts as held out. The second corpus is the out-of-sample one.

| Scanner | Correct | Miss rate | Sample |
|---|---|---|---|
| S3, in-sample | 75 of 75 resolvable, 84 links | 0% | Built while reading them |
| S3, its small hold-out | 5 of 5 resolvable, 9 links | 0% | 4 repositories, chosen by a search for `"dbt run" BashOperator` |
| **Phase 5, frozen** | **17 of 106** | **84.0%** | **17 repositories, labelled blind** |
| Phase 5, default DAG folder (`**/dags`) | 15 of 106 | 85.8% | The same, with no setting touched |
| After the Entry fixes, in-sample | 55 of 106 | 46.2% | This corpus, after it was read: [results/after-entry-fixes.json](results/after-entry-fixes.json) |
| **0.2.0 and Entry, on the second corpus** | **15 of 35** | **57.1%** | **12 other repositories, labelled blind: [fresh/](fresh/)** |

## Why it misses

Every miss, with its cause, is in [results/adjudication.json](results/adjudication.json). Three causes make up 62 of the 89, and each is a general gap rather than one repository's quirk:

| Links | Cause | Reported as | Follow-up |
|---|---|---|---|
| 35 | `os.environ["AIRFLOW_HOME"]` has no default, so the project path is unknown, though its tail names the project | Unknown | LP-0o1 |
| 16 | A helper imported from another workspace file builds the operator | Unknown | LP-z2x |
| 11 | A container path such as `/app` matches no project by its tail; a mount names the host folder | Unknown | LP-c05 |
| 8 | Repo-local operator subclasses, and factories driven by YAML | Unknown | LP-2cd |
| 7 | A pod whose dbt command is in its image (4); `cmds=["/bin/bash", "-cx"]`, not recognised as a shell (3) | **Nothing** | LP-x29 |
| 4 | `cd ${AIRFLOW_HOME}` with several projects in the workspace | Unknown | LP-x29 |
| 3 | A shell variable after a selector, such as `--select x ${FULL_REFRESH_FLAG}` | Unknown | LP-x29 |
| 2 | A factory from a library. Expected unknown, but it must be said | **Nothing** | Fixed |
| 2 | A date in an f-string makes the command unknown (1); a task with no `dag=` takes its DAG from the task before it (1, wrong) | Unknown; wrong | LP-x29, LP-4aw |
| 1 | The stand-in manifest counted a test with `enabled: false`. A real manifest leaves it out | Wrong | LP-ykj |

The shell-variable rule (3 links) was added in Phase 5, before the freeze, by reasoning about a selector that would silently select nothing. Here it was too strict.

### How to read the number

- **The labels are demanding.** They count what a careful person can work out from the whole repository. For 12 links the answer is outside the DAG files altogether: an image's entrypoint, a setup script, a YAML config. No scanner of DAG files reaches those. They stay misses, because a person can follow them.
- **One repository is a third of the corpus.** `astronomer/cosmos-ebook-companion` has 34 of the 106 links, and all 34 miss for one reason. Without it the scanner gets 17 of 72, a miss rate of 76.4%. Four repositories are fully right, and a fifth has 4 of 5.
- **Only 2 links were expected to be unknown,** so this says little about how the scanner handles legitimately unknown links.
- **All 17 correct links are `heuristic`,** as S3 predicted.
- **Without the 10 labels marked `UNSURE:`**, the scanner gets 16 of 96, a miss rate of 83.3%.

### By operator

| Operator | Correct | Links |
|---|---|---|
| `BashOperator` | 12 | 18 |
| `DockerOperator` | 4 | 35 |
| `KubernetesPodOperator` | 0 | 7 |
| `DbtDag` | 0 | 18 |
| `DbtTaskGroup` | 1 | 16 |
| `DbtBuildLocalOperator` | 0 | 2 |
| Repo-local subclasses (`DbtK8Operator`, `DbtSpark*`) | 0 | 8 |
| A library's factory (`compile_dbt_af_dags`) | 0 | 2 |

### By repository

| Repository | Correct | Links |
|---|---|---|
| astronomer/cosmos-ebook-companion | 0 | 34 |
| dataengineerankur/dms-snowpipe-dbt-airflow | 0 | 16 |
| 21voidx/F1_RealtimeReplay_Historical_Project | 1 | 14 |
| dungtran174/ecommerce-lakehouse-platform | 6 | 6 |
| Som-Energia/somenergia-jardiner | 4 | 5 |
| anhhoangdev/KonohaDataPlatform | 0 | 4 |
| feast-dev/feast-workshop | 0 | 4 |
| teamdatatonic/dop | 0 | 4 |
| datamindedbe/eu-data-platform | 0 | 3 |
| sungchun12/airflow-toolkit | 0 | 3 |
| halltape/HalltapeETL | 2 | 2 |
| Kevin-Nduati/My-Spotify-Wrapped | 0 | 2 |
| Nathnael12/Datawarehouse | 2 | 2 |
| Toloka/dbt-af | 0 | 2 |
| UsmanAdnan234/flight-elt-airflow-dbt-snowflake | 2 | 2 |
| zhfem05-star/steam_analysis | 0 | 2 |
| feluelle/kind-data-platform | 0 | 1 |

## How it was done

1. **Labelled blind.** An agent that was told not to read the scanner, its spike or the design chose the repositories and labelled them, from their own files only. It worked out model sets with a script of its own that applies dbt's selection rules to project text, and checked them by eye. Nothing from the repositories was run.
2. **Chosen without looking for easy ones.** GitHub code search for `BashOperator`, `KubernetesPodOperator`, `DockerOperator` and Cosmos with dbt, taking hits in the order returned. S3's repositories and their copies were excluded. The queries, the stopping points and the 7 rejected candidates are in [repos.json](repos.json).
3. **Frozen, then read.** The scanner was committed, and its hash recorded, before anyone on the scanning side opened a label or a repository:

   | | |
   |---|---|
   | Commit | `5a50fc8` |
   | Scanner sha256 | `78b914834e14f62a11a7f9f620f73d95959e14d129d56c6a06e99fc2a6bf0a07` |
   | Run | [results/first.json](results/first.json), the first and only run on that hash |

4. **Scored by S3's definition.** A link is correct when the DAG, the task or group, the dbt project and the model set all match. The scorer, now [../score.py](../score.py) and [tool.py](tool.py), was written before the labels were read.
5. **Adjudicated, without changing a label.** Each miss was read against its DAG file afterwards, to find its cause. No label was corrected, and no miss was excused.

### Limits

- **One labeller, not two.** Nobody checked the labels independently. 10 are marked `UNSURE:`.
- **The blind was not perfect.** The agent's report, read before the freeze, mentioned in passing that some links used repo-local subclasses or a library's factory. No scanner support was added for either. The scanner's author had also read S3's own held-out repositories, which are excluded here.
- **Manifests are mostly stand-ins.** Public repositories rarely commit `target/manifest.json`, so most model sets come from the approximate manifest built from project files. A real workspace has a real manifest, which is what the extension uses.

## After the freeze

One change, made because the plan's definition of done forbids silence: a task that mentions dbt and shows no readable command is now an unknown, not nothing. It only adds unknowns (commit `64ce438`).

| | Frozen | After |
|---|---|---|
| Correct | 17 | 17, the same links |
| Silent | 9 | 0 |
| Misses | 89 (84.0%) | 87 (82.1%): 2 expected-unknown links now match |
| False positives | 0 | 0 |

[results/after-silence-fixed.json](results/after-silence-fixed.json) has this run. It is not a new out-of-sample result. Nothing else was tuned: the causes above are filed, not fixed, so that a fresh corpus can measure the fixes.

## A second corpus, after the Entry fixes (LP-aax)

The Entry phase ([implementation_plan_next_phase.md](../../docs/implementation_plan_next_phase.md)) fixed the three largest causes above: environment variables with no default (LP-0o1), helpers imported from other files (LP-z2x) and container paths (LP-c05). A fresh corpus, in [fresh/](fresh/), was labelled blind while they were built, to measure the scanner again.

**On 35 links in 12 unseen repositories, the frozen scanner got 15 right: a miss rate of 57.1%, with no false positives. The 0.2.0 scanner, from before the fixes, gets the same 15.**

| Scanner, on the second corpus | Correct | Miss rate | Said nothing | Wrong | False positives |
|---|---|---|---|---|---|
| 0.2.0, before the Entry fixes ([baseline-0.2.0.json](fresh/results/baseline-0.2.0.json)) | 15 of 35 | 57.1% | 4 | 1 | 0 |
| **Entry, frozen** ([first.json](fresh/results/first.json)) | **15 of 35** | **57.1%** | **4** | **1** | **0** |
| Entry, after the silence fixes ([after-silence-fixed.json](fresh/results/after-silence-fixed.json)) | 15 of 35 | 57.1% | 0 | 1 | 0 |
| Entry, frozen, default DAG folder (`**/dags`) | 8 of 35 | 77.1% | | | |
| Entry, frozen, without the 3 labels marked `UNSURE:` | 15 of 32 | 53.1% | | | |

All 15 correct links are `heuristic`.

### How to read it

- **This sample does not measure the Entry fixes.** One of its 35 links passes through one of them: `cd $AIRFLOW_HOME/restaurant_analytis && dbt run`, now matched by the path after the variable. 0.2.0 linked it too, through its only-project fallback. No link here needs a container mount, the only-project fallback for a container path, or an imported helper. What the fixes do is shown by their fixtures and, in-sample, by the first corpus: 17 to 55 correct of 106, with no false positives ([results/after-entry-fixes.json](results/after-entry-fixes.json)). That second figure was made after reading the corpus the fixes were aimed at, so it is not a held-out claim.
- **The fall from 84.0% to 57.1% is the sample, not the scanner.** The same 0.2.0 scanner scores both. In this sample 11 of 35 links run dbt in a container, against 50 of 106 in the first, and 6 are Cosmos, against 36: the Cosmos searches ran dry, and the first corpus's largest cause was one Cosmos repository.
- **It is small, and two repositories weigh heavily.** 35 links against the first corpus's 106. `harrydevforlife/building-lakehouse` has 11, 10 of them generated from `manifest.json`; `hainguyenle89/airflow-dags` has 6, all correct. Without the first, the scanner gets 14 of 24.
- **Read together, the two corpora say the scanner misses about half to four-fifths of links on repositories it has not seen, says so at the line nearly every time, and has not yet linked a task that does not run dbt.**

### Why it misses

Every miss, with its cause, is in [fresh/results/adjudication.json](fresh/results/adjudication.json), read after the first run without changing a label.

| Links | Cause | Reported as | Follow-up |
|---|---|---|---|
| 10 | One `BashOperator` per model, made in a loop over the nodes of a `manifest.json` read at parse time. The reason names `HOME` in the same command, not the loop | Unknown | LP-244 |
| 3 | A DockerOperator whose image's entrypoint is dbt, given only `run --target dev` (2, as a string) or a list (1) | **Nothing** (2); unknown (1) | Silence fixed; LP-245 |
| 2 | A repo-local operator subclass, `DbtDockerOperator` | Unknown | LP-2cd |
| 2 | The task is under `if os.getenv("DBT_ENABLED", "false").lower() == "true":`. The default decided the test, and the branch was skipped. Both labels `UNSURE:` | **Nothing** | Silence fixed; LP-246 |
| 1 | `cmds=["bash", "-cx"]`, not recognised as a shell | Unknown | LP-x29 |
| 1 | The pod's command is in its `pod_template_file`, outside the DAG files | Unknown | None: out of reach |
| 1 | `dbt test` where the only tests are YAML under `test-paths`, which the stand-in manifest does not read. Label `UNSURE:` | Wrong model set | LP-248 |

### How it was done

As for the first corpus, with these differences:

1. **Labelled blind** by an agent told to read only the first corpus's labels, its selection record and the scorer, never the scanner, the spike, the design or the first corpus's results. It took the same five searches' hits in returned order, aiming at three repositories per operator family and passing over the first corpus's repositories and its exclusions. The two Cosmos searches ran dry, 60 hits giving one new repository, so the mix is 4 `BashOperator`, 3 `KubernetesPodOperator`, 4 `DockerOperator` and 1 Cosmos. It rejected none. Its stopping points are in [fresh/repos.json](fresh/repos.json).
2. **The scorer came first.** `score.py --corpus` was committed (`9033508`) before any label existed, and the labels were committed unread (`dcc30a0`) before the scanner was frozen.
3. **Frozen, then read.**

   | | |
   |---|---|
   | Commit | `fd686c8` |
   | Scanner sha256 | `22444f3d5c9bdcec106c7307c0bb7ecbcd8dc5bac9173671eb39dbda8bacf777` |
   | Run | [fresh/results/first.json](fresh/results/first.json), the first and only run on that hash |

4. **A baseline afterwards.** The 0.2.0 scanner (sha256 `9eaf8e94…`, unchanged from `v0.2.0` to `dcc30a0`) was scored on the same labels once the first run was recorded. It tunes nothing; it shows what the fixes changed here.

**After the freeze**, as with the first corpus, the 4 silent links were fixed, because the definition of done forbids silence, by changes that only add unknowns (`08c6ee6`). The same 15 links are correct. On the first corpus that commit leaves the in-sample result unchanged.

Limits: one labeller; 3 labels `UNSURE:`; every link was labelled resolvable, so this says nothing about how the scanner handles links that are rightly unknown; manifests are stand-ins, as before.

## Reproduce

The repositories are data. They are fetched at a pinned commit into `corpus/airflow/work/`, which git ignores, with hooks off and without submodules or LFS, and read only as text and syntax. Nothing from them is imported, executed or installed, and no code from them is in this folder. The labels hold names, paths and short quotes. Review them before this repository goes public, along with LP-0tf.

```sh
export UV_PYTHON_PREFERENCE=only-managed
uv run --project packages/core python corpus/score.py fetch airflow         # the pinned commits
uv run --project packages/core python corpus/score.py hash airflow          # the reader's hash
uv run --project packages/core python corpus/score.py run airflow my-run    # writes results/my-run.json
uv run --project packages/core python corpus/score.py --corpus fresh fetch airflow        # the second corpus,
uv run --project packages/core python corpus/score.py --corpus fresh run airflow my-run   # in fresh/
```

`--work DIR` puts the repositories somewhere else. To reproduce a frozen run, check out its commit first: `5a50fc8` for the first corpus (in the private archive's history; the public history was rewritten when the source moved), `fd686c8` for the second (in the public history). The corpus was then at `corpus/`, and its commands took no tool. It needs third-party repositories, so it does not run in CI.

### The move to `corpus/airflow/`, and the merge

The scanner of 0.3.0 scores both corpora exactly as the Entry phase's runs recorded, link for link: [results/merged.json](results/merged.json) equals [results/after-entry-fixes.json](results/after-entry-fixes.json), and [fresh/results/merged.json](fresh/results/merged.json) equals [fresh/results/after-silence-fixed.json](fresh/results/after-silence-fixed.json). Those runs were made in the public repository, where the Entry phase was built, before its work was merged with Phase 7's on 18 November 2026.


Task 7.9 moved this corpus here, and the Airflow-only parts of the scorer into [tool.py](tool.py), so that one scorer serves every reader. The scanner scored the same before and after the move: [results/moved.json](results/moved.json) equals a run of the old scorer on the same clones, link for link. That is 17 correct and 87 misses, 82.1%, as after the freeze, and the reader's hash is the same, `d5786081bf4e9abbcc779e8b3d0a9831ea928346e7c11287d9b04e9dfbbabd40`. A run since task 7.9 records `reader_sha256` and `tool` where older runs have `scanner_sha256`.
