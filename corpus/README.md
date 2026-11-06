# Held-out DAG corpus (task 5.5, the Phase 5 exit)

How well the Airflow scanner does on repositories it had never seen. Spike S3's 0% miss rate was in-sample: the scanner was built while reading those repositories, and its own held-out check had only 9 links. This measures it properly.

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

**This is the only out-of-sample figure.** The corpus has now been read, so anything tuned against it is in-sample. A tuned scanner needs a fresh corpus (LP-aax) before it can claim a better one.

| Scanner | Correct | Miss rate | Sample |
|---|---|---|---|
| S3, in-sample | 75 of 75 resolvable, 84 links | 0% | Built while reading them |
| S3, its small hold-out | 5 of 5 resolvable, 9 links | 0% | 4 repositories, chosen by a search for `"dbt run" BashOperator` |
| **Phase 5, frozen** | **17 of 106** | **84.0%** | **17 repositories, labelled blind** |
| Phase 5, default DAG folder (`**/dags`) | 15 of 106 | 85.8% | The same, with no setting touched |

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

4. **Scored by S3's definition.** A link is correct when the DAG, the task or group, the dbt project and the model set all match. The scorer, [score.py](score.py), was written before the labels were read.
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

## Reproduce

The repositories are data. They are fetched at a pinned commit into `work/`, which git ignores, with hooks off and without submodules or LFS, and read only as text and syntax. Nothing from them is imported, executed or installed, and no code from them is in this folder. The labels hold names, paths and short quotes. Review them before this repository goes public, along with LP-0tf.

```sh
export UV_PYTHON_PREFERENCE=only-managed
uv run --project packages/core python corpus/score.py fetch        # the pinned commits
uv run --project packages/core python corpus/score.py hash         # compare with the table above
uv run --project packages/core python corpus/score.py run my-run   # writes results/my-run.json
```

To reproduce the frozen run, check out `5a50fc8` first. It needs third-party repositories, so it does not run in CI.
