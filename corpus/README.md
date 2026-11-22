# Held-out corpora

Each reader is measured on repositories it was not built from. The repositories are labelled blind, before the reader is frozen, and the reader is scored once (design_next_phase §6, "How every reader phase runs" in the plan). One scorer, [score.py](score.py), serves every reader. Each tool has a folder:

| Tool | Corpus | Result |
|---|---|---|
| Airflow | [airflow/](airflow/README.md), 17 repositories, 106 links (Phase 5) | 84.0% missed on the frozen run, with no false positives |
| Airflow, the second corpus | [airflow/fresh/](airflow/README.md#a-second-corpus-after-the-entry-fixes-lp-aax), 12 repositories, 35 links (the Entry phase, LP-aax) | 57.1% missed, with no false positives |
| Dagster | [dagster/](dagster/README.md), 12 repositories, 110 links (Phase 8) | 85.5% missed with the default setting, 92.7% in the labels' folders, with no false positives; 69 of the 110 links silent. After the silent links were fixed, in-sample: 83.6% missed, 6 silent |

## A tool's folder

`corpus/<tool>/` holds:

- `README.md`: the result, how the repositories were chosen and labelled, and the limits.
- `repos.json`: the searches, the stopping points and the candidates turned down.
- `labels/`: one file per repository, with its URL, the commit it is pinned to, and the links a careful person expects.
- `results/`: each run, with the reader's hash, `reader_sha256`, and the commit it ran on.
- `tool.py`: what is particular to the tool:
  - `HASH_FILES`, the files under `lineagepilot_core` that decide the reader's answer, as globs;
  - `SCANS`, how to scan a repository: first the scan that measures the reader, then any others;
  - `GROUP`, the label field the results are broken down by;
  - `score(label, scan)`, each labelled link's outcome, and what was found that no label expects.

`score.py` holds what every reader shares:
- Fetching the repositories.
- The reader's hash.
- The outcomes:
  - `correct`;
  - `miss`, with its reason;
  - `unknown_legit`, an unknown where only unknown is right;
  - false positives.
- The miss rate, also without the labels marked `UNSURE:`.
- Pooling the repositories into one result.

A phase that adds a reader adds its folder. It labels the corpus during the phase before, and freezes the reader, recording its hash, before scoring it.

## Running it

The repositories are data. They are fetched at a pinned commit, with hooks off and without submodules or LFS, into `corpus/<tool>/work/`, which git ignores. They are read only as text and syntax: nothing from them is imported, executed or installed. They need the network, so the corpora are scored by hand, not in CI. CI runs the scorer's own tests, on a fixture (`packages/core/tests/test_corpus_score.py`).

```sh
export UV_PYTHON_PREFERENCE=only-managed
uv run --project packages/core python corpus/score.py fetch airflow
uv run --project packages/core python corpus/score.py hash airflow          # compare with a recorded run
uv run --project packages/core python corpus/score.py run airflow my-run    # writes airflow/results/my-run.json
```

`--work DIR`, before the command, reads and fetches the repositories somewhere else. `--corpus NAME` scores another of the tool's corpora, in `corpus/<tool>/NAME/`, with its own `labels/`, `work/` and `results/`: `--corpus fresh` for the Airflow reader's second.
