# Phase 0 spikes

Throwaway code that answers Gate G0. The verdict is in [G0_REPORT.md](G0_REPORT.md).

| Spike | Question | Result | Beads |
|---|---|---|---|
| [S1 packaging](s1-packaging/README.md) | Does one universal VSIX run a bundled pure-Python pygls server on Windows, macOS and Linux? | Met: 54/54 tests in CI on 3 operating systems × Python 3.10 and 3.13 | LP-1ay.1, closed |
| [S2 parse and performance](s2-parse-perf/README.md) | Does the existing dbt parser meet the section 11 budgets without the database layer? | Measured. As-is it misses two budgets at 5,000 models; with the fixes it meets them all. Schema v12 is pinned. | LP-1ay.2, closed |
| [S3 static DAG analysis](s3-static-dag/README.md) | Does AST-only scanning link Airflow tasks to dbt models correctly? | 84/84 links found and 75/75 correct, in-sample; a held-out check also had 0 misses | LP-1ay.3, closed |
| S4 customer discovery | Would teams change how they review pull requests? | Waived by the product owner | LP-1ay.4, closed |

Each spike has its own `pyproject.toml` and `uv.lock`, and its tests run offline with `uv run pytest`. Large working data lives under each spike's `work/`, which is gitignored and recreated by the spike's scripts.
