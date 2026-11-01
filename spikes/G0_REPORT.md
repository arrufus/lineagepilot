# Gate G0: evidence and recommendation

**Date**: 1 November 2026
**Decision owner**: the product owner
**Beads**: LP-1ay.5. Closing it unblocks Phase 1.

## Recommendation: proceed to Phase 1

Every G0 criterion is met. No spike found a reason to stop. Two found work that the plan had underestimated, and the plan now includes it.

| G0 criterion | Status | Evidence |
|---|---|---|
| 1. S1 and S2 meet their criteria, or show a credible path | **Met** | S1 met all its criteria on 3 operating systems. S2 measured every budget: as-is it misses two at 5,000 models, but a one-line fix plus a child-process indexer meets them all, and that was measured too. |
| 2. Market need | **Met by product-owner judgment** | S4 was waived on 1 November 2026. Market validation now rests on the Phase 6 beta and Gate G1. |
| 3. S1 and S2 confirm D-2 (Python language server) | **Confirmed** | The bundled pure-Python server runs everywhere, and the existing parsers are reusable once fixed. |
| S3: static DAG analysis is credible (added to the G0 issue) | **Met, in-sample** | 84/84 links found, 75/75 correct, 0 misses, with a small held-out check also at 0 misses. A literals-only scanner would miss 22.6%. |

## S1: packaging ([report](s1-packaging/README.md))

One universal VSIX (1.36 MB, 0 compiled files) starts a bundled pygls server and returns a CodeLens. It ran on Windows, macOS and Linux with Python 3.10 and 3.13. GitHub Actions run 41317287230 passed 54/54 tests, and local macOS and Linux arm64 runs passed as well. The interpreter was found through the setting, the Python extension and `PATH`. Startup to the first CodeLens takes about 0.45 s cold and 0.23 s warm.

Changes now in the design:
- The bundle command needs `--only-binary=:all:`, and PyYAML must be built pure from its hashed sdist.
- Every `lineage/*` request must carry a params object, because pygls 2.1 hangs without one.
- Restricted mode never runs a workspace interpreter, not even to probe it.

## S2: parse and performance ([report](s2-parse-perf/README.md))

The existing `DbtParser` runs without the database layer, on dbt Core 1.8 to 1.12 and Fusion 2.0.9. Every one of them writes manifest schema v12, which is now pinned. Times below are on an M4 Max at 5,000 models, worst of Python 3.10 and 3.13:

| Budget | As-is | After the S2 fixes |
|---|---|---|
| Cold index ≤ 5 s (the budget is set at 2,000 models, where as-is takes 3.07 s) | 16.5 s | 2.16 s |
| Server memory ≤ 500 MB | about 1 GB in one process | 106 MB. The child indexer peaks at 1.06 GB, then exits. |
| Warm start, CodeLens, impact | met | met |
| Column lineage for one model | met on time, but the output is unusable | ≤ 0.35 s with `sqlglot.lineage`, resolving 91–99% of columns |

The parser's `has_tests` is False on every real manifest, which breaks the ported test rules. Its column-lineage parser returns `*` for most models. Both are fixed by Phase 1 task 1.2 (LP-58r.2) and the new task 1.9 (LP-58r.8).

## S3: static DAG analysis ([report](s3-static-dag/README.md))

The scanner reads DAG files with the AST only, never importing them. It found every expected link in astronomer-cosmos, cosmos-demo and airflow-dbt-demo. Nine links were legitimate unknowns, such as remote manifests and DAGs generated at parse time.

This result is an upper bound: the scanner was tuned on the same repositories. A small held-out set of 4 repositories, labelled blind, also had 0 misses. Two of its five correct links depended on a fallback, though. 71 of the 75 correct links are `heuristic`, so the review must show heuristic links by default.

The 22.6% gap between a literals-only scanner and this one comes from three features, all now in design section 6.2:
- container-path remapping;
- constants imported from other modules;
- helper inlining.

## What changed in the plan

| Area | Change | Effect on the estimate |
|---|---|---|
| Phase 1 | Task 1.2 now fixes the parser (lineage loop, test derivation, exposures, file paths, identity, version gate). New task 1.9 rebuilds column lineage on `sqlglot.lineage`. | 3 weeks → about 4 weeks |
| Phase 2 | Indexing runs in a child process, and the dbt refresh is asynchronous with progress (LP-243.1, LP-243.5). | No change; this is design, not new scope |
| Phase 5 | Tasks 5.1–5.4 carry the S3 resolution rules. New task 5.5 builds a held-out corpus of 10+ repos, labelled before tuning; it can start right after G0 (LP-56q.5). | Unchanged, because the S3 prototype already covers much of 5.1. Add about a week of labelling, which can run in parallel. |
| Phase 6 | Beta teams are recruited by the product owner, since there are no interviewees (LP-7er.3). | No change |

The total is about 16–19 weeks for one engineer, up from 15–18.

## Risks carried forward

1. **Market risk is untested until the beta.** S4 was waived, so Gate G1 is the first external check. Recruit beta teams early, during Phase 3.
2. **DAG linkage accuracy is unproven out of sample.** Task 5.5 measures it before release.
3. **Supply chain.**
   - The npm production dependencies carry two moderate `minimatch` advisories, via vscode-languageclient 10.1.2.
   - vitest is held back by npm and vitest advisories, which affects the Phase 3 webview tests.
   - Re-check both in Phase 2.
4. **The leaked Snowflake key (DCS-hov) is still open.** It does not depend on any phase.

## Decision requested

Close LP-1ay.5 to proceed to Phase 1. Alternatively, record a stop or a re-scope there.
