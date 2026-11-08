# LineagePilot

A VS Code extension that shows, inside the editor, what a dbt change will break: downstream dbt models, exposures, and the Airflow tasks that run them.

- Design: [docs/design.md](docs/design.md)
- Plan: [docs/implementation_plan.md](docs/implementation_plan.md)
- Demo scripts: [docs/demo.md](docs/demo.md) (Phases 1–4) and [docs/demo_phase5.md](docs/demo_phase5.md) (Airflow linkage)
- Releases: [docs/release.md](docs/release.md). The closed beta and Gate G1: [docs/beta.md](docs/beta.md)
- Work items: `bd ready` (beads, prefix `LP`)

## Layout

```
spikes/           Phase 0 spikes (S1–S3): throwaway code that answers Gate G0
packages/core/    lineagepilot_core (Phase 1)
packages/lsp/     lineagepilot_lsp, the pygls language server (Phase 2)
extension/        the VS Code extension (Phase 2)
protocol/         the lineage/* JSON Schema, its generator and contract examples
fixtures/         dbt project workspaces for the core, server and extension tests
corpus/           the held-out DAG corpus: labels, scorer and results (Phase 5)
docs/             design, plan, rules reference, demo scripts, release process and beta guide
.github/          CI and release workflows, and the beta's issue forms
```

LineagePilot is released under the [MIT licence](LICENSE).

## Where this came from

LineagePilot started inside the data-capsule repository (arrufus/data-capsule) and moved here on 1 November 2026. The git history moved with it, filtered to LineagePilot's own files. data-capsule keeps:
- the frozen legacy server and dashboard, which Phase 1 copies parsers from (design section 12);
- the legacy wind-down issues (DCS-4sz) and the Snowflake key rotation (DCS-hov).

Issues here use the prefix `LP`. A `DCS-…` ID in these documents is a data-capsule issue.

## Status

Phase 0 is complete: S1–S3 have been measured and S4 was waived. Gate G0 (LP-1ay.5) passed on 2 November 2026, on the evidence in [spikes/G0_REPORT.md](spikes/G0_REPORT.md). Phase 1, the `lineagepilot_core` library ([packages/core](packages/core/README.md)), was completed on 2 November 2026: every exit criterion is met, with the evidence in the package README.

Phase 2, the language server ([packages/lsp](packages/lsp/README.md)) and the extension shell ([extension](extension/README.md)), was completed on 2 November 2026. Its exit criteria are met:
- the `@vscode/test-cli` integration tests pass on Linux, macOS and Windows with Python 3.10 and 3.13, in four configurations: trusted, Restricted Mode, Python from `PATH`, and Python from the Python extension (CI run 41468735984);
- the Restricted Mode tests prove that dbt cannot run and that workspace settings cannot change the interpreter, the dbt executable or the artifact size limit;
- extension coverage is 84.8% of lines (gate 75%).

Open items from Phase 2: the server stays 15% over its 500 MB budget after a cold index of 5,000 models (LP-dtt); the server's own unit tests hang on Windows CI, though the server is tested on Windows through the extension workflow (LP-7t7); Open VSX accepts only licensed extensions, so publishing waits for decision D-5; the jaffle-shop fixtures need replacing before the repository goes public (LP-0tf).

Phase 3, the lineage graph webview (LP-b7z), was completed on 4 November 2026: the graph in model and column modes, with depth, direction, filters, search, auto-layout and lazy expansion, its details, impact and settings panels, VS Code theming and keyboard navigation. Its exit criteria are met:
- React Testing Library coverage of the view is 99.3% of lines (gate 75%);
- a 500-node graph stays interactive: in Chrome, selection, search and the arrow keys answer within 86 ms, zooming holds 57–59 fps on a 60 Hz display, and the page reports no CSP violations ([design §11](docs/design.md#11-performance-budgets), LP-08j);
- clicking a node previews its file at the node's range, and double-click or **Open** opens it: tested in the view, and in VS Code from the graph the webview was given.

The browser measurement found two faults, both fixed: the page reported a CSP violation for zod's probe for eval, and on a graph over 200 nodes an arrow key towards a node off screen did nothing. Selection and search grow with the graph, to 159 and 127 ms at 1,000 nodes. Open from Phase 3: an `npm audit` advisory for minimatch through vscode-languageclient, whose fix cannot be adopted before 10 November 2026 under the two-week rule (LP-dy1).

Phase 4, the Change Impact Review (LP-zod), was built on 5 November 2026: **Review Changes** in the editor and `lineagepilot review` on the command line, which print the same Markdown for the same inputs ([design §8](docs/design.md#8-change-impact-review-algorithm)). Its exit criteria are met, in CI on Linux, macOS and Windows with Python 3.10 and 3.13 (run 42002861421):
- the dbt half of the acceptance scenario passes in a real VS Code with the real `vscode.git`, on real dbt 1.12 artifacts from before and after renaming a mart column (`fixtures/impact-shop`): the downstream models, the exposure and the owners are listed, an unparseable model is under Unknowns, and Restricted Mode keeps lineage, refuses dbt and says it cannot review without Git. The Airflow half is Phase 5;
- golden-file tests pin the Markdown, and it does not change with the order of the input or the hash seed. An end-to-end test runs the panel's request and the CLI on one repository, and they agree, Markdown and JSON;
- at 5,000 models a review takes 2–45 ms, and the server's peak stays within its budget, at 375 MB on Linux ([design §11](docs/design.md#11-performance-budgets)).

CI found one bug that local runs could not: on Windows, git checks files out with CRLF, and the stale check called them changed against a manifest built with LF. Line endings are now normalised before comparing. Follow-ups, none blocking: a changed macro is an unknown because which models call it is not worked out (LP-a04); everything below a changed model is listed at model level because comparing names cannot rule out a change in how a column is computed (LP-85e); and a properties file touches every node it documents (LP-07a).

Phase 5, Airflow linkage (LP-56q), was built on 6 November 2026: DAG files read as syntax only and never imported or run, and tasks linked to dbt nodes in CodeLens, the **Airflow** view, the lineage graph, **Review Changes** and `lineagepilot review` ([design §6.2](docs/design.md#62-airflow-mvp-static-analysis)). What could not be read is listed as an unknown, at the line that caused it, and `.lineagepilot/airflow.yml` can answer it.

**The miss rate on repositories the scanner had never seen is 84.0%** (LP-56q.5, [corpus/README.md](corpus/README.md)). On 106 links in 17 repositories labelled blind, the frozen scanner got 17 right, reported 78 as unknown at the call site, said nothing on 9 and was wrong on 2, with no false positives. Spike S3's 0% was in-sample and says little about other repositories. The scanner rarely misleads, but on repositories like these it often cannot tell, so the review's Airflow section is mostly a list of tasks it could not read. Three general gaps make up 62 of the 89 misses (LP-0o1, LP-z2x, LP-c05). The silent links are now unknowns, and the correct count did not move. Nothing else was tuned: this corpus has been read, so a tuned scanner needs a fresh one (LP-aax).

The exit criteria:
- the full acceptance scenario passes in a real VS Code with the real `vscode.git`, on real dbt 1.12 artifacts: the review lists the downstream models, the exposure, the Airflow task and its owner, an unparseable model is under Unknowns, and Restricted Mode keeps lineage and refuses dbt. Its Airflow assertions passed in CI on Linux, macOS and Windows on an earlier commit (run 42236319899, where the only failures were three new tests with bugs of their own, since fixed); the whole scenario passes on the final code in the Linux container, on Python 3.10 and 3.13;
- the miss rate on the held-out corpus is measured and reported, above.

**CI is not complete, and the epic stays open for it** (LP-kf5). GitHub Actions stopped starting jobs on 6 November, with "recent account payments have failed or your spending limit needs to be increased". At the frozen commit `5a50fc8` the core passed on Linux, macOS and Windows with Python 3.10 and 3.13, the selector conformance job passed against real dbt 1.9 and 1.12 (run 42238432705), the server passed on Linux and macOS (run 42238432674), and the extension built and met its coverage gate (run 42238432678), but its test matrix never started. The commits after it have had no CI. Locally on the final code: the core and server suites pass on macOS with Python 3.10 and 3.13, and the extension's suite on Linux with both. Extension coverage is 85.3% of lines (gate 75%), the review page's view tests 99.5%, and the core 93.5% (gate 80%).

At 5,000 models with 2,000 tasks linked, a warm start has its scan linked in 2.1 s on macOS and 2.8 s on Linux, and the server's peak on Linux is 379 MB, within its 500 MB budget. On macOS it reads 511 MB warm and 608 MB cold, over budget (LP-dtt). A changed DAG file is listed, not followed (LP-o59).


Phase 6, beta and release (LP-7er), was built on 8 November 2026, after four decisions (design section 15):
- the closed beta gets the VSIX from GitHub releases of this private repository, not from a marketplace;
- the licence is MIT (D-5);
- there is no telemetry (D-7). Instead, opt-in usage counts stay on the user's machine, and the user copies them into a weekly check-in.

What was built:
- **6.1**: an extension README for beta users, with installing, getting started, usage counts and troubleshooting. A **Get Started with LineagePilot** walkthrough of seven steps. A sample project, built into the VSIX from `fixtures/impact-shop`: **Open Sample Project** copies it, and **Apply the Sample Change** makes its prepared column rename, so the review can be tried with neither dbt nor Airflow installed.
- **6.2**: the release workflow ([docs/release.md](docs/release.md)). A tag on `main` runs the three test workflows on the tagged commit and releases the VSIX they tested, with its SHA-256. It publishes to the Marketplace and Open VSX only when that is turned on and a reviewer approves. The VSIX carries `LICENSE` and `THIRD_PARTY_NOTICES.md`, and an npm or Python licence outside the allowed list fails the build.
- **6.3**: the beta guide and how Gate G1 is scored ([docs/beta.md](docs/beta.md)), issue forms for bugs, feedback and the weekly check-in, and the usage counts.

Tested locally, in the Linux container, with Python 3.10 and 3.13:
- the extension's unit, view and integration tests pass in every configuration, trusted and in Restricted Mode;
- extension coverage is 85.9% of lines (gate 75%);
- the build-script tests pass, including one that follows the sample's walkthrough through `lineagepilot review`.

**None of Phase 6 has run in CI** (LP-kf5): GitHub Actions still starts no jobs. So the release workflow has never run, and the extension has not been tested on macOS or Windows at this code.

What is left is the owner's: the checklist in [docs/beta.md](docs/beta.md#before-the-first-invitation), then recruiting the teams and the four weeks of beta before Gate G1 (LP-7er.4). Setting up the registries for the public release is LP-2xl.
