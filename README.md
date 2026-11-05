# LineagePilot

A VS Code extension that shows, inside the editor, what a dbt change will break: downstream dbt models, exposures, and the Airflow tasks that run them.

- Design: [docs/design.md](docs/design.md)
- Plan: [docs/implementation_plan.md](docs/implementation_plan.md)
- Demo script: [docs/demo.md](docs/demo.md)
- Work items: `bd ready` (beads, prefix `LP`)

## Layout

```
spikes/           Phase 0 spikes (S1–S3): throwaway code that answers Gate G0
packages/core/    lineagepilot_core (Phase 1)
packages/lsp/     lineagepilot_lsp, the pygls language server (Phase 2)
extension/        the VS Code extension (Phase 2)
protocol/         the lineage/* JSON Schema, its generator and contract examples
fixtures/         dbt project workspaces for the core, server and extension tests
docs/             design, implementation plan and the rules reference
```

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
