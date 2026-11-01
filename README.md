# LineagePilot

A VS Code extension that shows, inside the editor, what a dbt change will break: downstream dbt models, exposures, and the Airflow tasks that run them.

- Design: [docs/design.md](docs/design.md)
- Plan: [docs/implementation_plan.md](docs/implementation_plan.md)
- Work items: `bd ready` (beads, prefix `LP`)

## Layout

```
spikes/           Phase 0 spikes (S1–S3): throwaway code that answers Gate G0
packages/core/    lineagepilot_core (Phase 1)
packages/lsp/     lineagepilot_lsp, the pygls server (Phase 2)
extension/        the VS Code extension (Phase 2)
docs/             design and implementation plan
```

`packages/` and `extension/` do not exist yet. Phase 1 starts after Gate G0.

## Where this came from

LineagePilot started inside the data-capsule repository (arrufus/data-capsule) and moved here on 1 November 2026. The git history moved with it, filtered to LineagePilot's own files. data-capsule keeps:
- the frozen legacy server and dashboard, which Phase 1 copies parsers from (design section 12);
- the legacy wind-down issues (DCS-4sz) and the Snowflake key rotation (DCS-hov).

Issues here use the prefix `LP`. A `DCS-…` ID in these documents is a data-capsule issue.

## Status

Phase 0 is complete: S1–S3 have been measured and S4 was waived. [spikes/G0_REPORT.md](spikes/G0_REPORT.md) recommends proceeding and sets out the evidence. Phase 1 starts once Gate G0 (LP-1ay.5) is closed.
