# LineagePilot

A VS Code extension that shows, inside the editor, what a dbt change will break: downstream dbt models, exposures, and the Airflow tasks that run them.

- Design: [../docs/design_docs/vscode_extension_design.md](../docs/design_docs/vscode_extension_design.md)
- Plan: [../docs/design_docs/vscode_extension_implementation_plan.md](../docs/design_docs/vscode_extension_implementation_plan.md)
- Work items: `bd ready --label lineagepilot`

## Layout

```
spikes/           Phase 0 spikes (S1–S3). Throwaway code that answers Gate G0.
packages/core/    lineagepilot_core (Phase 1)
packages/lsp/     lineagepilot_lsp, the pygls server (Phase 2)
extension/        the VS Code extension (Phase 2)
```

`packages/` and `extension/` do not exist yet. Phase 1 starts after Gate G0.

## Status

Phase 0 is in progress. The spike reports are in [spikes/](spikes/).
