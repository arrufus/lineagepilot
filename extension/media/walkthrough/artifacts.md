# Give it your dbt artifacts

LineagePilot reads the files dbt already writes to `target/`. It needs no warehouse connection and no account.

| You run | LineagePilot gets |
|---|---|
| `dbt parse` | model-level lineage, exposures, tests and owners, from `manifest.json` |
| `dbt compile` | compiled SQL, so lineage column by column, and the Change Impact Review |
| `dbt docs generate` | `catalog.json`: every column the warehouse holds, for column lineage and as the review's baseline |

Manifest schema v12 is supported: dbt 1.8 to 1.12, and Fusion.

In a trusted workspace, **Refresh dbt Artifacts** runs one of them for you, with the `dbt` on your `PATH` or the one in `lineagePilot.dbt.executablePath`. LineagePilot re-indexes when `target/` changes, and the status bar warns when your model files are newer than the manifest.
