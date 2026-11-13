# Changelog

The versions of LineagePilot, newest first. Each release's entry is its release notes.

## 0.2.0

The first public release, on the VS Code Marketplace and Open VSX. It is a **Preview**: what it shows is accurate as far as it goes, and it says plainly where it could not tell.

LineagePilot shows what a dbt change reaches, from the artifacts already in your workspace. It needs no server, no account and no warehouse connection, and makes no network connections.

- **Lineage as you edit**: CodeLens, hovers, the Problems panel, the Lineage view and the lineage graph, at model and column level, from `manifest.json`, compiled SQL and `catalog.json`.
- **Change Impact Review**: what your branch and working tree change, what that reaches across models, exposures, Airflow tasks and owners, and what could not be checked. **Copy as Markdown** puts it in your pull request; `lineagepilot review` prints the same text.
- **Airflow linkage**: tasks linked to the models they run, from DAG files read as syntax and never run, with `.lineagepilot/airflow.yml` for what cannot be read.
- **Get Started walkthrough** and a **sample project** that needs neither dbt nor Airflow.
- **Restricted Mode**: only reads artifacts, never runs dbt or an interpreter from the workspace.

**What to expect from the Airflow linkage.** On 106 task links in 17 repositories it had never seen, it linked 17 correctly and listed most of the rest as unknown, at the line that caused each one. Two were wrong, and it made no link where there was none. Expect the review's Airflow section to list tasks it could not read, and use `.lineagepilot/airflow.yml` to answer them.

**Removed**: usage counts and **Copy Usage Counts**, which existed only for the closed beta's weekly check-in. Anything they counted on your machine is deleted when 0.2.0 starts. A `lineagePilot.usageCounts.enabled` line left in your user settings does nothing, and you can delete it.

## 0.1.1

The first beta, for teams invited to the closed beta.

- **Lineage from your dbt artifacts**, in a local language server: CodeLens, hovers, the Problems panel, the Lineage view and the lineage graph, at model and column level, from `manifest.json`, compiled SQL and `catalog.json`.
- **Change Impact Review**: what your branch and working tree change, what that reaches across models, exposures, Airflow tasks and owners, and what could not be checked. **Copy as Markdown** for the pull request; `lineagepilot review` prints the same text.
- **Airflow linkage**: tasks linked to the models they run, from DAG files read as syntax and never run, with `.lineagepilot/airflow.yml` for what cannot be read.
- **Get Started walkthrough** and a **sample project** that needs neither dbt nor Airflow.
- **Usage counts** for the beta's weekly check-in: off unless you turn them on, kept on your machine, never sent.
- Restricted Mode: only reads artifacts, never runs dbt or an interpreter from the workspace.
