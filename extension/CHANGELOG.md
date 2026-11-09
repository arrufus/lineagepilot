# Changelog

The versions of LineagePilot, newest first. Each release's entry is its release notes.

## 0.1.1

The first beta, for teams invited to the closed beta.

- **Lineage from your dbt artifacts**, in a local language server: CodeLens, hovers, the Problems panel, the Lineage view and the lineage graph, at model and column level, from `manifest.json`, compiled SQL and `catalog.json`.
- **Change Impact Review**: what your branch and working tree change, what that reaches across models, exposures, Airflow tasks and owners, and what could not be checked. **Copy as Markdown** for the pull request; `lineagepilot review` prints the same text.
- **Airflow linkage**: tasks linked to the models they run, from DAG files read as syntax and never run, with `.lineagepilot/airflow.yml` for what cannot be read.
- **Get Started walkthrough** and a **sample project** that needs neither dbt nor Airflow.
- **Usage counts** for the beta's weekly check-in: off unless you turn them on, kept on your machine, never sent.
- Restricted Mode: only reads artifacts, never runs dbt or an interpreter from the workspace.
