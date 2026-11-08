# The LineagePilot sample project

A small dbt project written for LineagePilot, a VS Code extension that shows what a dbt change reaches: downstream models, exposures, and the Airflow tasks that run them. Everything here works with neither dbt nor Airflow installed: the dbt artifacts in `target/` were built with dbt 1.12, and the DAG files are only ever read, never run.

## What is in it

| Path | What it is |
|---|---|
| `models/staging/` | A source of raw orders and customers, and the staging models over it |
| `models/marts/orders.sql` | The mart, owned by `finance-data`, which the prepared change edits |
| `models/marts/` | Models built on the mart, an exposure (`revenue_dashboard`), and `broken_report`, whose SQL cannot be parsed on purpose |
| `dags/daily_marts.py` | A Cosmos `DbtTaskGroup` over `orders+`, then a task that runs after it |
| `dags/operations.py` | `dbt run` in a `BashOperator`, and a SQL task that reads the mart |
| `dags/backfill.py` | Tasks built in a loop, so what they run cannot be known without running the file |
| `target/` | `manifest.json` and `catalog.json` from dbt 1.12 |
| `changes/rename-order-total/` | The prepared change: `order_total` renamed to `total_usd` in the mart, and the manifest `dbt compile` writes after it |

## Try it

1. **Trust the folder** when VS Code asks. It holds only this sample, and the review needs Git, which VS Code turns off in Restricted Mode.
2. Open `models/marts/orders.sql`. The CodeLens above it counts what is downstream. Hover over `ref('stg_orders')`, and over a column. Click the CodeLens, or run **LineagePilot: Show Lineage Graph**.
3. Open the **Airflow** view in the LineagePilot activity bar: the tasks that run the mart, how sure each link is and why, and the DAG it could not read.
4. Commit the sample: in **Source Control**, choose **Initialize Repository**, then commit everything. `.gitignore` leaves out `target/`, `changes/` and what dbt writes.
5. Run **LineagePilot: Apply the Sample Change**. It copies the files in `changes/rename-order-total/` over the project's: the edited model and its properties, then the manifest, as an edit followed by `dbt compile` would leave them.
6. Run **LineagePilot: Review Changes**. The rename reaches four models, the dashboard and four Airflow tasks, each with its owner, how sure the link is and what says so. `broken_report` and the loop in `dags/backfill.py` are under **Unknowns**, rather than left out. **Copy as Markdown** gives the text you would paste into a pull request.

To start again, delete this folder and run **LineagePilot: Open Sample Project** again.

## With dbt installed

The project builds with `dbt-duckdb`. It needs a `profiles.yml` of your own, beside `dbt_project.yml` or in `~/.dbt/`, for example:

```yaml
impact_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: impact_shop.duckdb
```

Then `dbt seed && dbt run --exclude broken_report && dbt docs generate` rebuilds `target/`. Instead of step 5, edit `orders.sql` yourself and run `dbt compile` (or **Refresh dbt Artifacts**).
