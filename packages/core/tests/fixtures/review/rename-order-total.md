## Change impact review: `impact_shop`

Compared with the merge-base of `HEAD` and `origin/main` (`0123456789ab`). Manifest written `2026-11-17T19:15:15.206996Z` by dbt `1.12.6`.

**1 changed node · 2 changed columns · 21 impacted (4 models, 1 exposure, 1 semantic model, 4 metrics, 1 saved query, 8 tasks (Airflow 4, Dagster 4), 2 Dagster pipelines) · 5 unknowns**

> catalog.json lists what the warehouse held when dbt docs generate last ran. If a changed model was built before that, a rename no longer shows as a change: set a baseline manifest to compare with what is deployed.

### Changed

- `orders` (model, modified): `models/marts/orders.sql`, `models/marts/orders.yml`; owner `finance-data`
  - Removed: `order_total`
  - Added: `total_usd`
  - Compared with `catalog.json`.

### Downstream impact

#### Models (4)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `customer_revenue` | Column `lifetime_value` | inferred | compiled SQL, from `orders.order_total` | `growth-analytics` |
| `revenue_tiers` | Column `tier` | inferred | compiled SQL, from `customer_revenue.lifetime_value` | No owner |
| `broken_report` | Model level: column lineage unknown: SQL could not be parsed: ParseError: Required keyword: 'this' missing for \<class 'sqlglot.expre… | inferred | manifest, from `orders.order_total` | No owner |
| `order_counts` | Model level: depends on a changed model, but on none of the columns known to have changed | inferred | manifest, from `orders` | `operations` |

#### Exposures (1)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `revenue_dashboard` | Model level: uses the whole node | inferred | manifest, from `customer_revenue.lifetime_value` | `Revenue Analytics` |

#### Semantic layer (6)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `orders` (semantic model) | Fields `order_size`, `order_total` | declared | manifest, from `orders.order_total` | `finance-data` |
| `large_orders` (metric) | Field `order_size` | inferred | manifest, from `orders.order_size` | No owner |
| `revenue` (metric) | Field `order_total` | declared | manifest, from `orders.order_total` | `finance-analytics` |
| `order_count` (metric) | Model level: depends on a changed model, but on none of the columns known to have changed | inferred | manifest, from `orders` | No owner |
| `weekly_revenue` (saved query; exports `exports.weekly_revenue`) | Model level: uses the whole node | inferred | manifest, from `average_order_value`, `revenue` | `operations` |
| `average_order_value` (metric) | Model level: uses the whole node | inferred | manifest, from `revenue` | No owner |

### Pipelines

#### Airflow (4 tasks in 2 DAGs)

| DAG | Task | What it has to do with this | Confidence | Evidence | Owner |
|---|---|---|---|---|---|
| `daily_marts` | `marts`. Derived: `marts.customer_revenue_run`, `marts.orders.run`, `marts.orders.test`, `marts.revenue_tiers_run` | dbt, through Cosmos: `customer_revenue`, `orders`, `revenue_tiers` | inferred | DAG file `dags/daily_marts.py`: LoadMode.AUTOMATIC without manifest\_path: Cosmos runs dbt ls at parse time; resolved over the project's manifest instead | `data-platform` |
| `daily_marts` | `notify_finance` | runs after `marts` | inferred | DAG file `dags/daily_marts.py` | `data-platform` |
| `operations` | `export_orders` | reads: `orders` | heuristic | DAG file `dags/operations.py` | `operations-eng` |
| `operations` | `refresh_counts` | dbt run: `order_counts` | heuristic | DAG file `dags/operations.py`: no project\_dir: dbt uses its working directory; assumed the only dbt project in the workspace | `operations-eng` |

#### Dagster (2 dbt asset definitions, 1 job, 1 schedule and 2 assets in 2 code locations)

| Code location | Definition | What it has to do with this | Confidence | Evidence | Owner |
|---|---|---|---|---|---|
| `operations` | dbt asset definition `operations_dbt` | dbt, through Dagster: `order_counts` | inferred | Dagster definitions `dagster/operations/definitions.py` | No owner |
| `operations` | job `hourly_counts` | dbt, through Dagster: `order_counts` | inferred | Dagster definitions `dagster/operations/definitions.py` | No owner |
| `operations` | schedule `hourly_counts_schedule` | dbt, through Dagster: `order_counts`; job `hourly_counts`, `0 * * * *` | inferred | Dagster definitions `dagster/operations/definitions.py` | No owner |
| `operations` | asset `order_counts_export` | reads, as a Dagster asset: `order_counts` | inferred | Dagster definitions `dagster/operations/definitions.py` | No owner |
| `operations` | asset `revenue_forecast` | reads, as a Dagster asset: `customer_revenue` | inferred | Dagster definitions `dagster/operations/definitions.py` | No owner |
| `shop-analytics` | dbt asset definition `marts` | dbt, through Dagster: `customer_revenue`, `orders`, `revenue_tiers` | inferred | Dagster definitions `dagster/analytics/src/shop_analytics/defs/marts/defs.yaml` | No owner |

### Owners

| Owner | Impacted |
|---|---|
| `Revenue Analytics` | `revenue_dashboard` |
| `data-platform` | `daily_marts.marts`, `daily_marts.notify_finance` |
| `finance-analytics` | `revenue` |
| `finance-data` | `orders` |
| `growth-analytics` | `customer_revenue` |
| `operations` | `order_counts`, `weekly_revenue` |
| `operations-eng` | `operations.export_orders`, `operations.refresh_counts` |
| No owner | `operations.operations_dbt`, `operations.order_counts_export`, `operations.revenue_forecast`, `operations/job/hourly_counts`, `operations/schedule/hourly_counts_schedule`, `shop-analytics.marts`, `average_order_value`, `large_orders`, `order_count`, `broken_report` and 1 more |

### Unknowns (5)

#### dbt (3)

- `macros/cents_to_dollars.sql`, `changed file`: a macro: it can change any model that calls it, and which ones is not known
- `model.impact_shop.broken_report`, `column lineage`: SQL could not be parsed: ParseError: Required keyword: 'this' missing for \<class 'sqlglot.expressions.core.Paren'\>. Line 4, Col: 8. les it, nothing can parse it select order\_id, order\_total from "impact\_shop"."main"."orders" where ((
- `models/marts/brand_new.sql`, `changed file`: not in the manifest yet; run dbt parse or dbt compile to include it

#### Airflow (1)

- `airflow:backfill`, `loop or factory`: dags/backfill.py:12: inside a for-loop (line 11): tasks created a dynamic number of times; shell command not statically resolvable: an f-string part is not resolvable (loop variable of the for-loop at line 11). Tasks or assets are made a number of times that is only known when the file runs. Declar…

#### Dagster (1)

- `dagster:operations.snapshots`, `dbt from Python`: dagster/operations/definitions.py:42: snapshots() runs dbt from Python (subprocess.run); what it runs is not read. dbt is run from Python code inside a task or an asset, which is not analysed. Declare what it runs in .lineagepilot/lineage.yml.

<details><summary>Other changed files (3)</summary>

- `README.md` (added)
- 2 files outside this dbt project

</details>
