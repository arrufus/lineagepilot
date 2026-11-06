## Change impact review: `impact_shop`

Compared with the merge-base of `HEAD` and `origin/main` (`0123456789ab`). Manifest written `2026-11-05T08:05:23.750310Z` by dbt `1.12.6`.

**1 changed node · 2 changed columns · 9 impacted (4 models, 1 exposure, 4 Airflow tasks) · 4 unknowns**

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

#### Airflow tasks (4 tasks in 2 DAGs)

| DAG | Task | What it has to do with this | Confidence | Evidence | Owner |
|---|---|---|---|---|---|
| `daily_marts` | `marts`. Derived: `marts.customer_revenue_run`, `marts.orders.run`, `marts.orders.test`, `marts.revenue_tiers_run` | dbt, through Cosmos: `customer_revenue`, `orders`, `revenue_tiers` | inferred | DAG file `dags/daily_marts.py`: LoadMode.AUTOMATIC without manifest\_path: Cosmos runs dbt ls at parse time; resolved over the project's manifest instead | `data-platform` |
| `daily_marts` | `notify_finance` | runs after `marts` | inferred | DAG file `dags/daily_marts.py` | `data-platform` |
| `operations` | `export_orders` | reads: `orders` | heuristic | DAG file `dags/operations.py` | `operations-eng` |
| `operations` | `refresh_counts` | dbt run: `order_counts` | heuristic | DAG file `dags/operations.py`: no project\_dir: dbt uses its working directory; assumed the only dbt project in the workspace | `operations-eng` |

#### Owners

| Owner | Impacted |
|---|---|
| `Revenue Analytics` | `revenue_dashboard` |
| `data-platform` | `daily_marts.marts`, `daily_marts.notify_finance` |
| `growth-analytics` | `customer_revenue` |
| `operations` | `order_counts` |
| `operations-eng` | `operations.export_orders`, `operations.refresh_counts` |
| No owner | `broken_report`, `revenue_tiers` |

### Unknowns (4)

- `airflow:backfill`, `loop or factory`: dags/backfill.py:12: inside a for-loop (line 11): tasks created a dynamic number of times; shell command not statically resolvable: an f-string part is not resolvable (loop variable of the for-loop at line 11). Tasks are created a number of times that is only known when the file runs. Declare what …
- `macros/cents_to_dollars.sql`, `changed file`: a macro: it can change any model that calls it, and which ones is not known
- `model.impact_shop.broken_report`, `column lineage`: SQL could not be parsed: ParseError: Required keyword: 'this' missing for \<class 'sqlglot.expressions.core.Paren'\>. Line 4, Col: 8. les it, nothing can parse it select order\_id, order\_total from "impact\_shop"."main"."orders" where ((
- `models/marts/brand_new.sql`, `changed file`: not in the manifest yet; run dbt parse or dbt compile to include it

<details><summary>Other changed files (3)</summary>

- `README.md` (added)
- 2 files outside this dbt project

</details>
