## Change impact review: `impact_shop`

Compared with the merge-base of `HEAD` and `origin/main` (`0123456789ab`). Manifest written `2026-11-05T08:05:23.750310Z` by dbt `1.12.6`.

**1 changed node · 2 changed columns · 5 impacted (4 models, 1 exposure) · 3 unknowns**

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

#### Airflow tasks

Airflow DAGs were not analysed in this version.

#### Owners

| Owner | Impacted |
|---|---|
| `Revenue Analytics` | `revenue_dashboard` |
| `growth-analytics` | `customer_revenue` |
| `operations` | `order_counts` |
| No owner | `broken_report`, `revenue_tiers` |

### Unknowns (3)

- `macros/cents_to_dollars.sql`, `changed file`: a macro: it can change any model that calls it, and which ones is not known
- `model.impact_shop.broken_report`, `column lineage`: SQL could not be parsed: ParseError: Required keyword: 'this' missing for \<class 'sqlglot.expressions.core.Paren'\>. Line 4, Col: 8. les it, nothing can parse it select order\_id, order\_total from "impact\_shop"."main"."orders" where ((
- `models/marts/brand_new.sql`, `changed file`: not in the manifest yet; run dbt parse or dbt compile to include it

<details><summary>Other changed files (3)</summary>

- `README.md` (added)
- 2 files outside this dbt project

</details>
