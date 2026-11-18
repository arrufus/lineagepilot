## Change impact review: `shop`

Compared with the merge-base of `HEAD` and `main` (`abc`).

**1 changed node · 1 changed column · 7 impacted (1 model, 1 exposure, 1 metric, 2 tasks (Airflow 1, Dagster 1), 2 consumers (Looker 1, Tableau 1)) · 4 unknowns**

### Changed

- `a` (model, modified): `a.sql`; owner `o`
  - Removed: `x`
  - Compared with `catalog.json`.

### Downstream impact

#### Models (1)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `b` | Column `y` | inferred | compiled SQL, from `a.x` | `o` |

#### Exposures (1)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `dash` | Model level | declared | annotation | `bi` |

#### Semantic layer (1)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `revenue` (metric) | Model level | declared | annotation | No owner |

### Pipelines

#### Airflow (1 task in 1 DAG)

| DAG | Task | What it has to do with this | Confidence | Evidence | Owner |
|---|---|---|---|---|---|
| `daily` | `run_b` | dbt run: `b` | heuristic | annotation | `etl` |

#### Dagster (1 task in 1 pipeline)

| Pipeline | Task | What it has to do with this | Confidence | Evidence | Owner |
|---|---|---|---|---|---|
| `nightly` | `load_b` | dbt run: `b` | heuristic | annotation | No owner |

### Consumers

#### Looker (1)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `b` (view) | Fields `total`, `id` | inferred | annotation | `bi` |

#### Power BI

Not analysed: no .SemanticModel folder was found.

#### Tableau (1)

| Name | Reached | Confidence | Evidence | Owner |
|---|---|---|---|---|
| `gl` | Model level | declared | annotation | No owner |

### Owners

| Owner | Impacted |
|---|---|
| `o` | `b` |
| `bi` | `dash`, `view.b` |
| `etl` | `daily.run_b` |

### Unknowns (4)

#### dbt (2)

- `model.s.b`, `r`: why
- `macros/m.sql`, `changed file`: a macro

#### Airflow (1)

- `airflow:backfill`, `loop or factory`: a loop

#### Looker (1)

- `looker:view.b`, `sql_table_name`: a Liquid template

<details><summary>Other changed files (1)</summary>

- `README.md` (modified)

</details>
