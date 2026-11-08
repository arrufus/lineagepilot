# Or try the sample project

The sample is a small dbt project written for LineagePilot:

- a source, staging models, a mart and the models built on it;
- an exposure, and owners;
- one model whose SQL cannot be parsed, to show what the review does with what it cannot check;
- three Airflow DAG files: a Cosmos task group, a `dbt run` in a `BashOperator` with a SQL task after it, and tasks built in a loop;
- real dbt 1.12 artifacts in `target/`, and a prepared change: renaming a column of the mart.

You need neither dbt nor Airflow to try it. **Open Sample Project** asks where to put a copy, then opens it. When VS Code asks whether you trust the folder, trust it: it holds only the sample, and the review needs Git, which Restricted Mode turns off.

Open `models/marts/orders.sql` to see the CodeLens, then carry on with the next steps.
