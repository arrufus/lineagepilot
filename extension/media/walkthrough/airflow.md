# Link your Airflow and Dagster pipelines

LineagePilot reads the DAG files in your workspace **as syntax**. It never imports or runs them, so Airflow and Cosmos need not be installed. By default it looks in every folder called `dags`; change `lineagePilot.airflow.dagFolders` if yours are elsewhere.

It links tasks to the models they run from:

- Astronomer Cosmos `DbtDag` and `DbtTaskGroup`;
- dbt commands in `BashOperator`, `KubernetesPodOperator`, `DockerOperator` and `SSHOperator`;
- SQL operators that read or write a model's table.

The **Pipelines view** shows, under Airflow, the DAGs and tasks that run the open model, each with how sure the link is and why. Most links are labelled *heuristic*: a path in a DAG file is usually a path inside a container.

It reads **Dagster** definitions the same way, as syntax, so Dagster need not be installed either. By default it looks in the whole workspace; change `lineagePilot.dagster.paths` to narrow it. Under Dagster, the Pipelines view shows each code location's dbt asset definitions (`@dbt_assets`, or a `DbtProjectComponent` in a `defs.yaml`), the jobs and schedules that run them, and the assets that read dbt.

What it cannot read, such as tasks or assets built in a loop or a factory, selectors that need dbt's state, or a custom Dagster translator, is listed as an unknown at the line that caused it. To answer one, or to say where a container path really is, add `.lineagepilot/lineage.yml` at the root of the dbt project. It can also declare what reads your models from outside dbt, such as a dashboard. The extension's README shows the format.
