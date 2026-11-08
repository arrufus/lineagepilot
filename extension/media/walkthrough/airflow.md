# Link your Airflow tasks

LineagePilot reads the DAG files in your workspace **as syntax**. It never imports or runs them, so Airflow and Cosmos need not be installed. By default it looks in every folder called `dags`; change `lineagePilot.airflow.dagFolders` if yours are elsewhere.

It links tasks to the models they run from:

- Astronomer Cosmos `DbtDag` and `DbtTaskGroup`;
- dbt commands in `BashOperator`, `KubernetesPodOperator`, `DockerOperator` and `SSHOperator`;
- SQL operators that read or write a model's table.

The **Airflow view** shows the DAGs and tasks that run the open model, each with how sure the link is and why. Most links are labelled *heuristic*: a path in a DAG file is usually a path inside a container.

What it cannot read, such as tasks built in a loop or a factory, or selectors that need dbt's state, is listed as an unknown at the line that caused it. To answer one, or to say where a container path really is, add `.lineagepilot/airflow.yml` at the root of the dbt project. The extension's README shows the format.
