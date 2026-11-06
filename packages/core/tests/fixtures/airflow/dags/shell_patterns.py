"""Fixture for spike S3: shell-style dbt calls in an Airflow DAG (Bash, Kubernetes pod, Docker, @task.bash)."""
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.docker.operators.docker import DockerOperator

HERE = os.path.dirname(os.path.realpath(__file__))
SHOP = os.path.join(HERE, "..", "dbt", "shop")
TARGET = os.environ.get("DBT_TARGET", "dev")
SELECT = "tag:daily"
IMAGE_SHOP = "/usr/local/airflow/dbt/shop"

with DAG(dag_id="shell_dag", start_date=datetime(2024, 1, 1)) as shell_dag:
    start = EmptyOperator(task_id="start")
    cd_run = BashOperator(task_id="cd_run",
                          bash_command=f"cd {SHOP} && dbt run -s {SELECT} --exclude fct_revenue --target {TARGET}")
    multi = BashOperator(
        task_id="multi",
        bash_command=(f"dbt seed --project-dir {SHOP}; "
                      f"dbt --no-write-json build --select stg_customers+1 --project-dir {SHOP} && "
                      f"dbt test -m dim_customers --project-dir={SHOP}"),
    )
    selector = BashOperator(task_id="selector", bash_command="dbt run --selector core_not_python --project-dir " + SHOP)
    jinja = BashOperator(task_id="jinja", bash_command="dbt run --select {{ params.models }} --project-dir " + SHOP)
    relative = BashOperator(task_id="relative", bash_command="cd dbt && cd shop && dbt run --select resource_type:model,tag:pii")
    env_dir = BashOperator(task_id="env_dir", bash_command="dbt snapshot", env={"DBT_PROJECT_DIR": SHOP})
    with_cwd = BashOperator(task_id="with_cwd", bash_command="dbt run -s marts", cwd=SHOP)
    not_dbt = BashOperator(task_id="not_dbt", bash_command="echo hello")
    script = BashOperator(task_id="script", bash_command="scripts/run_dbt.sh ")
    unknown_cmd = BashOperator(task_id="unknown_cmd", bash_command=os.environ["CMD"])
    deps = BashOperator(task_id="deps", bash_command="dbt deps --project-dir %s" % SHOP)
    pod = KubernetesPodOperator(task_id="pod", name="dbt", image="dbt:1", cmds=["dbt"],
                                arguments=["build", "--select", "fct_orders", "--project-dir", IMAGE_SHOP])
    pod_sh = KubernetesPodOperator(task_id="pod_sh", name="dbt", image="dbt:1", cmds=["/bin/bash", "-c"],
                                   arguments=["cd /app/dbt/shop && dbt run --select stg_orders"])
    pod_entry = KubernetesPodOperator(task_id="pod_entry", name="dbt", image="dbt:1",
                                      arguments=["run", "--select", "dim_products", "--project-dir", IMAGE_SHOP])
    docker = DockerOperator(task_id="docker", image="dbt:1",
                            command="dbt run --select stg_products --project-dir {}".format(IMAGE_SHOP))
    docker_list = DockerOperator(task_id="docker_list", image="dbt:1", command=["dbt", "seed", "--project-dir", IMAGE_SHOP])
    start >> cd_run >> multi
    start << selector
    chain(start, [jinja, relative], [env_dir, with_cwd], pod)
    pod_sh.set_upstream([pod, docker])


@dag(schedule=None, start_date=datetime(2024, 1, 1))
def decorated_dag():
    @task.bash
    def bash_task(model: str = "stg_orders"):
        return f"dbt run --select {model} --project-dir {SHOP}"

    @task
    def py_task():
        import subprocess

        subprocess.run(["dbt", "run", "--select", "x"])

    bash_task() >> py_task()


decorated_dag()

legacy_dag = DAG("positional_dag", start_date=datetime(2024, 1, 1))
attached = BashOperator(task_id="attached", bash_command="dbt run --project-dir " + SHOP + " --models fct_orders", dag=legacy_dag)
