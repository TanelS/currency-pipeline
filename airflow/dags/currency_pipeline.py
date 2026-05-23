import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PIPELINE_HOST_PATH = os.environ["PIPELINE_HOST_PATH"]
IMAGE = "pipeline-spark"

project_mount = Mount(source=PIPELINE_HOST_PATH, target="/app", type="bind")

COMMON = dict(
    image=IMAGE,
    working_dir="/app",
    mounts=[project_mount],
    network_mode="pipeline-network",
    auto_remove="success",
    do_xcom_push=False,
    mount_tmp_dir=False,
)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="currency_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    bronze = DockerOperator(
        task_id="ingest_bronze",
        command="python3 ingestion/ingest_bronze.py",
        sla=timedelta(minutes=10),
        **COMMON,
    )

    silver = DockerOperator(
        task_id="transform_silver",
        command="python3 transformation/transform_silver.py",
        sla=timedelta(minutes=20),
        **COMMON,
    )

    load = DockerOperator(
        task_id="load_to_postgres",
        command="python3 scripts/load_silver_to_postgres.py",
        sla=timedelta(minutes=25),
        **COMMON,
    )

    snapshot = DockerOperator(
        task_id="run_dbt_snapshot",
        command="bash -c \"set -a && source <(tr -d '\\r' < /app/.env) && set +a && dbt snapshot --project-dir dbt --profiles-dir dbt\"",
        sla=timedelta(minutes=35),
        **COMMON,
    )

    dbt = DockerOperator(
        task_id="run_dbt",
        sla=timedelta(minutes=45),
        command="bash -c \"set -a && source <(tr -d '\\r' < /app/.env) && set +a && dbt run --project-dir dbt --profiles-dir dbt\"",
        **COMMON,
    )

    bronze >> silver >> load >> snapshot >> dbt
