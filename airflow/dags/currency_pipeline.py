import os
from datetime import datetime

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

with DAG(
    dag_id="currency_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    bronze = DockerOperator(
        task_id="ingest_bronze",
        command="python3 ingestion/ingest_bronze.py",
        **COMMON,
    )

    silver = DockerOperator(
        task_id="transform_silver",
        command="python3 transformation/transform_silver.py",
        **COMMON,
    )

    load = DockerOperator(
        task_id="load_to_postgres",
        command="python3 scripts/load_silver_to_postgres.py",
        **COMMON,
    )

    snapshot = DockerOperator(
        task_id="run_dbt_snapshot",
        command="bash -c \"set -a && source <(tr -d '\\r' < /app/.env) && set +a && dbt snapshot --project-dir dbt --profiles-dir dbt\"",
        **COMMON,
    )

    dbt = DockerOperator(
        task_id="run_dbt",
        command='bash -c "set -a && source <(tr -d \'\\r\' < /app/.env) && set +a && dbt run --project-dir dbt --profiles-dir dbt"',
        **COMMON,
    )

    bronze >> silver >> load >> snapshot >> dbt
