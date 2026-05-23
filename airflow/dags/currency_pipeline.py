import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.sensors.http import HttpSensor
from docker.types import Mount

from airflow import DAG

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
    api_check = HttpSensor(
        task_id="check_api_availability",
        http_conn_id="currencybeacon_api",
        endpoint="v1/status",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
        extra_options={"check_response": False},
        response_check=lambda response: response.status_code in [200, 401],
    )

    bronze = DockerOperator(
        task_id="ingest_bronze",
        command="python3 ingestion/ingest_bronze.py",
        sla=timedelta(minutes=15),
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

    api_check >> bronze >> silver >> load >> snapshot >> dbt
