from __future__ import annotations

import os
from datetime import datetime, timedelta

from docker.types import Mount

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "energy-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def ingest_container(task_id: str, contract: str) -> DockerOperator:
    secrets = os.environ.get("INGEST_SECRETS_HOST_PATH", "")
    mounts = []
    if secrets:
        mounts.append(
            Mount(
                source=secrets,
                target="/app/.dlt/secrets.toml",
                type="bind",
                read_only=True,
            )
        )
    return DockerOperator(
        task_id=task_id,
        image=os.environ.get("INGEST_IMAGE", "energy-platform-ingest:latest"),
        command=f"--contract {contract} --destination {{{{ params.destination }}}}",
        docker_url=os.environ.get("DOCKER_URL", "unix://var/run/docker.sock"),
        docker_conn_id=None,
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=mounts,
        force_pull=False,
        tty=False,
    )


with DAG(
    dag_id="energy_ingest",
    description="Ingest Electricity Maps mix and flows into landing + bronze.",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"destination": "databricks"},
    tags=["energy", "ingest", "bronze"],
) as dag:
    ingest_container("ingest_electricity_mix", "contracts/electricity_maps.yaml")
    ingest_container("ingest_electricity_flows", "contracts/electricity_flows.yaml")
