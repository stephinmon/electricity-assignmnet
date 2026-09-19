from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {
    "owner": "energy-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _job_target() -> dict[str, int | str]:
    job_id = os.environ.get("ELECTRICITY_ETL_JOB_ID", "").strip()
    if job_id:
        return {"job_id": int(job_id)}
    return {
        "job_name": os.environ.get("ELECTRICITY_ETL_JOB_NAME", "electricity-etl"),
    }


with DAG(
    dag_id="electricity_etl",
    description="Run the Databricks electricity-etl job (silver + gold tables).",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["energy", "etl", "databricks"],
) as dag:
    DatabricksRunNowOperator(
        task_id="run_electricity_etl_job",
        databricks_conn_id="databricks_default",
        wait_for_termination=True,
        polling_period_seconds=30,
        **_job_target(),
    )
