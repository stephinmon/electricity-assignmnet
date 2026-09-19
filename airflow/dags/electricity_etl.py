from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

default_args = {
    "owner": "energy-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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
        job_name="electricity-etl",
        wait_for_termination=True,
        polling_period_seconds=30,
    )
