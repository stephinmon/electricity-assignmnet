from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import dlt
from dlt.common import logger as dlt_logger
from dlt.destinations import databricks
from dlt.destinations.adapters import databricks_adapter

from energy_platform.contracts.models import IngestionContract
from energy_platform.ingestion.source import (
    collect_raw_payloads,
    electricity_maps_source,
    to_bronze_rows,
)
from energy_platform.ingestion.uc_volume import (
    landing_filename,
    publish_jsonl_to_volume,
    volume_path,
    write_jsonl,
)

LOCAL_JSON_ROOT = Path(".dlt/landing_zone")


def dlt_logs_to_stdout() -> None:
    """dlt logs INFO to stderr; Airflow treats stderr as ERROR. Keep the same messages on stdout."""

    def retarget(log: logging.Logger | None) -> None:
        if log is None:
            return
        for handler in log.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setStream(sys.stdout)

    retarget(dlt_logger.LOGGER)
    retarget(logging.getLogger("dlt"))


def build_destination(contract: IngestionContract, destination_name: str | None = None) -> Any:
    name = destination_name or contract.bronze.destination
    if name == "duckdb":
        return "duckdb"
    if name != "databricks":
        raise ValueError(f"Unsupported destination: {name}")

    credentials: dict[str, Any] = {}
    if contract.bronze.catalog:
        credentials["catalog"] = contract.bronze.catalog
    kwargs: dict[str, Any] = {"keep_staged_files": True}
    if credentials:
        kwargs["credentials"] = credentials
    if contract.bronze.staging_volume_name:
        kwargs["staging_volume_name"] = contract.bronze.staging_volume_name
    return databricks(**kwargs)


def apply_databricks_hints(source, contract: IngestionContract) -> None:
    resource = source.resources[contract.bronze.table]
    kwargs: dict[str, Any] = {
        "table_format": contract.bronze.table_format.upper(),
        "table_comment": contract.bronze.table_comment
        or contract.description
        or contract.bronze.table,
        "column_hints": {
            column.name: {"column_comment": column.description}
            for column in contract.schema_.columns
            if column.description
        },
    }
    if contract.bronze.partition_by:
        kwargs["partition"] = contract.bronze.partition_by
    if contract.bronze.cluster:
        kwargs["cluster"] = contract.bronze.cluster
    databricks_adapter(resource, **kwargs)


def run_bronze_pipeline(
    contract: IngestionContract,
    *,
    destination_name: str | None = None,
    pipeline_name: str | None = None,
) -> Any:
    payloads = collect_raw_payloads(contract)
    resolved_destination = destination_name or contract.bronze.destination
    if resolved_destination == "databricks":
        target_dir = LOCAL_JSON_ROOT / contract.landing_prefix()
        target_dir.mkdir(parents=True, exist_ok=True)
        write_jsonl(
            [item.payload for item in payloads],
            target_dir / landing_filename(contract),
        )
        uploaded = publish_jsonl_to_volume(contract, target_dir)
        print(f"Published {len(uploaded)} JSONL file(s) to {volume_path(contract)}")
        for path in uploaded:
            print(f"  {path}")

    destination = build_destination(contract, destination_name)
    source = electricity_maps_source(contract, records=to_bronze_rows(payloads))
    if resolved_destination == "databricks":
        apply_databricks_hints(source, contract)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name or f"{contract.name}_bronze",
        destination=destination,
        dataset_name=contract.bronze.schema_name,
        progress="log",
    )
    dlt_logs_to_stdout()
    load_info = pipeline.run(source, loader_file_format=contract.bronze.file_format)
    load_info.raise_on_failed_jobs()
    return load_info
