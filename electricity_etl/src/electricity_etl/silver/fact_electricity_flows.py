from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from electricity_etl.models import EtlContract
from electricity_etl.schemas import flows_payload_schema
from electricity_etl.utilities import deduplicate, explode_payload_data

TABLE_NAME = "fact_electricity_flows"
CONTRACT_NAME = "electricity_maps_electricity_flows"


def transform(bronze: DataFrame, contract: EtlContract) -> DataFrame:
    exploded = explode_payload_data(bronze, flows_payload_schema())
    base = exploded.select(
        F.col("zone").cast("string").alias("zone"),
        F.col("temporal_granularity").cast("string").alias("temporal_granularity"),
        F.col("unit").cast("string").alias("unit"),
        F.to_timestamp(F.col("item.datetime")).alias("datetime"),
        F.to_timestamp(F.col("item.updatedAt")).alias("updated_at"),
        F.col("item.import").alias("import_map"),
        F.col("item.export").alias("export_map"),
        F.col("extracted_at").cast("timestamp").alias("extracted_at"),
        F.col("source_url").cast("string").alias("source_url"),
    )
    imports = (
        base.select(
            "zone",
            "temporal_granularity",
            "unit",
            "datetime",
            "updated_at",
            "extracted_at",
            "source_url",
            F.explode_outer("import_map").alias("neighbor_zone", "power_mw"),
        )
        .withColumn("flow_direction", F.lit("import"))
        .filter(F.col("neighbor_zone").isNotNull())
    )
    exports = (
        base.select(
            "zone",
            "temporal_granularity",
            "unit",
            "datetime",
            "updated_at",
            "extracted_at",
            "source_url",
            F.explode_outer("export_map").alias("neighbor_zone", "power_mw"),
        )
        .withColumn("flow_direction", F.lit("export"))
        .filter(F.col("neighbor_zone").isNotNull())
    )
    frame = (
        imports.unionByName(exports)
        .filter(F.col("zone").isNotNull() & F.col("datetime").isNotNull())
        .select(
            F.col("zone").cast("string").alias("zone"),
            F.col("neighbor_zone").cast("string").alias("neighbor_zone"),
            F.col("flow_direction").cast("string").alias("flow_direction"),
            F.col("power_mw").cast("double").alias("power_mw"),
            F.col("unit").cast("string").alias("unit"),
            F.col("temporal_granularity").cast("string").alias("temporal_granularity"),
            F.col("datetime").cast("timestamp").alias("datetime"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("extracted_at").cast("timestamp").alias("extracted_at"),
            F.col("source_url").cast("string").alias("source_url"),
        )
    )
    return deduplicate(frame, contract.business_keys)
