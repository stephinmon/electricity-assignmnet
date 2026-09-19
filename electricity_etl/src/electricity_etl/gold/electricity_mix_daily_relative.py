from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from electricity_etl.gold._common import hours_expr, pct
from electricity_etl.models import EtlContract
from electricity_etl.schemas import GENERATION_SOURCES

TABLE_NAME = "electricity_mix_daily_relative"
CONTRACT_NAME = "electricity_maps_electricity_mix"


def transform(silver: DataFrame, contract: EtlContract) -> DataFrame:
    hours = hours_expr()
    source_mwh = {
        name: F.coalesce(F.col(f"mix_{name}"), F.lit(0.0)) * hours for name in GENERATION_SOURCES
    }
    aggregated = silver.groupBy("dt", "zone", "zone_name", "country", "region").agg(
        F.min("datetime").alias("interval_start"),
        F.max("datetime").alias("interval_end"),
        F.max("extracted_at").alias("last_extracted_at"),
        *[F.sum(expr).alias(f"{name}_mwh") for name, expr in source_mwh.items()],
        F.sum(F.coalesce(F.col("mix_hydro_storage_charge"), F.lit(0.0)) * hours).alias(
            "hydro_storage_charge_mwh"
        ),
        F.sum(F.coalesce(F.col("mix_hydro_storage_discharge"), F.lit(0.0)) * hours).alias(
            "hydro_storage_discharge_mwh"
        ),
        F.sum(F.coalesce(F.col("mix_battery_storage_charge"), F.lit(0.0)) * hours).alias(
            "battery_storage_charge_mwh"
        ),
        F.sum(F.coalesce(F.col("mix_battery_storage_discharge"), F.lit(0.0)) * hours).alias(
            "battery_storage_discharge_mwh"
        ),
        F.sum(F.coalesce(F.col("mix_flows_imports"), F.lit(0.0)) * hours).alias("imports_mwh"),
        F.sum(F.coalesce(F.col("mix_flows_exports"), F.lit(0.0)) * hours).alias("exports_mwh"),
    )
    generation = None
    for name in GENERATION_SOURCES:
        col = F.col(f"{name}_mwh")
        generation = col if generation is None else generation + col
    frame = aggregated.withColumn("generation_mwh", generation)
    for name in GENERATION_SOURCES:
        frame = frame.withColumn(f"{name}_pct", pct(F.col(f"{name}_mwh"), F.col("generation_mwh")))
    frame = (
        frame.withColumn("computed_at", F.current_timestamp())
        .withColumn("reference_date", F.col("dt"))
        .withColumn("grain", F.lit("daily"))
        .withColumn("unit_energy", F.lit("MWh"))
        .withColumn("unit_share", F.lit("percent"))
    )
    columns = [
        "dt",
        "reference_date",
        "zone",
        "zone_name",
        "country",
        "region",
        "grain",
        "unit_energy",
        "unit_share",
        "interval_start",
        "interval_end",
        "computed_at",
        "last_extracted_at",
        "generation_mwh",
        *[f"{name}_mwh" for name in GENERATION_SOURCES],
        *[f"{name}_pct" for name in GENERATION_SOURCES],
        "hydro_storage_charge_mwh",
        "hydro_storage_discharge_mwh",
        "battery_storage_charge_mwh",
        "battery_storage_discharge_mwh",
        "imports_mwh",
        "exports_mwh",
    ]
    return frame.select(*columns)
