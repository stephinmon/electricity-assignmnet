from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from electricity_etl.gold._common import hours_expr
from electricity_etl.schemas import FRANCE_ZONE


def daily_net_by_neighbor(silver: DataFrame) -> DataFrame:
    hours = hours_expr()
    france = silver.filter(F.col("zone") == F.lit(FRANCE_ZONE)).withColumn(
        "energy_mwh", F.col("power_mw") * hours
    )
    daily = france.groupBy("dt", "zone", "zone_name", "country", "region", "neighbor_zone").agg(
        F.min("datetime").alias("interval_start"),
        F.max("datetime").alias("interval_end"),
        F.max("extracted_at").alias("last_extracted_at"),
        F.max("neighbor_zone_name").alias("neighbor_zone_name"),
        F.max("neighbor_country").alias("neighbor_country"),
        F.max("neighbor_region").alias("neighbor_region"),
        F.sum(
            F.when(F.col("flow_direction") == "import", F.col("energy_mwh")).otherwise(0.0)
        ).alias("import_mwh"),
        F.sum(
            F.when(F.col("flow_direction") == "export", F.col("energy_mwh")).otherwise(0.0)
        ).alias("export_mwh"),
    )
    return (
        daily.withColumn("net_import_mwh", F.col("import_mwh") - F.col("export_mwh"))
        .withColumn("computed_at", F.current_timestamp())
        .withColumn("reference_date", F.col("dt"))
        .withColumn("grain", F.lit("daily"))
        .withColumn("unit_energy", F.lit("MWh"))
        .withColumn("dest_zone", F.col("zone"))
        .withColumn("dest_zone_name", F.col("zone_name"))
    )
