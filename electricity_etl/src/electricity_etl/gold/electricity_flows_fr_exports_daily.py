from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from electricity_etl.gold._france_daily import daily_net_by_neighbor
from electricity_etl.models import EtlContract

TABLE_NAME = "electricity_flows_fr_exports_daily"
CONTRACT_NAME = "electricity_maps_electricity_flows"


def transform(silver: DataFrame, contract: EtlContract) -> DataFrame:
    daily = daily_net_by_neighbor(silver)
    return daily.filter(F.col("export_mwh") > 0).select(
        F.col("dt"),
        F.col("reference_date"),
        F.col("zone").alias("source_zone"),
        F.col("zone_name").alias("source_zone_name"),
        F.col("country").alias("source_country"),
        F.col("region").alias("source_region"),
        F.col("neighbor_zone").alias("dest_zone"),
        F.col("neighbor_zone_name").alias("dest_zone_name"),
        F.col("neighbor_country").alias("dest_country"),
        F.col("neighbor_region").alias("dest_region"),
        F.col("export_mwh"),
        F.col("import_mwh"),
        F.col("net_import_mwh"),
        F.col("interval_start"),
        F.col("interval_end"),
        F.col("computed_at"),
        F.col("last_extracted_at"),
        F.col("grain"),
        F.col("unit_energy"),
    )
