from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

TABLE_NAME = "dim_zone"

ZONE_METADATA: dict[str, dict[str, str]] = {
    "AT": {"zone_name": "Austria", "country": "Austria", "region": "Europe"},
    "BE": {"zone_name": "Belgium", "country": "Belgium", "region": "Europe"},
    "DE": {"zone_name": "Germany", "country": "Germany", "region": "Europe"},
    "ES": {"zone_name": "Spain", "country": "Spain", "region": "Europe"},
    "FR": {"zone_name": "France", "country": "France", "region": "Europe"},
    "GB": {"zone_name": "Great Britain", "country": "United Kingdom", "region": "Europe"},
    "IT": {"zone_name": "Italy", "country": "Italy", "region": "Europe"},
    "NL": {"zone_name": "Netherlands", "country": "Netherlands", "region": "Europe"},
    "PL": {"zone_name": "Poland", "country": "Poland", "region": "Europe"},
    "SE": {"zone_name": "Sweden", "country": "Sweden", "region": "Europe"},
}


def transform(spark: SparkSession) -> DataFrame:
    rows = [
        (zone, meta["zone_name"], meta["country"], meta["region"])
        for zone, meta in sorted(ZONE_METADATA.items())
    ]
    schema = StructType(
        [
            StructField("zone", StringType(), False),
            StructField("zone_name", StringType(), False),
            StructField("country", StringType(), False),
            StructField("region", StringType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def join_to_fact(fact: DataFrame, dim_zone: DataFrame | None = None) -> DataFrame:
    dim = dim_zone if dim_zone is not None else transform(fact.sparkSession)
    frame = fact
    if "zone_name" not in frame.columns:
        frame = frame.join(dim, on="zone", how="left")
    if "neighbor_zone" in frame.columns and "neighbor_zone_name" not in frame.columns:
        neighbor = (
            dim.withColumnRenamed("zone", "neighbor_zone")
            .withColumnRenamed("zone_name", "neighbor_zone_name")
            .withColumnRenamed("country", "neighbor_country")
            .withColumnRenamed("region", "neighbor_region")
        )
        frame = frame.join(neighbor, on="neighbor_zone", how="left")
    return frame
