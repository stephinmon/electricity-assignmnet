from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def parse_payload(df: DataFrame, schema: StructType) -> DataFrame:
    parsed = F.from_json(F.col("payload").cast("string"), schema)
    return (
        df.withColumn("extracted_at", F.to_timestamp("extracted_at"))
        .withColumn("source_url", F.col("source_url").cast("string"))
        .withColumn("parsed", parsed)
        .filter(F.col("parsed").isNotNull())
    )


def explode_payload_data(df: DataFrame, schema: StructType) -> DataFrame:
    """Parse bronze JSON and explode the shared Electricity Maps `data` array."""
    return (
        parse_payload(df, schema)
        .select(
            "extracted_at",
            "source_url",
            "parsed.zone",
            F.col("parsed.temporalGranularity").alias("temporal_granularity"),
            "parsed.unit",
            F.explode_outer("parsed.data").alias("item"),
        )
        .filter(F.col("item").isNotNull())
    )
