from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate(df: DataFrame, keys: list[str]) -> DataFrame:
    """Keep the latest row per business key (`updated_at`, then `extracted_at`)."""
    missing = [key for key in keys if key not in df.columns]
    if missing:
        raise ValueError(f"Dedup keys not present after flatten: {missing}")
    frame = df
    for key in keys:
        frame = frame.filter(F.col(key).isNotNull())
    if "datetime" in frame.columns:
        frame = frame.withColumn("datetime", F.date_trunc("second", F.col("datetime")))
    if "updated_at" in frame.columns:
        frame = frame.withColumn("updated_at", F.date_trunc("second", F.col("updated_at")))
    if "extracted_at" in frame.columns:
        frame = frame.withColumn("extracted_at", F.date_trunc("second", F.col("extracted_at")))
    order_cols = []
    if "updated_at" in frame.columns:
        order_cols.append(F.col("updated_at").desc_nulls_last())
    if "extracted_at" in frame.columns:
        order_cols.append(F.col("extracted_at").desc_nulls_last())
    if not order_cols:
        order_cols = [F.monotonically_increasing_id().desc()]
    window = Window.partitionBy(*[F.col(key) for key in keys]).orderBy(*order_cols)
    return (
        frame.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .dropDuplicates(keys)
    )
