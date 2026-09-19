from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DATE_PARTITIONS = ("year", "month", "day")


def add_date_partitions(df: DataFrame, timestamp_col: str) -> DataFrame:
    """Add Hive partition columns year=YYYY / month=MM / day=DD from a timestamp."""
    ts = F.col(timestamp_col)
    return (
        df.withColumn("dt", F.to_date(ts))
        .withColumn("year", F.date_format(ts, "yyyy"))
        .withColumn("month", F.date_format(ts, "MM"))
        .withColumn("day", F.date_format(ts, "dd"))
    )
