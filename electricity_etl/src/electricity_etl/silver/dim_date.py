from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TABLE_NAME = "dim_date"


def transform(fact: DataFrame) -> DataFrame:
    return (
        fact.select("dt", "year", "month", "day")
        .filter(F.col("dt").isNotNull())
        .dropDuplicates(["dt"])
    )


def union_all(frames: list[DataFrame]) -> DataFrame:
    combined = frames[0]
    for extra in frames[1:]:
        combined = combined.unionByName(extra, allowMissingColumns=True)
    return combined.dropDuplicates(["dt"])
