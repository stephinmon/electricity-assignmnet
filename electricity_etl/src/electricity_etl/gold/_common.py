from __future__ import annotations

from pyspark.sql import functions as F

from electricity_etl.schemas import GRANULARITY_HOURS


def hours_expr() -> F.Column:
    mapping = F.create_map(
        *[item for key, value in GRANULARITY_HOURS.items() for item in (F.lit(key), F.lit(value))]
    )
    return F.coalesce(mapping[F.col("temporal_granularity")], F.lit(1.0))


def pct(numerator: F.Column, denominator: F.Column) -> F.Column:
    return F.when(denominator > 0, F.round(numerator / denominator * 100.0, 6)).otherwise(
        F.lit(None).cast("double")
    )
