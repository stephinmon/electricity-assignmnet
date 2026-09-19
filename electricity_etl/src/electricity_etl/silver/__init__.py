from __future__ import annotations

from pyspark.sql import DataFrame

from electricity_etl.models import EtlContract
from electricity_etl.silver.dim_date import TABLE_NAME as DIM_DATE
from electricity_etl.silver.dim_date import transform as build_dim_date
from electricity_etl.silver.dim_date import union_all as union_dim_dates
from electricity_etl.silver.dim_zone import TABLE_NAME as DIM_ZONE
from electricity_etl.silver.dim_zone import join_to_fact
from electricity_etl.silver.dim_zone import transform as build_dim_zone
from electricity_etl.silver.fact_electricity_flows import (
    CONTRACT_NAME as FLOWS_CONTRACT,
)
from electricity_etl.silver.fact_electricity_flows import transform as transform_flows
from electricity_etl.silver.fact_electricity_mix import CONTRACT_NAME as MIX_CONTRACT
from electricity_etl.silver.fact_electricity_mix import transform as transform_mix
from electricity_etl.utilities import add_date_partitions

FACT_NAME = "fact"
SHARED_DIMS = (DIM_ZONE, DIM_DATE)

SILVER_TRANSFORMS = {
    MIX_CONTRACT: transform_mix,
    FLOWS_CONTRACT: transform_flows,
}


def transform_silver(bronze: DataFrame, contract: EtlContract) -> dict[str, DataFrame]:
    try:
        transform = SILVER_TRANSFORMS[contract.name]
    except KeyError as exc:
        raise ValueError(f"No silver transform registered for {contract.name}") from exc
    fact = transform(bronze, contract)
    if "datetime" not in fact.columns:
        raise ValueError(f"{contract.name} silver must include data timestamp column 'datetime'")
    fact = add_date_partitions(fact, "datetime")
    return {
        FACT_NAME: fact,
        DIM_ZONE: build_dim_zone(bronze.sparkSession),
        DIM_DATE: build_dim_date(fact),
    }


def join_dimensions(silver: DataFrame | dict[str, DataFrame]) -> DataFrame:
    if not isinstance(silver, dict):
        return join_to_fact(silver)
    fact = silver.get(FACT_NAME)
    if fact is None:
        raise ValueError("Silver outputs must include a 'fact' DataFrame")
    return join_to_fact(fact, silver.get(DIM_ZONE))


__all__ = [
    "DIM_DATE",
    "DIM_ZONE",
    "FACT_NAME",
    "SHARED_DIMS",
    "SILVER_TRANSFORMS",
    "join_dimensions",
    "transform_silver",
    "union_dim_dates",
]
