from __future__ import annotations

from pyspark.sql import DataFrame

from electricity_etl.gold.electricity_flows_fr_exports_daily import (
    TABLE_NAME as FR_EXPORTS,
)
from electricity_etl.gold.electricity_flows_fr_exports_daily import (
    transform as transform_fr_exports,
)
from electricity_etl.gold.electricity_flows_fr_imports_daily import (
    TABLE_NAME as FR_IMPORTS,
)
from electricity_etl.gold.electricity_flows_fr_imports_daily import (
    transform as transform_fr_imports,
)
from electricity_etl.gold.electricity_mix_daily_relative import TABLE_NAME as MIX_DAILY
from electricity_etl.gold.electricity_mix_daily_relative import (
    transform as transform_mix_daily,
)
from electricity_etl.models import EtlContract
from electricity_etl.silver import join_dimensions

GOLD_TABLES = {
    MIX_DAILY: transform_mix_daily,
    FR_IMPORTS: transform_fr_imports,
    FR_EXPORTS: transform_fr_exports,
}


def transform_gold(
    silver: DataFrame | dict[str, DataFrame], contract: EtlContract
) -> dict[str, DataFrame]:
    wide = join_dimensions(silver)
    products: dict[str, DataFrame] = {}
    for output in contract.layers.gold.outputs():
        try:
            transform = GOLD_TABLES[output.table]
        except KeyError as exc:
            raise ValueError(f"No gold transform registered for table {output.table}") from exc
        products[output.name or output.table] = transform(wide, contract)
    return products


__all__ = ["GOLD_TABLES", "transform_gold"]
