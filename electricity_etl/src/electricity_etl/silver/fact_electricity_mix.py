from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from electricity_etl.models import EtlContract
from electricity_etl.schemas import mix_payload_schema
from electricity_etl.utilities import deduplicate, explode_payload_data

TABLE_NAME = "fact_electricity_mix"
CONTRACT_NAME = "electricity_maps_electricity_mix"


def transform(bronze: DataFrame, contract: EtlContract) -> DataFrame:
    exploded = explode_payload_data(bronze, mix_payload_schema())
    mix = F.col("item.mix")
    frame = exploded.select(
        F.col("zone").cast("string").alias("zone"),
        F.col("temporal_granularity").cast("string").alias("temporal_granularity"),
        F.col("unit").cast("string").alias("unit"),
        F.to_timestamp(F.col("item.datetime")).alias("datetime"),
        F.to_timestamp(F.col("item.updatedAt")).alias("updated_at"),
        F.col("item.breakdownType").cast("string").alias("breakdown_type"),
        F.col("item.isEstimated").cast("boolean").alias("is_estimated"),
        F.col("item.estimationMethod").cast("string").alias("estimation_method"),
        mix.getField("nuclear").cast("double").alias("mix_nuclear"),
        mix.getField("geothermal").cast("double").alias("mix_geothermal"),
        mix.getField("biomass").cast("double").alias("mix_biomass"),
        mix.getField("coal").cast("double").alias("mix_coal"),
        mix.getField("wind").cast("double").alias("mix_wind"),
        mix.getField("solar").cast("double").alias("mix_solar"),
        mix.getField("hydro").cast("double").alias("mix_hydro"),
        mix.getField("gas").cast("double").alias("mix_gas"),
        mix.getField("oil").cast("double").alias("mix_oil"),
        mix.getField("unknown").cast("double").alias("mix_unknown"),
        F.col("item.mix")["hydro storage"]["charge"]
        .cast("double")
        .alias("mix_hydro_storage_charge"),
        F.col("item.mix")["hydro storage"]["discharge"].cast("double").alias(
            "mix_hydro_storage_discharge"
        ),
        F.col("item.mix")["battery storage"]["charge"].cast("double").alias(
            "mix_battery_storage_charge"
        ),
        F.col("item.mix")["battery storage"]["discharge"].cast("double").alias(
            "mix_battery_storage_discharge"
        ),
        F.col("item.mix")["flows"]["imports"].cast("double").alias("mix_flows_imports"),
        F.col("item.mix")["flows"]["exports"].cast("double").alias("mix_flows_exports"),
        F.col("extracted_at").cast("timestamp").alias("extracted_at"),
        F.col("source_url").cast("string").alias("source_url"),
    ).filter(F.col("zone").isNotNull() & F.col("datetime").isNotNull())
    return deduplicate(frame, contract.business_keys)
