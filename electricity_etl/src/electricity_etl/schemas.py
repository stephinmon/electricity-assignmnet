from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)

GENERATION_SOURCES = (
    "nuclear",
    "geothermal",
    "biomass",
    "coal",
    "wind",
    "solar",
    "hydro",
    "gas",
    "oil",
    "unknown",
)

GRANULARITY_HOURS = {
    "hourly": 1.0,
    "5_minutes": 5.0 / 60.0,
    "15_minutes": 0.25,
    "daily": 24.0,
    "monthly": 24.0 * 30.0,
    "quarterly": 24.0 * 91.0,
    "yearly": 24.0 * 365.0,
}

FRANCE_ZONE = "FR"


def _storage_struct() -> StructType:
    return StructType(
        [
            StructField("charge", DoubleType(), True),
            StructField("discharge", DoubleType(), True),
        ]
    )


def mix_payload_schema() -> StructType:
    mix_struct = StructType(
        [
            *[StructField(name, DoubleType(), True) for name in GENERATION_SOURCES],
            StructField("hydro storage", _storage_struct(), True),
            StructField("battery storage", _storage_struct(), True),
            StructField(
                "flows",
                StructType(
                    [
                        StructField("imports", DoubleType(), True),
                        StructField("exports", DoubleType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
    data_struct = StructType(
        [
            StructField("datetime", StringType(), True),
            StructField("updatedAt", StringType(), True),
            StructField("breakdownType", StringType(), True),
            StructField("isEstimated", BooleanType(), True),
            StructField("estimationMethod", StringType(), True),
            StructField("mix", mix_struct, True),
        ]
    )
    return StructType(
        [
            StructField("zone", StringType(), True),
            StructField("temporalGranularity", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("data", ArrayType(data_struct), True),
        ]
    )


def flows_payload_schema() -> StructType:
    data_struct = StructType(
        [
            StructField("datetime", StringType(), True),
            StructField("updatedAt", StringType(), True),
            StructField("import", MapType(StringType(), DoubleType()), True),
            StructField("export", MapType(StringType(), DoubleType()), True),
        ]
    )
    return StructType(
        [
            StructField("zone", StringType(), True),
            StructField("temporalGranularity", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("data", ArrayType(data_struct), True),
        ]
    )
