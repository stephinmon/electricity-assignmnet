from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from electricity_etl.databricks import (
    fetch_sql,
    publish_parquet_to_uc,
    uses_unity_catalog,
)
from electricity_etl.models import EtlContract, LayerContract, LayerTableContract

BRONZE_SCHEMA = StructType(
    [
        StructField("extracted_at", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def resolve_path(path: str, output_root: str | None) -> str:
    if not output_root:
        return path
    return str(Path(output_root) / path.lstrip("/"))


def _read_uc_table(spark: SparkSession, fqn: str) -> DataFrame:
    if uses_unity_catalog(spark):
        return spark.table(fqn)
    names, rows = fetch_sql(f"SELECT * FROM {fqn}")
    if not names:
        return spark.createDataFrame([], BRONZE_SCHEMA)
    return spark.createDataFrame(rows, schema=names)


def read_bronze(
    spark: SparkSession,
    contract: EtlContract,
    *,
    bronze_path: str | None = None,
) -> DataFrame:
    # If no bronze_path is provided, read from Unity Catalog or fetch via SQL.
    if not bronze_path:
        # On Databricks/Unity Catalog, just use the table directly.
        if uses_unity_catalog(spark):
            return spark.table(contract.bronze_fqn)
        # Not on Unity Catalog: fetch table rows via SQL and cast fields to STRING type.
        names, rows = fetch_sql(
            "SELECT CAST(extracted_at AS STRING) AS extracted_at, "
            "CAST(source_url AS STRING) AS source_url, "
            f"CAST(payload AS STRING) AS payload FROM {contract.bronze_fqn}"
        )
        # If there are no names (columns), return an empty DataFrame conforming to BRONZE_SCHEMA.
        if not names:
            return spark.createDataFrame([], BRONZE_SCHEMA)
        # Otherwise, create DataFrame from fetched rows and standard schema.
        return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

    if bronze_path.endswith(".parquet"):
        return spark.read.parquet(bronze_path)

    raw = spark.read.json(bronze_path)
    if "payload" in raw.columns:
        return raw
    return raw.select(
        F.current_timestamp().alias("extracted_at"),
        F.lit(bronze_path).alias("source_url"),
        F.to_json(F.struct(*[raw[column] for column in raw.columns])).alias("payload"),
    )


def read_silver(
    spark: SparkSession,
    contract: EtlContract,
    *,
    output_root: str | None = None,
    include: set[str] | None = None,
) -> dict[str, DataFrame]:
    silver = contract.layers.silver
    if silver is None:
        raise ValueError(f"{contract.name} has no silver layer")
    products: dict[str, DataFrame] = {}
    for output in silver.outputs():
        key = output.name or output.table
        if include and key not in include and output.table not in include:
            continue
        if output_root:
            local = Path(resolve_path(silver.parquet_path(output.table, output.path), output_root))
            if local.exists():
                products[key] = spark.read.parquet(str(local))
                continue
            continue
        products[key] = _read_uc_table(spark, silver.qualified_table(output.table))
    return products


def write_delta_and_parquet(
    df: DataFrame,
    *,
    layer: LayerContract,
    output: LayerTableContract,
    output_root: str | None = None,
    write_tables: bool = True,
) -> None:
    if output.partition_by is not None:
        partition_by = output.partition_by
    else:
        partition_by = layer.partition_by or ["year", "month", "day"]
    parquet_path = resolve_path(layer.parquet_path(output.table, output.path), output_root)
    table = layer.qualified_table(output.table)

    parquet_writer = df.write.mode("overwrite")
    if partition_by:
        parquet_writer = parquet_writer.partitionBy(*partition_by)
    parquet_writer.parquet(parquet_path)
    print(f"Wrote parquet {parquet_path}")

    if not write_tables:
        return
    if uses_unity_catalog(df.sparkSession):
        table_writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            table_writer = table_writer.partitionBy(*partition_by)
        table_writer.saveAsTable(table)
        print(f"Wrote delta table {table}")
        return

    publish_parquet_to_uc(
        Path(parquet_path),
        volume_path=layer.parquet_path(output.table, output.path),
        table=table,
        partition_by=partition_by,
    )
