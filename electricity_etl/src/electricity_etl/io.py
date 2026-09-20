from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from electricity_etl.databricks import (
    fetch_sql,
    merge_parquet_to_uc,
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


_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def resolve_path(path: str, output_root: str | None) -> str:
    if not output_root:
        return path
    return str(Path(output_root) / path.lstrip("/"))


def merge_predicate(keys: list[str], left: str = "t", right: str = "s") -> str:
    if not keys:
        raise ValueError("merge_keys are required for a silver merge")
    parts = []
    for key in keys:
        if not _SAFE_IDENT.match(key):
            raise ValueError(f"Unsafe merge key: {key!r}")
        parts.append(f"{left}.`{key}` <=> {right}.`{key}`")
    return " AND ".join(parts)


def _spark_table_exists(spark: SparkSession, table: str) -> bool:
    try:
        if spark.catalog.tableExists(table):
            return True
    except Exception:
        pass
    parts = table.split(".")
    try:
        if len(parts) == 3 and spark.catalog.tableExists(f"{parts[0]}.{parts[1]}", parts[2]):
            return True
    except Exception:
        pass
    try:
        spark.read.table(table).limit(0).collect()
        return True
    except Exception:
        return False


def _write_parquet(
    df: DataFrame,
    parquet_path: str,
    partition_by: list[str],
    merge_keys: list[str] | None,
) -> None:
    outgoing = df
    target = Path(parquet_path)
    if merge_keys and target.exists():
        try:
            existing = df.sparkSession.read.parquet(parquet_path)
            if existing.take(1):
                outgoing = existing.join(df, on=list(merge_keys), how="left_anti").unionByName(
                    df, allowMissingColumns=True
                )
        except Exception:
            outgoing = df
    writer = outgoing.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(parquet_path)
    print(f"Wrote parquet {parquet_path}")


def _create_or_merge_delta(
    df: DataFrame,
    *,
    table: str,
    merge_keys: list[str],
    partition_by: list[str],
) -> None:
    spark = df.sparkSession
    try:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    except Exception:
        pass
    if not _spark_table_exists(spark, table):
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table)
        print(f"Created delta table {table}")
        return
    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(df.alias("s"), merge_predicate(merge_keys))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged into delta table {table}")


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
    merge_keys: list[str] | None = None,
) -> None:
    if output.partition_by is not None:
        partition_by = output.partition_by
    else:
        partition_by = layer.partition_by or ["year", "month", "day"]
    parquet_path = resolve_path(layer.parquet_path(output.table, output.path), output_root)
    table = layer.qualified_table(output.table)
    keys = merge_keys if merge_keys is not None else output.merge_keys
    if (layer.write_disposition or "overwrite").lower() != "merge":
        keys = None

    if not write_tables:
        _write_parquet(df, parquet_path, partition_by, keys)
        return

    if uses_unity_catalog(df.sparkSession):
        if keys:
            _create_or_merge_delta(
                df, table=table, merge_keys=keys, partition_by=partition_by
            )
            _write_parquet(df.sparkSession.table(table), parquet_path, partition_by, None)
        else:
            _write_parquet(df, parquet_path, partition_by, None)
            table_writer = (
                df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            )
            if partition_by:
                table_writer = table_writer.partitionBy(*partition_by)
            table_writer.saveAsTable(table)
            print(f"Wrote delta table {table}")
        return

    _write_parquet(df, parquet_path, partition_by, keys)
    if keys:
        merge_parquet_to_uc(
            Path(parquet_path),
            volume_path=layer.parquet_path(output.table, output.path),
            table=table,
            partition_by=partition_by,
            merge_keys=keys,
        )
        return
    publish_parquet_to_uc(
        Path(parquet_path),
        volume_path=layer.parquet_path(output.table, output.path),
        table=table,
        partition_by=partition_by,
    )
