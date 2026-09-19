from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "electricity-etl") -> SparkSession:
    """Reuse a Databricks session when present; otherwise start local Spark."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    builder = (
        SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC")
    )
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return builder.getOrCreate()

    builder = (
        builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
        .config(
            "spark.driver.bindAddress",
            os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1"),
        )
    )
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:
        return builder.getOrCreate()

    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
