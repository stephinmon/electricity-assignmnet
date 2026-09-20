import json

import pytest

from electricity_etl.contract import load_contracts
from electricity_etl.gold import transform_gold
from electricity_etl.silver import transform_silver

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from electricity_etl.spark import get_spark

    try:
        session = get_spark("electricity-etl-tests")
        session.conf.set("spark.sql.session.timeZone", "UTC")
    except Exception as exc:
        pytest.skip(f"Spark is not available: {exc}")
    yield session
    session.stop()


def _contracts():
    return {item.name: item for item in load_contracts()}


def _utc_clock(frame, column="extracted_at"):
    from pyspark.sql import functions as F

    formatted = F.date_format(column, "yyyy-MM-dd HH:mm:ss")
    return {row[0] for row in frame.select(formatted).collect()}


def test_silver_and_gold_mix(spark):
    contract = _contracts()["electricity_maps_electricity_mix"]
    payload = {
        "zone": "DE",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "data": [
            {
                "datetime": "2026-09-16T12:00:00Z",
                "updatedAt": "2026-09-16T12:05:00Z",
                "breakdownType": "flow-traced",
                "isEstimated": False,
                "estimationMethod": None,
                "mix": {
                    "nuclear": 10,
                    "geothermal": 0,
                    "biomass": 0,
                    "coal": 10,
                    "wind": 60,
                    "solar": 20,
                    "hydro": 0,
                    "gas": 0,
                    "oil": 0,
                    "unknown": 0,
                    "hydro storage": {"charge": 1, "discharge": 0},
                    "battery storage": {"charge": 0, "discharge": 2},
                    "flows": {"imports": 5, "exports": 3},
                },
            }
        ],
    }
    bronze = spark.createDataFrame(
        [
            ("2026-09-17T01:00:00Z", "https://example/mix?zone=DE", json.dumps(payload)),
            ("2026-09-17T00:30:00Z", "https://example/mix?zone=DE", json.dumps(payload)),
        ],
        ["extracted_at", "source_url", "payload"],
    )
    silver = transform_silver(bronze, contract)
    fact = silver["fact"]
    rows = fact.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.zone == "DE"
    assert "zone_name" not in fact.columns
    assert silver["dim_zone"].filter("zone = 'DE'").collect()[0].zone_name == "Germany"
    assert row.mix_wind == 60.0
    assert row.mix_hydro_storage_charge == 1.0
    assert str(row.dt) == "2026-09-16"
    assert row.year == "2026"
    assert row.month == "09"
    assert row.day == "16"
    assert row.extracted_at is not None
    assert str(silver["dim_date"].collect()[0].dt) == "2026-09-16"

    gold = transform_gold(silver, contract)
    product = next(iter(gold.values())).collect()[0]
    assert product.wind_pct == 60.0
    assert product.solar_pct == 20.0
    assert product.generation_mwh == 100.0


def test_silver_mix_keeps_latest_duplicate(spark):
    contract = _contracts()["electricity_maps_electricity_mix"]
    older = {
        "zone": "FR",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "data": [
            {
                "datetime": "2026-09-16T12:00:00.000Z",
                "updatedAt": "2026-09-16T12:01:00Z",
                "mix": {"wind": 10, "solar": 10, "nuclear": 0, "coal": 0},
            }
        ],
    }
    newer = {
        "zone": "FR",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "data": [
            {
                "datetime": "2026-09-16T12:00:00Z",
                "updatedAt": "2026-09-16T12:10:00Z",
                "mix": {"wind": 70, "solar": 30, "nuclear": 0, "coal": 0},
            }
        ],
    }
    bronze = spark.createDataFrame(
        [
            ("2026-09-16T11:00:00Z", "https://example/mix?zone=FR", json.dumps(older)),
            ("2026-09-16T13:00:00Z", "https://example/mix?zone=FR", json.dumps(newer)),
            ("2026-09-16T12:30:00Z", "https://example/mix?zone=FR", json.dumps(newer)),
        ],
        ["extracted_at", "source_url", "payload"],
    )
    silver = transform_silver(bronze, contract)
    fact = silver["fact"]
    rows = fact.collect()
    assert len(rows) == 1
    assert rows[0].mix_wind == 70.0
    assert rows[0].mix_solar == 30.0
    assert _utc_clock(fact) == {"2026-09-16 13:00:00"}

    gold = transform_gold(silver, contract)
    product = next(iter(gold.values())).collect()[0]
    assert product.wind_pct == 70.0
    assert product.solar_pct == 30.0
    assert product.generation_mwh == 100.0


def test_silver_flows_dedupes_repeated_ingests(spark):
    contract = _contracts()["electricity_maps_electricity_flows"]
    payload = {
        "zone": "FR",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "data": [
            {
                "datetime": "2026-09-16T12:00:00Z",
                "updatedAt": "2026-09-16T12:05:00Z",
                "import": {"DE": 40.0},
                "export": {"IT": 20.0},
            }
        ],
    }
    bronze = spark.createDataFrame(
        [
            ("2026-09-16T12:00:00Z", "https://example/flows?zone=FR", json.dumps(payload)),
            ("2026-09-16T13:00:00Z", "https://example/flows?zone=FR", json.dumps(payload)),
        ],
        ["extracted_at", "source_url", "payload"],
    )
    silver = transform_silver(bronze, contract)
    fact = silver["fact"]
    rows = fact.collect()
    assert len(rows) == 2
    pairs = {(row.neighbor_zone, row.flow_direction) for row in rows}
    assert pairs == {("DE", "import"), ("IT", "export")}
    assert _utc_clock(fact) == {"2026-09-16 13:00:00"}
    assert "neighbor_zone_name" not in fact.columns

    gold = transform_gold(silver, contract)
    imports = {row.source_zone: row for row in gold["france_imports"].collect()}
    exports = {row.dest_zone: row for row in gold["france_exports"].collect()}
    assert imports["DE"].import_mwh == 40.0
    assert exports["IT"].export_mwh == 20.0


def test_silver_and_gold_france_flows(spark):
    contract = _contracts()["electricity_maps_electricity_flows"]
    payload = {
        "zone": "FR",
        "temporalGranularity": "hourly",
        "unit": "MW",
        "data": [
            {
                "datetime": "2026-09-16T12:00:00Z",
                "updatedAt": "2026-09-16T12:05:00Z",
                "import": {"DE": 40.0, "ES": 10.0},
                "export": {"DE": 5.0, "IT": 20.0},
            }
        ],
    }
    bronze = spark.createDataFrame(
        [("2026-09-16T13:00:00Z", "https://example/flows?zone=FR", json.dumps(payload))],
        ["extracted_at", "source_url", "payload"],
    )
    silver = transform_silver(bronze, contract)
    fact = silver["fact"]
    assert fact.count() == 4
    assert {(row.year, row.month, row.day) for row in fact.collect()} == {("2026", "09", "16")}
    gold = transform_gold(silver, contract)
    imports = {row.source_zone: row for row in gold["france_imports"].collect()}
    exports = {row.dest_zone: row for row in gold["france_exports"].collect()}
    assert imports["DE"].import_mwh == 40.0
    assert imports["DE"].net_import_mwh == 35.0
    assert exports["IT"].export_mwh == 20.0
    assert exports["DE"].net_import_mwh == 35.0


def test_silver_parquet_merge_keeps_existing_keys(spark, tmp_path):
    from electricity_etl.io import write_delta_and_parquet
    from electricity_etl.models import LayerContract, LayerTableContract

    layer = LayerContract(
        catalog="nxp",
        schema_name="silver",
        table="fact_electricity_mix",
        path="/Volumes/nxp/silver/fact_electricity_mix",
        write_disposition="merge",
        partition_by=["year", "month", "day"],
    )
    output = LayerTableContract(
        name="fact",
        table="fact_electricity_mix",
        path="/Volumes/nxp/silver/fact_electricity_mix",
        merge_keys=["zone", "datetime"],
    )
    first = spark.createDataFrame(
        [("AT", "2026-09-16T10:00:00", 1.0, "2026", "09", "16")],
        ["zone", "datetime", "mix_wind", "year", "month", "day"],
    )
    second = spark.createDataFrame(
        [
            ("AT", "2026-09-16T10:00:00", 9.0, "2026", "09", "16"),
            ("FR", "2026-09-16T11:00:00", 4.0, "2026", "09", "16"),
        ],
        ["zone", "datetime", "mix_wind", "year", "month", "day"],
    )
    write_delta_and_parquet(
        first, layer=layer, output=output, output_root=str(tmp_path), write_tables=False
    )
    write_delta_and_parquet(
        second, layer=layer, output=output, output_root=str(tmp_path), write_tables=False
    )
    rows = {
        (row.zone, row.mix_wind)
        for row in spark.read.parquet(
            str(tmp_path / "Volumes/nxp/silver/fact_electricity_mix")
        ).collect()
    }
    assert rows == {("AT", 9.0), ("FR", 4.0)}
