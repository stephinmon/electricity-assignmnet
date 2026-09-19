from pathlib import Path

from energy_platform.contracts.loader import load_contract


def test_load_electricity_maps_contract():
    contract = load_contract(Path("contracts/electricity_maps.yaml"))
    assert contract.name == "electricity_maps_electricity_mix"
    assert contract.source.auth.name == "auth-token"
    assert contract.landing_zone.catalog == "nxp"
    assert contract.landing_zone.schema_name == "landing"
    assert contract.landing_zone.volume == "raw"
    assert contract.landing_zone.retention_days == 7
    assert contract.bronze.catalog == "nxp"
    assert contract.bronze.schema_name == "bronze"
    assert contract.bronze.table == "electricity_mix_latest"
    assert list(contract.dlt_columns()) == [
        "extracted_at",
        "source_url",
        "payload",
        "year",
        "month",
        "day",
    ]
    assert contract.bronze.partition_by == ["year", "month", "day"]
    assert "DE" in contract.iterable_param("zone")
    assert contract.layers is not None
    assert contract.layers.silver is not None
    assert contract.layers.silver.table == "fact_electricity_mix"
    assert [item.name for item in contract.layers.silver.outputs()] == [
        "fact",
        "dim_zone",
        "dim_date",
    ]
    assert contract.layers.silver.partition_by == ["year", "month", "day"]
    assert contract.keys is not None
    assert contract.keys.business_keys == ["zone", "datetime"]
    assert contract.layers.gold is not None
    assert contract.layers.gold.table == "electricity_mix_daily_relative"
    assert contract.layers.gold.partition_by == ["dt"]


def test_load_electricity_flows_contract():
    contract = load_contract(Path("contracts/electricity_flows.yaml"))
    assert contract.name == "electricity_maps_electricity_flows"
    assert contract.resource.path == "electricity-flows/latest"
    assert contract.landing_zone.prefix == "electricity_flows_latest"
    assert contract.landing_zone.retention_days == 7
    assert contract.bronze.table == "electricity_flows_latest"
    assert list(contract.dlt_columns()) == [
        "extracted_at",
        "source_url",
        "payload",
        "year",
        "month",
        "day",
    ]
    assert contract.bronze.partition_by == ["year", "month", "day"]
    assert "DE" in contract.iterable_param("zone")
    assert contract.keys is not None
    assert "neighbor_zone" in contract.keys.business_keys
    assert contract.layers is not None
    assert contract.layers.gold is not None
    assert [item.table for item in contract.layers.gold.outputs()] == [
        "electricity_flows_fr_imports_daily",
        "electricity_flows_fr_exports_daily",
    ]
    assert contract.layers.silver is not None
    assert contract.layers.silver.table == "fact_electricity_flows"
    assert [item.name for item in contract.layers.silver.outputs()] == [
        "fact",
        "dim_zone",
        "dim_date",
    ]
    assert contract.layers.silver.partition_by == ["year", "month", "day"]
    assert contract.layers.gold.partition_by == ["dt"]
