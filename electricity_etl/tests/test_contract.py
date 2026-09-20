from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from electricity_etl.cli import build_parser
from electricity_etl.contract import load_contracts
from electricity_etl.databricks import parse_volume_path, publish_parquet_to_uc
from electricity_etl.io import merge_predicate
from electricity_etl.runner import find_layer_output
from electricity_etl.silver import SILVER_TRANSFORMS


def test_load_repo_contracts():
    contracts = {item.name: item for item in load_contracts()}
    assert "electricity_maps_electricity_mix" in contracts
    mix = contracts["electricity_maps_electricity_mix"]
    assert mix.bronze.table == "electricity_mix_latest"
    assert mix.layers.silver.table == "fact_electricity_mix"
    assert [item.name for item in mix.layers.silver.outputs()] == [
        "fact",
        "dim_zone",
        "dim_date",
    ]
    assert mix.layers.silver.outputs()[0].table == "fact_electricity_mix"
    assert mix.layers.silver.partition_by == ["year", "month", "day"]
    assert mix.layers.silver.write_disposition == "merge"
    assert mix.layers.silver.outputs()[0].merge_keys == ["zone", "datetime"]
    assert mix.layers.gold.table == "electricity_mix_daily_relative"
    assert mix.layers.gold.partition_by == ["dt"]
    assert mix.business_keys == ["zone", "datetime"]

    flows = contracts["electricity_maps_electricity_flows"]
    assert flows.layers.silver.write_disposition == "merge"
    assert flows.layers.silver.outputs()[0].merge_keys == [
        "zone",
        "datetime",
        "neighbor_zone",
        "flow_direction",
    ]
    assert [item.name for item in flows.layers.gold.outputs()] == [
        "france_imports",
        "france_exports",
    ]
    assert set(SILVER_TRANSFORMS) == set(contracts)


def test_load_single_yaml_as_contracts_dir():
    path = Path(__file__).resolve().parents[2] / "contracts" / "electricity_flows.yaml"
    contracts = load_contracts(contracts_dir=path)
    assert [item.name for item in contracts] == ["electricity_maps_electricity_flows"]


def test_parse_volume_path():
    assert parse_volume_path("/Volumes/nxp/silver/electricity_mix") == (
        "nxp",
        "silver",
        "electricity_mix",
    )


def test_publish_replaces_volume_before_put(tmp_path):
    part = tmp_path / "part-0000.parquet"
    part.write_bytes(b"parquet")
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
    with patch("electricity_etl.databricks.warehouse_connect", return_value=conn):
        publish_parquet_to_uc(
            tmp_path,
            volume_path="/Volumes/nxp/silver/electricity_mix",
            table="nxp.silver.electricity_mix",
            partition_by=["dt"],
        )
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "DROP TABLE IF EXISTS nxp.silver.electricity_mix" in statements
    assert "DROP VOLUME IF EXISTS nxp.silver.electricity_mix" in statements
    assert any(item.startswith("LIST ") for item in statements)
    assert any(item.startswith("PUT ") for item in statements)
    assert any("CREATE OR REPLACE TABLE nxp.silver.electricity_mix" in item for item in statements)


def test_merge_parquet_does_not_drop_target(tmp_path):
    part = tmp_path / "part-0000.parquet"
    part.write_bytes(b"parquet")
    cursor = MagicMock()
    cursor.fetchall.return_value = [("col",)]
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
    with patch("electricity_etl.databricks.warehouse_connect", return_value=conn):
        from electricity_etl.databricks import merge_parquet_to_uc

        merge_parquet_to_uc(
            tmp_path,
            volume_path="/Volumes/nxp/silver/fact_electricity_mix",
            table="nxp.silver.fact_electricity_mix",
            partition_by=["year", "month", "day"],
            merge_keys=["zone", "datetime"],
        )
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "DROP TABLE IF EXISTS nxp.silver.fact_electricity_mix" not in statements
    assert any(item.startswith("MERGE INTO nxp.silver.fact_electricity_mix") for item in statements)
    assert any("WHEN MATCHED THEN UPDATE SET *" in item for item in statements)


def test_find_layer_output_maps_star_tables():
    contracts = load_contracts()
    _, kind, output = find_layer_output(contracts, "dim_zone")
    assert kind == "silver"
    assert output.table == "dim_zone"
    _, kind, output = find_layer_output(contracts, "fact_electricity_mix")
    assert kind == "silver"
    assert output.table == "fact_electricity_mix"
    _, kind, output = find_layer_output(contracts, "electricity_flows_fr_imports_daily")
    assert kind == "gold"
    assert output.name == "france_imports"


def test_find_layer_output_unknown_table():
    with pytest.raises(ValueError, match="No contract output"):
        find_layer_output(load_contracts(), "does_not_exist")


def test_merge_predicate_quotes_keys():
    assert (
        merge_predicate(["zone", "datetime"])
        == "t.`zone` <=> s.`zone` AND t.`datetime` <=> s.`datetime`"
    )


def test_cli_table_flag():
    args = build_parser().parse_args(["--table", "dim_zone", "--contracts-dir", "contracts"])
    assert args.table == "dim_zone"
