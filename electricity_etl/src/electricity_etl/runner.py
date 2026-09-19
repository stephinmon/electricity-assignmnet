from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from electricity_etl.contract import load_contracts
from electricity_etl.gold import transform_gold
from electricity_etl.io import read_bronze, read_silver, write_delta_and_parquet
from electricity_etl.models import EtlContract, LayerTableContract
from electricity_etl.silver import (
    DIM_DATE,
    DIM_ZONE,
    FACT_NAME,
    SHARED_DIMS,
    transform_silver,
    union_dim_dates,
)
from electricity_etl.silver.dim_date import transform as build_dim_date
from electricity_etl.silver.dim_zone import transform as build_dim_zone


def _output_key(output) -> str:
    return output.name or output.table


def _write_layer_output(frame, *, layer, output, output_root, write_tables) -> None:
    write_delta_and_parquet(
        frame,
        layer=layer,
        output=output,
        output_root=output_root,
        write_tables=write_tables,
    )


def run_silver(
    spark: SparkSession,
    contract: EtlContract,
    *,
    bronze_path: str | None = None,
    output_root: str | None = None,
    write_tables: bool = True,
    write_shared_dims: bool = False,
):
    if contract.layers.silver is None:
        raise ValueError(f"{contract.name} has no silver layer")
    bronze = read_bronze(spark, contract, bronze_path=bronze_path)
    products = transform_silver(bronze, contract)
    for output in contract.layers.silver.outputs():
        key = _output_key(output)
        if key in SHARED_DIMS and not write_shared_dims:
            continue
        frame = products.get(key) or products.get("fact")
        if frame is None:
            raise KeyError(f"Silver product '{key}' was not produced for {contract.name}")
        _write_layer_output(
            frame,
            layer=contract.layers.silver,
            output=output,
            output_root=output_root,
            write_tables=write_tables,
        )
    return products


def _write_shared_dimensions(contracts, all_products, *, output_root, write_tables) -> None:
    dim_frames = [item.get(DIM_ZONE) for item in all_products if item.get(DIM_ZONE) is not None]
    date_frames = [item.get(DIM_DATE) for item in all_products if item.get(DIM_DATE) is not None]
    if not dim_frames or not date_frames:
        return
    shared = {
        DIM_ZONE: dim_frames[0],
        DIM_DATE: union_dim_dates(date_frames),
    }
    written: set[str] = set()
    for contract in contracts:
        if contract.layers.silver is None:
            continue
        for output in contract.layers.silver.outputs():
            key = _output_key(output)
            if key not in shared or key in written:
                continue
            _write_layer_output(
                shared[key],
                layer=contract.layers.silver,
                output=output,
                output_root=output_root,
                write_tables=write_tables,
            )
            written.add(key)


def run_gold(
    spark: SparkSession,
    contract: EtlContract,
    *,
    silver=None,
    output_root: str | None = None,
    write_tables: bool = True,
):
    if contract.layers.gold is None:
        raise ValueError(f"{contract.name} has no gold layer")
    if silver is None:
        silver = read_silver(spark, contract, output_root=output_root)
    products = transform_gold(silver, contract)
    written = {}
    for output in contract.layers.gold.outputs():
        key = output.name or output.table
        frame = products.get(key) or products.get(output.table)
        if frame is None:
            raise KeyError(f"Gold product '{key}' was not produced for {contract.name}")
        _write_layer_output(
            frame,
            layer=contract.layers.gold,
            output=output,
            output_root=output_root,
            write_tables=write_tables,
        )
        written[key] = frame
    return written


def run_etl(
    spark: SparkSession,
    *,
    layer: str = "all",
    contracts_dir: str | Path | None = None,
    contract_paths: list[str | Path] | None = None,
    bronze_paths: dict[str, str] | None = None,
    output_root: str | None = None,
    write_tables: bool = True,
) -> None:
    if layer not in {"silver", "gold", "all"}:
        raise ValueError("layer must be silver, gold, or all")
    contracts = load_contracts(contracts_dir=contracts_dir, contract_paths=contract_paths)
    bronze_paths = bronze_paths or {}
    silver_by_contract: dict[str, dict] = {}
    if layer in {"silver", "all"}:
        for contract in contracts:
            print(f"ETL {contract.name} layer=silver")
            silver_by_contract[contract.name] = run_silver(
                spark,
                contract,
                bronze_path=bronze_paths.get(contract.name),
                output_root=output_root,
                write_tables=write_tables,
            )
        _write_shared_dimensions(
            contracts,
            list(silver_by_contract.values()),
            output_root=output_root,
            write_tables=write_tables,
        )
    if layer in {"gold", "all"}:
        for contract in contracts:
            print(f"ETL {contract.name} layer=gold")
            run_gold(
                spark,
                contract,
                silver=silver_by_contract.get(contract.name),
                output_root=output_root,
                write_tables=write_tables,
            )


def find_layer_output(
    contracts: list[EtlContract], table: str
) -> tuple[EtlContract, str, LayerTableContract]:
    for contract in contracts:
        for kind, layer in (("silver", contract.layers.silver), ("gold", contract.layers.gold)):
            if layer is None:
                continue
            for output in layer.outputs():
                if output.table == table or output.name == table:
                    return contract, kind, output
    raise ValueError(f"No contract output named {table!r}")


def run_table(
    spark: SparkSession,
    table: str,
    *,
    contracts_dir: str | Path | None = None,
    contract_paths: list[str | Path] | None = None,
    bronze_paths: dict[str, str] | None = None,
    output_root: str | None = None,
    write_tables: bool = True,
) -> None:
    contracts = load_contracts(contracts_dir=contracts_dir, contract_paths=contract_paths)
    contract, kind, output = find_layer_output(contracts, table)
    layer = contract.layers.silver if kind == "silver" else contract.layers.gold
    if layer is None:
        raise ValueError(f"{contract.name} has no {kind} layer")
    key = output.name or output.table
    bronze_paths = bronze_paths or {}
    print(f"ETL table={output.table} layer={kind} contract={contract.name}")

    if kind == "silver" and key == DIM_ZONE:
        frame = build_dim_zone(spark)
        _write_layer_output(
            frame,
            layer=layer,
            output=output,
            output_root=output_root,
            write_tables=write_tables,
        )
        return

    if kind == "silver" and key == DIM_DATE:
        date_frames = []
        for item in contracts:
            if item.layers.silver is None:
                continue
            products = read_silver(spark, item, output_root=output_root, include={FACT_NAME})
            fact = products.get(FACT_NAME)
            if fact is not None and "dt" in fact.columns:
                date_frames.append(build_dim_date(fact))
        if not date_frames:
            raise RuntimeError("dim_date needs silver fact tables; run fact tasks first")
        _write_layer_output(
            union_dim_dates(date_frames),
            layer=layer,
            output=output,
            output_root=output_root,
            write_tables=write_tables,
        )
        return

    if kind == "silver":
        bronze = read_bronze(spark, contract, bronze_path=bronze_paths.get(contract.name))
        products = transform_silver(bronze, contract)
        frame = products.get(key) or products.get(FACT_NAME)
        if frame is None:
            raise KeyError(f"Silver product '{key}' was not produced for {contract.name}")
        _write_layer_output(
            frame,
            layer=layer,
            output=output,
            output_root=output_root,
            write_tables=write_tables,
        )
        return

    silver = read_silver(
        spark, contract, output_root=output_root, include={FACT_NAME, DIM_ZONE}
    )
    if FACT_NAME not in silver:
        raise RuntimeError(f"Gold {output.table} needs silver fact for {contract.name}")
    products = transform_gold(silver, contract)
    frame = products.get(key) or products.get(output.table)
    if frame is None:
        raise KeyError(f"Gold product '{key}' was not produced for {contract.name}")
    _write_layer_output(
        frame,
        layer=layer,
        output=output,
        output_root=output_root,
        write_tables=write_tables,
    )
