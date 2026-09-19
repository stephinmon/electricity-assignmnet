from __future__ import annotations

import argparse
import sys
from pathlib import Path

from electricity_etl.databricks import uses_unity_catalog
from electricity_etl.runner import run_etl, run_table
from electricity_etl.spark import get_spark


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PySpark silver/gold ETL for Electricity Maps bronze tables.",
    )
    parser.add_argument(
        "--layer",
        choices=["silver", "gold", "all"],
        default="all",
        help="Which medallion layer to build.",
    )
    parser.add_argument(
        "--table",
        default=None,
        help=(
            "Build a single silver or gold table (Databricks job task). "
            "Example: dim_zone, fact_electricity_mix, electricity_mix_daily_relative."
        ),
    )
    parser.add_argument(
        "--contract",
        action="append",
        dest="contracts",
        help="Path to a YAML contract. Repeat to run a subset.",
    )
    parser.add_argument(
        "--contracts-dir",
        default=None,
        help="Directory of YAML contracts, or a single .yaml file. Defaults to repo contracts/.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Local root for parquet files. Omit on Databricks to use /Volumes paths.",
    )
    parser.add_argument(
        "--no-tables",
        action="store_true",
        help="Write parquet only; skip Delta saveAsTable (useful for local runs).",
    )
    parser.add_argument(
        "--bronze-path",
        action="append",
        default=None,
        metavar="NAME=PATH",
        help="Override bronze input for a contract name, e.g. mix=/tmp/mix.json",
    )
    return parser


def _parse_bronze_paths(values: list[str] | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in values or []:
        if "=" not in item:
            raise ValueError(f"--bronze-path must be NAME=PATH, got {item}")
        name, path = item.split("=", 1)
        mapping[name] = path
    return mapping


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spark = get_spark()
    output_root = args.output_root
    if output_root is None and not uses_unity_catalog(spark):
        output_root = str(Path.cwd() / "output")
        print(f"Local Spark session: writing parquet under {output_root}")
    common = {
        "contracts_dir": args.contracts_dir,
        "contract_paths": [Path(path) for path in args.contracts] if args.contracts else None,
        "bronze_paths": _parse_bronze_paths(args.bronze_path),
        "output_root": output_root,
        "write_tables": not args.no_tables,
    }
    if args.table:
        run_table(spark, args.table, **common)
        return 0
    run_etl(spark, layer=args.layer, **common)
    return 0


if __name__ == "__main__":
    sys.exit(main())
