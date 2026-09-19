from __future__ import annotations

import argparse
import sys
from pathlib import Path

from energy_platform.contracts.loader import load_contract
from energy_platform.ingestion.pipeline import run_bronze_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run contract-based dlt bronze ingestion into DuckDB or Databricks Unity Catalog."
        ),
    )
    parser.add_argument(
        "--contract",
        action="append",
        dest="contracts",
        help="Path to a YAML ingestion contract. Repeat to run multiple sources.",
    )
    parser.add_argument(
        "--all-contracts",
        action="store_true",
        help="Run every YAML contract in the contracts/ directory.",
    )
    parser.add_argument(
        "--destination",
        choices=["databricks", "duckdb"],
        default=None,
        help="Override the bronze table destination.",
    )
    parser.add_argument(
        "--pipeline-name",
        default=None,
        help="Optional dlt pipeline name override (single contract only).",
    )
    return parser


def contract_paths(args: argparse.Namespace) -> list[Path]:
    if args.all_contracts:
        return sorted(Path("contracts").glob("*.yaml"))
    if args.contracts:
        return [Path(path) for path in args.contracts]
    return [Path("contracts/electricity_maps.yaml")]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paths = contract_paths(args)
    if not paths:
        raise FileNotFoundError("No ingestion contracts found.")
    if args.pipeline_name and len(paths) > 1:
        raise ValueError("--pipeline-name can only be used with a single --contract.")
    for path in paths:
        contract = load_contract(path)
        print(f"Running contract {path}: {contract.name}")
        load_info = run_bronze_pipeline(
            contract,
            destination_name=args.destination,
            pipeline_name=args.pipeline_name,
        )
        print(load_info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
