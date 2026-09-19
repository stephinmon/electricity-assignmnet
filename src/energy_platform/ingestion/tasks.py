from __future__ import annotations

import os
from pathlib import Path

from energy_platform.contracts.loader import load_contract
from energy_platform.ingestion.pipeline import run_bronze_pipeline


def _working_dir(contract_path: Path) -> Path:
    for root in [contract_path.parent, *contract_path.parents]:
        if (root / "contracts").is_dir() and (root / ".dlt").is_dir():
            return root
    return Path.cwd()


def ingest_contract(contract_path: str, destination: str = "databricks") -> None:
    """Run one YAML contract. Used by the Airflow ingest DAG and the CLI."""
    path = Path(contract_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Ingestion contract not found: {path}")
    os.chdir(_working_dir(path))
    contract = load_contract(path)
    print(f"Running contract {path}: {contract.name} destination={destination}")
    load_info = run_bronze_pipeline(contract, destination_name=destination)
    print(f"Ingest succeeded for {contract.name}")
    packages = getattr(load_info, "loads_ids", None)
    if packages:
        print(f"Load packages: {', '.join(str(item) for item in packages)}")
