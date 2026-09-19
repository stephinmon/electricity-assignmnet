from __future__ import annotations

import os
from pathlib import Path

import yaml

from electricity_etl.models import EtlContract


def find_contracts_dir(explicit: str | Path | None = None) -> Path:
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise FileNotFoundError(f"Contracts directory not found: {path}")
        return path
    env = os.environ.get("ELECTRICITY_ETL_CONTRACTS")
    if env:
        return Path(env)

    search_roots = [Path.cwd(), *Path.cwd().parents, *Path(__file__).resolve().parents]
    seen: set[Path] = set()
    for root in search_roots:
        candidate = root / "contracts"
        if candidate in seen:
            continue
        seen.add(candidate)
        if (candidate / "electricity_maps.yaml").exists():
            return candidate
    raise FileNotFoundError(
        "Cannot find contracts/. Pass --contracts-dir or set ELECTRICITY_ETL_CONTRACTS."
    )


def load_contract(path: str | Path) -> EtlContract:
    payload = yaml.safe_load(Path(path).read_text())
    return EtlContract.model_validate(payload)


def load_contracts(
    *,
    contracts_dir: str | Path | None = None,
    contract_paths: list[str | Path] | None = None,
) -> list[EtlContract]:
    paths = [Path(path) for path in contract_paths or []]
    if contracts_dir:
        directory = Path(contracts_dir)
        if directory.is_file() and directory.suffix.lower() in {".yaml", ".yml"}:
            paths.append(directory)
        else:
            directory = find_contracts_dir(contracts_dir)
            paths.extend(sorted(directory.glob("*.yaml")))
    elif not paths:
        paths.extend(sorted(find_contracts_dir().glob("*.yaml")))
    if not paths:
        raise FileNotFoundError("No YAML contracts found.")
    return [load_contract(path) for path in paths]
