from __future__ import annotations

from pathlib import Path

import yaml

from energy_platform.contracts.models import IngestionContract


def _default_candidates() -> list[Path]:
    return [
        Path("contracts/electricity_maps.yaml"),
        Path(__file__).resolve().parents[3] / "contracts" / "electricity_maps.yaml",
    ]


def load_contract(path: str | Path | None = None) -> IngestionContract:
    candidates = [Path(path)] if path else _default_candidates()
    for contract_path in candidates:
        if contract_path.exists():
            payload = yaml.safe_load(contract_path.read_text())
            return IngestionContract.model_validate(payload)
    searched = ", ".join(str(item) for item in candidates)
    raise FileNotFoundError(f"Ingestion contract not found. Looked in: {searched}")
