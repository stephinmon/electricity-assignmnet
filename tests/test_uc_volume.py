from datetime import datetime, timezone
from pathlib import Path

import pytest

from energy_platform.contracts.loader import load_contract
from energy_platform.ingestion.uc_volume import (
    _require_ident,
    is_past_retention,
    volume_path,
    write_jsonl,
)


def test_volume_path_uses_landing_zone():
    contract = load_contract(Path("contracts/electricity_maps.yaml"))
    assert volume_path(contract) == "/Volumes/nxp/landing/raw/electricity_mix_latest"


def test_write_jsonl(tmp_path: Path):
    path = write_jsonl([{"zone": "DE", "datetime": "2026-09-16T12:00:00Z"}], tmp_path / "a.jsonl")
    assert path.read_text().strip() == '{"zone": "DE", "datetime": "2026-09-16T12:00:00Z"}'


def test_landing_retention_is_seven_days():
    now = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)
    assert is_past_retention(
        "electricity_mix_latest_20260901T120000Z.jsonl", now=now, retention_days=7
    )
    assert not is_past_retention(
        "electricity_mix_latest_20260914T120000Z.jsonl", now=now, retention_days=7
    )


def test_reject_unsafe_identifier():
    with pytest.raises(ValueError):
        _require_ident("nxp; drop schema", "catalog")
