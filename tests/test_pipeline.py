from energy_platform.contracts.loader import load_contract
from energy_platform.ingestion.pipeline import build_destination, dlt_logs_to_stdout


def test_build_duckdb_destination():
    contract = load_contract("contracts/electricity_maps.yaml")
    assert build_destination(contract, "duckdb") == "duckdb"


def test_dlt_logs_to_stdout(monkeypatch):
    import logging
    import sys

    handler = logging.StreamHandler(sys.stderr)
    log = logging.getLogger("dlt")
    log.addHandler(handler)
    try:
        dlt_logs_to_stdout()
        assert handler.stream is sys.stdout
    finally:
        log.removeHandler(handler)
