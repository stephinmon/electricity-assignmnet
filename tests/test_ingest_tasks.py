from pathlib import Path

import pytest

from energy_platform.ingestion.tasks import ingest_contract


def test_ingest_contract_runs_named_pipeline(monkeypatch):
    ran = {}

    def fake_run(contract, *, destination_name=None, pipeline_name=None):
        ran["name"] = contract.name
        ran["destination"] = destination_name
        return "loaded"

    monkeypatch.setattr("energy_platform.ingestion.tasks.run_bronze_pipeline", fake_run)
    ingest_contract("contracts/electricity_maps.yaml", destination="databricks")
    assert ran["name"] == "electricity_maps_electricity_mix"
    assert ran["destination"] == "databricks"


def test_ingest_contract_missing_file(tmp_path: Path):
    missing = tmp_path / "missing.yaml"
    with pytest.raises(FileNotFoundError, match="missing.yaml"):
        ingest_contract(str(missing))


def test_airflow_dags_define_two_pipelines():
    dags = Path("airflow/dags")
    ingest = (dags / "energy_ingest.py").read_text()
    etl = (dags / "electricity_etl.py").read_text()
    assert 'dag_id="energy_ingest"' in ingest
    assert "ingest_electricity_mix" in ingest
    assert "ingest_electricity_flows" in ingest
    assert "DockerOperator" in ingest
    assert "energy-platform-ingest" in ingest
    assert 'dag_id="electricity_etl"' in etl
    assert 'schedule="0 7 * * *"' in etl
    assert "DatabricksRunNowOperator" in etl
    assert "run_electricity_etl_job" in etl


def test_airflow_dags_parse_when_airflow_installed():
    pytest.importorskip("airflow")
    pytest.importorskip("airflow.providers.docker")
    pytest.importorskip("airflow.providers.databricks")
    from airflow.models import DagBag

    bag = DagBag(dag_folder="airflow/dags", include_examples=False)
    assert bag.import_errors == {}
    assert set(bag.dags) >= {"energy_ingest", "electricity_etl"}
    ingest = bag.get_dag("energy_ingest")
    assert set(ingest.task_ids) == {
        "ingest_electricity_mix",
        "ingest_electricity_flows",
    }
    assert bag.get_dag("electricity_etl").task_ids == ["run_electricity_etl_job"]
