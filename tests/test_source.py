from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from requests.exceptions import ConnectionError as RequestsConnectionError

from energy_platform.contracts.loader import load_contract
from energy_platform.ingestion.source import (
    ExtractedPayload,
    SourceApiError,
    build_source_url,
    extract_resource,
    get_json,
    to_bronze_rows,
)


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None, text=""):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeClient:
    def __init__(self, responses=None):
        self.calls = []
        self._responses = list(responses) if responses is not None else None

    def get(self, path, params=None, timeout=None):
        self.calls.append({"path": path, "params": params, "timeout": timeout})
        if self._responses is not None:
            item = self._responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return FakeResponse(
            {
                "zone": params["zone"],
                "temporalGranularity": "hourly",
                "unit": "MW",
                "data": [
                    {
                        "datetime": "2026-09-16T12:00:00Z",
                        "mix": {"wind": 0.4, "solar": 0.2},
                    }
                ],
            }
        )


def test_extract_resource_iterates_zones():
    contract = load_contract("contracts/electricity_maps.yaml")
    client = FakeClient()
    records = list(
        extract_resource(
            client,
            contract.resource,
            zone_values=["DE", "FR"],
            base_url=contract.source.base_url,
        )
    )
    assert [record.payload["zone"] for record in records] == ["DE", "FR"]
    assert records[0].source_url == (
        "https://api.electricitymaps.com/v4/electricity-mix/latest?zone=DE"
    )
    assert records[1].source_url.endswith("zone=FR")
    assert client.calls[0]["path"] == "electricity-mix/latest"
    assert client.calls[0]["params"]["zone"] == "DE"


def test_flows_contract_uses_flows_endpoint():
    contract = load_contract("contracts/electricity_flows.yaml")
    client = FakeClient()
    records = list(
        extract_resource(
            client,
            contract.resource,
            zone_values=["DE"],
            base_url=contract.source.base_url,
        )
    )
    assert client.calls[0]["path"] == "electricity-flows/latest"
    assert records[0].source_url == (
        "https://api.electricitymaps.com/v4/electricity-flows/latest?zone=DE"
    )


def test_extract_keeps_nested_payload():
    contract = load_contract("contracts/electricity_maps.yaml")
    client = FakeClient()
    records = list(
        extract_resource(
            client,
            contract.resource,
            zone_values=["DE"],
            base_url=contract.source.base_url,
        )
    )
    assert len(records) == 1
    assert "data" in records[0].payload
    assert records[0].payload["data"][0]["mix"]["wind"] == 0.4


def test_build_source_url_joins_base_path_and_query():
    url = build_source_url(
        "https://api.electricitymaps.com/v4",
        "electricity-mix/latest",
        {"zone": "DE"},
    )
    assert url == "https://api.electricitymaps.com/v4/electricity-mix/latest?zone=DE"


def test_bronze_rows_are_timestamp_url_and_payload():
    rows = to_bronze_rows(
        [
            ExtractedPayload(
                source_url="https://api.electricitymaps.com/v4/electricity-mix/latest?zone=DE",
                payload={"zone": "DE", "data": [{"mix": {"wind": 1}}]},
            )
        ],
        extracted_at=datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc),
    )
    assert list(rows[0].keys()) == [
        "extracted_at",
        "source_url",
        "payload",
        "year",
        "month",
        "day",
    ]
    assert rows[0]["extracted_at"] == "2026-09-16T12:00:00+00:00"
    assert rows[0]["year"] == "2026"
    assert rows[0]["month"] == "09"
    assert rows[0]["day"] == "16"
    assert (
        rows[0]["source_url"]
        == "https://api.electricitymaps.com/v4/electricity-mix/latest?zone=DE"
    )
    assert '"zone": "DE"' in rows[0]["payload"]
    assert '"data"' in rows[0]["payload"]


def test_get_json_retries_request_limit_then_succeeds():
    client = FakeClient(
        [
            FakeResponse(
                {"error": "request limit"},
                status_code=429,
                headers={"Retry-After": "2"},
            ),
            FakeResponse({"zone": "DE"}),
        ]
    )
    with patch("energy_platform.ingestion.source.time.sleep") as sleep:
        payload = get_json(
            client,
            "electricity-mix/latest",
            {"zone": "DE"},
            request_url="https://api.electricitymaps.com/v4/electricity-mix/latest?zone=DE",
        )
    assert payload == {"zone": "DE"}
    assert len(client.calls) == 2
    sleep.assert_called_once_with(2.0)


def test_get_json_raises_after_request_limit_retries():
    client = FakeClient(
        [FakeResponse({"error": "request limit"}, status_code=429) for _ in range(5)]
    )
    with patch("energy_platform.ingestion.source.time.sleep"):
        with pytest.raises(SourceApiError, match="request limit exceeded") as exc_info:
            get_json(client, "electricity-mix/latest", {"zone": "DE"})
    assert exc_info.value.status_code == 429
    assert exc_info.value.retryable is True
    assert len(client.calls) == 5


def test_get_json_does_not_retry_auth_errors():
    client = FakeClient([FakeResponse({"message": "invalid token"}, status_code=401)])
    with patch("energy_platform.ingestion.source.time.sleep") as sleep:
        with pytest.raises(SourceApiError, match="auth failed") as exc_info:
            get_json(client, "electricity-mix/latest", {"zone": "DE"})
    assert exc_info.value.status_code == 401
    assert exc_info.value.retryable is False
    sleep.assert_not_called()
    assert len(client.calls) == 1


def test_get_json_retries_connection_errors():
    client = FakeClient(
        [RequestsConnectionError("reset"), FakeResponse({"zone": "DE"})]
    )
    with patch("energy_platform.ingestion.source.time.sleep") as sleep:
        payload = get_json(client, "electricity-mix/latest", {"zone": "DE"})
    assert payload == {"zone": "DE"}
    sleep.assert_called_once()
