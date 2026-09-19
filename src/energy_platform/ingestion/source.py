from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, NamedTuple
from urllib.parse import urlencode, urljoin

import dlt
import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from energy_platform.contracts.models import IngestionContract, ResourceContract

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 60
MAX_REQUEST_ATTEMPTS = 5
RETRY_BACKOFF_SECONDS = 1.0
MAX_RETRY_BACKOFF_SECONDS = 30.0
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class ExtractedPayload(NamedTuple):
    source_url: str
    payload: dict[str, Any]


class SourceApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.retryable = retryable


def resolve_secret(secret_key: str) -> str:
    try:
        value = dlt.secrets[secret_key]
    except Exception as exc:
        raise RuntimeError(
            f"Missing secret '{secret_key}'. Add it to .dlt/secrets.toml "
            "or set the matching SOURCES__ELECTRICITY_MAPS__API_KEY environment variable."
        ) from exc
    if not value:
        raise RuntimeError(f"Secret '{secret_key}' is empty.")
    return str(value)


def build_rest_client(contract: IngestionContract, api_key: str) -> RESTClient:
    auth = contract.source.auth
    paginator = SinglePagePaginator() if contract.resource.paginator == "single_page" else None
    session = requests.Session()
    session.headers["User-Agent"] = "energy-platform"
    return RESTClient(
        base_url=contract.source.base_url,
        auth=APIKeyAuth(name=auth.name, api_key=api_key, location=auth.location),
        paginator=paginator,
        session=session,
    )


def _static_params(resource: ResourceContract) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for key, value in resource.params.items():
        if hasattr(value, "type") and getattr(value, "type") == "values":
            continue
        if isinstance(value, dict) and value.get("type") == "values":
            continue
        params[key] = value
    return params


def build_source_url(base_url: str, path: str, params: dict[str, Any]) -> str:
    joined = urljoin(base_url if base_url.endswith("/") else f"{base_url}/", path.lstrip("/"))
    query = urlencode(params)
    return f"{joined}?{query}" if query else joined


def _response_detail(response: Any) -> str:
    payload = None
    json_fn = getattr(response, "json", None)
    if callable(json_fn):
        try:
            payload = json_fn()
        except Exception:
            payload = None
    if isinstance(payload, dict):
        for key in ("message", "error", "error_message", "detail"):
            value = payload.get(key)
            if value:
                return str(value)
        return json.dumps(payload, default=str)[:300]
    text = getattr(response, "text", "") or ""
    return str(text).strip()[:300]


def _retry_after_seconds(response: Any) -> float | None:
    headers = getattr(response, "headers", None) or {}
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        seconds = float(str(raw).strip())
    except ValueError:
        return None
    if seconds <= 0:
        return None
    return min(seconds, MAX_RETRY_BACKOFF_SECONDS)


def _backoff_seconds(attempt: int, response: Any | None = None) -> float:
    retry_after = _retry_after_seconds(response) if response is not None else None
    if retry_after is not None:
        return retry_after
    return min(MAX_RETRY_BACKOFF_SECONDS, RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))


def _error_message(status: int, url: str, detail: str) -> str:
    suffix = f": {detail}" if detail else ""
    if status in {401, 403}:
        return f"Electricity Maps auth failed ({status}) for {url}{suffix}"
    if status == 429:
        return f"Electricity Maps request limit exceeded for {url}. Retries exhausted{suffix}"
    if status >= 500:
        return f"Electricity Maps server error {status} for {url}{suffix}"
    return f"Electricity Maps request failed with HTTP {status} for {url}{suffix}"


def get_json(
    client: Any,
    path: str,
    params: dict[str, Any],
    *,
    request_url: str | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    max_attempts: int = MAX_REQUEST_ATTEMPTS,
) -> dict[str, Any]:
    url = request_url or path
    transient = (RequestsConnectionError, RequestsTimeout)
    last_response: Any = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(path, params=params, timeout=timeout)
        except transient as exc:
            if attempt == max_attempts:
                raise SourceApiError(
                    f"Electricity Maps request failed for {url}: {exc}",
                    url=url,
                    retryable=True,
                ) from exc
            wait = _backoff_seconds(attempt)
            logger.warning(
                "Transient error for %s (attempt %s/%s); retrying in %.1fs: %s",
                url,
                attempt,
                max_attempts,
                wait,
                exc,
            )
            time.sleep(wait)
            continue

        last_response = response
        status = int(getattr(response, "status_code", 200) or 200)
        if 200 <= status < 300:
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError(f"Expected object payload from {url}, got {type(payload)}")
            return payload

        detail = _response_detail(response)
        retryable = status in RETRYABLE_STATUS_CODES
        if retryable and attempt < max_attempts:
            wait = _backoff_seconds(attempt, response)
            logger.warning(
                "HTTP %s for %s (attempt %s/%s); retrying in %.1fs%s",
                status,
                url,
                attempt,
                max_attempts,
                wait,
                f" ({detail})" if detail else "",
            )
            time.sleep(wait)
            continue

        raise SourceApiError(
            _error_message(status, url, detail),
            status_code=status,
            url=url,
            retryable=retryable,
        )

    status = int(getattr(last_response, "status_code", 0) or 0)
    detail = _response_detail(last_response) if last_response is not None else ""
    raise SourceApiError(
        _error_message(status, url, detail),
        status_code=status or None,
        url=url,
        retryable=True,
    )


def extract_resource(
    client: RESTClient,
    resource: ResourceContract,
    *,
    zone_values: list[str],
    base_url: str,
) -> Iterator[ExtractedPayload]:
    """Yield the raw API object per zone plus the request URL. Bronze does not expand JSON."""
    static_params = _static_params(resource)
    path = resource.path.lstrip("/")
    for zone in zone_values:
        params = {**static_params, "zone": zone}
        source_url = build_source_url(base_url, path, params)
        payload = get_json(client, path, params, request_url=source_url)
        yield ExtractedPayload(source_url=source_url, payload=payload)


def to_bronze_rows(
    extracted: list[ExtractedPayload], extracted_at: datetime | None = None
) -> list[dict[str, Any]]:
    ingested_at = extracted_at or datetime.now(timezone.utc)
    if ingested_at.tzinfo is None:
        ingested_at = ingested_at.replace(tzinfo=timezone.utc)
    ingested_at = ingested_at.astimezone(timezone.utc)
    return [
        {
            "extracted_at": ingested_at.isoformat(),
            "source_url": item.source_url,
            "payload": json.dumps(item.payload, default=str),
            "year": ingested_at.strftime("%Y"),
            "month": ingested_at.strftime("%m"),
            "day": ingested_at.strftime("%d"),
        }
        for item in extracted
    ]


def collect_raw_payloads(
    contract: IngestionContract, api_key: str | None = None
) -> list[ExtractedPayload]:
    token = api_key or resolve_secret(contract.source.auth.secret_key)
    client = build_rest_client(contract, token)
    return list(
        extract_resource(
            client,
            contract.resource,
            zone_values=contract.iterable_param("zone"),
            base_url=contract.source.base_url,
        )
    )


def electricity_maps_source(
    contract: IngestionContract,
    api_key: str | None = None,
    records: list[dict[str, Any]] | None = None,
):
    """Build a dlt source: bronze rows are extracted_at, source_url, and payload."""
    write_disposition = contract.bronze.write_disposition
    cached = records

    @dlt.source(name=contract.source.name)
    def _source():
        @dlt.resource(
            name=contract.bronze.table,
            write_disposition=write_disposition,
            columns=contract.dlt_columns(),
            max_table_nesting=0,
        )
        def bronze_rows() -> Iterator[dict[str, Any]]:
            rows = cached
            if rows is None:
                rows = to_bronze_rows(collect_raw_payloads(contract, api_key))
            yield from rows

        return bronze_rows

    return _source()
