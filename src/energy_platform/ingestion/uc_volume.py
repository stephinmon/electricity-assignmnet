from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dlt
from databricks import sql

from energy_platform.contracts.models import IngestionContract

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LANDING_STAMP = re.compile(r"_(\d{8}T\d{6}Z)\.jsonl(?:\.gz)?$", re.IGNORECASE)


def _require_ident(value: str, field: str) -> str:
    if not _SAFE_IDENT.match(value):
        raise ValueError(f"Unsafe Unity Catalog identifier for {field}: {value!r}")
    return value


def _hostname() -> str:
    host = str(dlt.secrets["destination.databricks.credentials.server_hostname"])
    return host.replace("https://", "").replace("http://", "").split("/")[0]


def _connect(catalog: str, staging_allowed_local_path: str | Path):
    return sql.connect(
        server_hostname=_hostname(),
        http_path=str(dlt.secrets["destination.databricks.credentials.http_path"]),
        access_token=str(dlt.secrets["destination.databricks.credentials.access_token"]),
        catalog=catalog,
        staging_allowed_local_path=str(Path(staging_allowed_local_path).resolve()),
    )


def volume_path(contract: IngestionContract) -> str:
    zone = contract.landing_zone
    catalog = _require_ident(zone.catalog, "catalog")
    schema = _require_ident(zone.schema_name, "schema")
    volume = _require_ident(zone.volume, "volume")
    prefix = _require_ident(contract.landing_prefix(), "prefix")
    return f"/Volumes/{catalog}/{schema}/{volume}/{prefix}"


def write_jsonl(records: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str) + "\n")
    return path


def landing_filename(contract: IngestionContract, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"{contract.landing_prefix()}_{stamp}.jsonl"


def parse_landing_timestamp(filename: str) -> datetime | None:
    match = _LANDING_STAMP.search(Path(filename).name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def is_past_retention(filename: str, *, now: datetime, retention_days: int) -> bool:
    timestamp = parse_landing_timestamp(filename)
    if timestamp is None:
        return False
    return timestamp < now - timedelta(days=retention_days)


def publish_jsonl_to_volume(contract: IngestionContract, local_root: Path) -> list[str]:
    """Upload JSONL to the landing-zone volume and delete files older than retention."""
    zone = contract.landing_zone
    catalog = _require_ident(zone.catalog, "catalog")
    schema = _require_ident(zone.schema_name, "schema")
    volume = _require_ident(zone.volume, "volume")
    files = sorted(
        path for path in local_root.rglob("*") if path.is_file() and ".jsonl" in path.name
    )
    if not files:
        raise FileNotFoundError(f"No JSONL files to publish under {local_root}")

    uploaded: list[str] = []
    dest_root = volume_path(contract)
    with _connect(catalog, local_root) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            cur.execute(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
            for jsonl in files:
                dest = f"{dest_root}/{jsonl.name}"
                cur.execute(f"PUT '{jsonl.resolve().as_posix()}' INTO '{dest}' OVERWRITE")
                uploaded.append(dest)
            removed = _purge_expired_files(cur, dest_root, zone.retention_days)
    _purge_local_expired(local_root, zone.retention_days)
    if removed:
        print(f"Removed {len(removed)} landing-zone file(s) older than {zone.retention_days} days")
        for path in removed:
            print(f"  removed {path}")
    return uploaded


def _purge_expired_files(cur, dest_root: str, retention_days: int) -> list[str]:
    now = datetime.now(timezone.utc)
    try:
        cur.execute(f"LIST '{dest_root}/'")
        rows = cur.fetchall() or []
    except Exception as exc:
        print(f"Could not list landing zone {dest_root}: {exc}")
        return []
    description = [col[0].lower() for col in (cur.description or [])]
    removed: list[str] = []
    for row in rows:
        named = dict(zip(description, row, strict=False)) if description else {}
        path = str(named.get("path") or named.get("name") or row[0])
        if ".jsonl" not in Path(path).name:
            continue
        if is_past_retention(path, now=now, retention_days=retention_days):
            try:
                cur.execute(f"REMOVE '{path}'")
                removed.append(path)
            except Exception as exc:
                print(f"Could not remove expired landing file {path}: {exc}")
    return removed


def _purge_local_expired(local_root: Path, retention_days: int) -> None:
    now = datetime.now(timezone.utc)
    for path in local_root.rglob("*"):
        if path.is_file() and is_past_retention(path.name, now=now, retention_days=retention_days):
            path.unlink(missing_ok=True)
