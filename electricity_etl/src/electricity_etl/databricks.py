from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class DatabricksCredentials:
    server_hostname: str
    http_path: str
    access_token: str


def uses_unity_catalog(spark) -> bool:
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    master = ""
    try:
        master = str(spark.conf.get("spark.master", ""))
    except Exception:
        pass
    return not master.startswith("local")


def _require_ident(value: str, field: str) -> str:
    if not _SAFE_IDENT.match(value):
        raise ValueError(f"Unsafe Unity Catalog identifier for {field}: {value!r}")
    return value


def parse_volume_path(path: str) -> tuple[str, str, str]:
    parts = [item for item in path.strip("/").split("/") if item]
    if len(parts) < 4 or parts[0] != "Volumes":
        raise ValueError(f"Expected /Volumes/<catalog>/<schema>/<volume>, got {path}")
    return (
        _require_ident(parts[1], "catalog"),
        _require_ident(parts[2], "schema"),
        _require_ident(parts[3], "volume"),
    )


def _normalize_host(host: str) -> str:
    return host.replace("https://", "").replace("http://", "").split("/")[0]


def _from_env() -> DatabricksCredentials | None:
    host = os.environ.get("DATABRICKS_HOST") or os.environ.get(
        "DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"
    )
    http_path = os.environ.get("DATABRICKS_HTTP_PATH") or os.environ.get(
        "DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"
    )
    token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get(
        "DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"
    )
    if host and http_path and token:
        return DatabricksCredentials(
            server_hostname=_normalize_host(host),
            http_path=http_path,
            access_token=token,
        )
    return None


def _find_secrets_toml() -> Path | None:
    for root in [Path.cwd(), *Path.cwd().parents]:
        candidate = root / ".dlt" / "secrets.toml"
        if candidate.exists():
            return candidate
    return None


def _from_secrets_toml() -> DatabricksCredentials | None:
    path = _find_secrets_toml()
    if path is None:
        return None
    try:
        import tomllib
    except ImportError:  # pragma: no cover
        import tomli as tomllib  # type: ignore[no-redef]
    payload = tomllib.loads(path.read_text())
    creds = payload.get("destination", {}).get("databricks", {}).get("credentials", {})
    host = creds.get("server_hostname")
    http_path = creds.get("http_path")
    token = creds.get("access_token")
    if host and http_path and token:
        return DatabricksCredentials(
            server_hostname=_normalize_host(str(host)),
            http_path=str(http_path),
            access_token=str(token),
        )
    return None


def load_databricks_credentials() -> DatabricksCredentials:
    creds = _from_env() or _from_secrets_toml()
    if creds is None:
        raise RuntimeError(
            "Local Spark cannot read Unity Catalog tables. Set Databricks warehouse "
            "credentials in .dlt/secrets.toml (same as ingestion) or pass --bronze-path."
        )
    return creds


def _clear_volume_files(cursor, path: str) -> None:
    """Remove leftover files so CTAS cannot reread previous parquet parts."""
    try:
        cursor.execute(f"LIST '{path}'")
        rows = cursor.fetchall() or []
        entries = list(rows)
    except Exception:
        return
    for row in entries:
        entry = row[0] if row else None
        if not entry:
            continue
        try:
            cursor.execute(f"REMOVE '{entry}'")
        except Exception:
            _clear_volume_files(cursor, str(entry))


def warehouse_connect(staging_allowed_local_path: str | Path | None = None):
    from databricks import sql

    creds = load_databricks_credentials()
    kwargs: dict = {
        "server_hostname": creds.server_hostname,
        "http_path": creds.http_path,
        "access_token": creds.access_token,
    }
    if staging_allowed_local_path is not None:
        kwargs["staging_allowed_local_path"] = str(Path(staging_allowed_local_path).resolve())
    return sql.connect(**kwargs)


def fetch_sql(query: str) -> tuple[list[str], list[tuple]]:
    with warehouse_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            names = [col[0] for col in (cursor.description or [])]
            rows = [tuple(row) for row in (cursor.fetchall() or [])]
    return names, rows


def publish_parquet_to_uc(
    local_dir: Path,
    *,
    volume_path: str,
    table: str,
    partition_by: list[str],
) -> None:
    catalog, schema, volume = parse_volume_path(volume_path)
    files = [
        path
        for path in local_dir.rglob("*")
        if path.is_file() and not path.name.startswith("_") and not path.name.startswith(".")
    ]
    if not files:
        raise FileNotFoundError(f"No parquet files to publish under {local_dir}")
    catalog = _require_ident(catalog, "catalog")
    schema = _require_ident(schema, "schema")
    volume = _require_ident(volume, "volume")
    table_parts = table.split(".")
    if len(table_parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got {table}")
    for part, field in zip(table_parts, ("catalog", "schema", "table")):
        _require_ident(part, field)

    with warehouse_connect(local_dir) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            try:
                cursor.execute(f"DROP VOLUME IF EXISTS {catalog}.{schema}.{volume}")
            except Exception as exc:
                print(f"Could not drop volume {catalog}.{schema}.{volume}: {exc}")
            cursor.execute(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
            _clear_volume_files(cursor, volume_path)
            for path in files:
                relative = path.relative_to(local_dir).as_posix()
                dest = f"{volume_path.rstrip('/')}/{relative}"
                cursor.execute(f"PUT '{path.resolve().as_posix()}' INTO '{dest}' OVERWRITE")
            if partition_by:
                partitioned = ", ".join(_require_ident(col, "partition") for col in partition_by)
                cursor.execute(
                    f"CREATE OR REPLACE TABLE {table} "
                    f"USING DELTA PARTITIONED BY ({partitioned}) "
                    f"AS SELECT * FROM parquet.`{volume_path}`"
                )
            else:
                cursor.execute(
                    f"CREATE OR REPLACE TABLE {table} "
                    f"USING DELTA AS SELECT * FROM parquet.`{volume_path}`"
                )
    print(f"Wrote delta table {table} from {volume_path}")
