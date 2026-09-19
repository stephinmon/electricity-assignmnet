from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Iterable
from typing import Any

import dlt
from databricks import sql

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SKIP_SCHEMAS = frozenset({"information_schema"})


def _require_ident(value: str, field: str) -> str:
    if not _SAFE_IDENT.match(value):
        raise ValueError(f"Unsafe Unity Catalog identifier for {field}: {value!r}")
    return value


def _hostname() -> str:
    host = str(dlt.secrets["destination.databricks.credentials.server_hostname"])
    return host.replace("https://", "").replace("http://", "").split("/")[0]


def connect():
    return sql.connect(
        server_hostname=_hostname(),
        http_path=str(dlt.secrets["destination.databricks.credentials.http_path"]),
        access_token=str(dlt.secrets["destination.databricks.credentials.access_token"]),
    )


def default_catalog() -> str:
    try:
        catalog = dlt.secrets["destination.databricks.credentials.catalog"]
    except Exception:
        catalog = None
    return str(catalog or "nxp")


def _named_row(cursor, row: tuple) -> dict[str, Any]:
    names = [str(col[0]).lower() for col in (cursor.description or [])]
    return dict(zip(names, row, strict=False)) if names else {}


def _value(row: tuple, named: dict[str, Any], keys: Iterable[str], fallback_index: int = 0) -> str:
    for key in keys:
        value = named.get(key)
        if value not in (None, ""):
            return str(value)
    return str(row[fallback_index])


def list_schemas(cursor, catalog: str) -> list[str]:
    catalog = _require_ident(catalog, "catalog")
    cursor.execute(f"SHOW SCHEMAS IN {catalog}")
    rows = cursor.fetchall() or []
    schemas: list[str] = []
    for row in rows:
        named = _named_row(cursor, row)
        name = _value(row, named, ("databasename", "namespace", "schema_name", "schema"))
        if name.lower() in _SKIP_SCHEMAS:
            continue
        schemas.append(_require_ident(name, "schema"))
    return sorted(set(schemas))


def list_tables(cursor, catalog: str, schema: str) -> list[str]:
    catalog = _require_ident(catalog, "catalog")
    schema = _require_ident(schema, "schema")
    cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
    rows = cursor.fetchall() or []
    tables: list[str] = []
    for row in rows:
        named = _named_row(cursor, row)
        name = _value(row, named, ("tablename", "table_name", "name"))
        tables.append(_require_ident(name, "table"))
    return sorted(set(tables))


def list_volumes(cursor, catalog: str, schema: str) -> list[str]:
    catalog = _require_ident(catalog, "catalog")
    schema = _require_ident(schema, "schema")
    try:
        cursor.execute(f"SHOW VOLUMES IN {catalog}.{schema}")
        rows = cursor.fetchall() or []
    except Exception as exc:
        print(f"Could not list volumes in {catalog}.{schema}: {exc}")
        return []
    volumes: list[str] = []
    for row in rows:
        named = _named_row(cursor, row)
        name = _value(row, named, ("volumename", "volume_name", "name"))
        volumes.append(_require_ident(name, "volume"))
    return sorted(set(volumes))


def collect_objects(
    cursor,
    catalog: str,
    schemas: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    schema_names = schemas or list_schemas(cursor, catalog)
    tables: list[str] = []
    volumes: list[str] = []
    for schema in schema_names:
        for table in list_tables(cursor, catalog, schema):
            tables.append(f"{catalog}.{schema}.{table}")
        for volume in list_volumes(cursor, catalog, schema):
            volumes.append(f"{catalog}.{schema}.{volume}")
    return tables, volumes


def _run_sql(cursor, statement: str, *, dry_run: bool) -> None:
    prefix = "[dry-run] " if dry_run else ""
    print(f"{prefix}{statement}")
    if not dry_run:
        cursor.execute(statement)


def _drop_schemas(cursor, catalog: str, schemas: list[str], *, dry_run: bool) -> None:
    for schema in schemas:
        statement = f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"
        _run_sql(cursor, statement, dry_run=dry_run)


def drop_tables_and_volumes(
    cursor,
    *,
    catalog: str,
    schemas: list[str] | None = None,
    dry_run: bool = True,
    drop_schemas: bool = False,
) -> None:
    catalog = _require_ident(catalog, "catalog")
    listed = schemas or list_schemas(cursor, catalog)
    schema_names = [_require_ident(item, "schema") for item in listed]
    tables, volumes = collect_objects(cursor, catalog, schema_names)
    if not tables and not volumes:
        print(f"No tables or volumes found in catalog {catalog}")
        if drop_schemas:
            _drop_schemas(cursor, catalog, schema_names, dry_run=dry_run)
        return
    print(f"Found {len(tables)} table(s) and {len(volumes)} volume(s) in {catalog}")
    for table in tables:
        _run_sql(cursor, f"DROP TABLE IF EXISTS {table}", dry_run=dry_run)
    for volume in volumes:
        try:
            _run_sql(cursor, f"DROP VOLUME IF EXISTS {volume}", dry_run=dry_run)
        except Exception as exc:
            print(f"Could not drop volume {volume}: {exc}")
    if drop_schemas:
        _drop_schemas(cursor, catalog, schema_names, dry_run=dry_run)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Drop Unity Catalog tables and volumes in this project's Databricks catalog "
            "(default: nxp). Uses the same .dlt/secrets.toml warehouse credentials as ingest."
        )
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Unity Catalog name. Defaults to destination.databricks.credentials.catalog or nxp.",
    )
    parser.add_argument(
        "--schema",
        action="append",
        dest="schemas",
        help="Limit to one schema (repeatable). Default: every schema in the catalog.",
    )
    parser.add_argument(
        "--drop-schemas",
        action="store_true",
        help="Also DROP SCHEMA ... CASCADE after tables and volumes.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print DROP statements without executing them.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Execute the drops. Required unless --dry-run is set.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.yes and args.dry_run:
        raise ValueError("Use either --yes or --dry-run, not both.")
    if not args.yes and not args.dry_run:
        args.dry_run = True
        print("No --yes given; running as --dry-run. Re-run with --yes to delete.")
    catalog = args.catalog or default_catalog()
    with connect() as conn:
        with conn.cursor() as cursor:
            drop_tables_and_volumes(
                cursor,
                catalog=catalog,
                schemas=args.schemas,
                dry_run=args.dry_run,
                drop_schemas=args.drop_schemas,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
