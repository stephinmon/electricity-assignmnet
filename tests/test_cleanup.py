from unittest.mock import MagicMock

import pytest

from energy_platform.ingestion.cleanup import (
    collect_objects,
    drop_tables_and_volumes,
    list_schemas,
    list_tables,
    list_volumes,
)


def _cursor(rows_by_sql: dict[str, list[tuple]], columns: dict[str, list[str]]) -> MagicMock:
    cursor = MagicMock()

    def execute(statement: str):
        cursor._last = statement
        cursor.description = [(name,) for name in columns.get(statement, [])]
        cursor._rows = rows_by_sql.get(statement, [])

    cursor.execute.side_effect = execute
    cursor.fetchall.side_effect = lambda: cursor._rows
    return cursor


def test_lists_tables_and_volumes_in_catalog():
    cursor = _cursor(
        {
            "SHOW SCHEMAS IN nxp": [("bronze",), ("landing",), ("information_schema",)],
            "SHOW TABLES IN nxp.bronze": [("electricity_mix_latest",), ("_dlt_loads",)],
            "SHOW TABLES IN nxp.landing": [],
            "SHOW VOLUMES IN nxp.bronze": [("dlt_staging",)],
            "SHOW VOLUMES IN nxp.landing": [("raw",)],
        },
        {
            "SHOW SCHEMAS IN nxp": ["databaseName"],
            "SHOW TABLES IN nxp.bronze": ["tableName"],
            "SHOW TABLES IN nxp.landing": ["tableName"],
            "SHOW VOLUMES IN nxp.bronze": ["volumeName"],
            "SHOW VOLUMES IN nxp.landing": ["volumeName"],
        },
    )
    tables, volumes = collect_objects(cursor, "nxp")
    assert tables == ["nxp.bronze._dlt_loads", "nxp.bronze.electricity_mix_latest"]
    assert volumes == ["nxp.bronze.dlt_staging", "nxp.landing.raw"]
    assert list_schemas(cursor, "nxp") == ["bronze", "landing"]


def test_drop_dry_run_does_not_execute_drops():
    cursor = _cursor(
        {
            "SHOW TABLES IN nxp.bronze": [("electricity_mix_latest",)],
            "SHOW VOLUMES IN nxp.bronze": [("raw",)],
        },
        {
            "SHOW TABLES IN nxp.bronze": ["tableName"],
            "SHOW VOLUMES IN nxp.bronze": ["volumeName"],
        },
    )
    drop_tables_and_volumes(cursor, catalog="nxp", schemas=["bronze"], dry_run=True)
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "DROP TABLE IF EXISTS nxp.bronze.electricity_mix_latest" not in statements
    assert "DROP VOLUME IF EXISTS nxp.bronze.raw" not in statements


def test_drop_yes_drops_tables_then_volumes():
    cursor = _cursor(
        {
            "SHOW TABLES IN nxp.bronze": [("electricity_mix_latest",)],
            "SHOW VOLUMES IN nxp.bronze": [("raw",)],
        },
        {
            "SHOW TABLES IN nxp.bronze": ["tableName"],
            "SHOW VOLUMES IN nxp.bronze": ["volumeName"],
        },
    )
    drop_tables_and_volumes(cursor, catalog="nxp", schemas=["bronze"], dry_run=False)
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    table_drop = "DROP TABLE IF EXISTS nxp.bronze.electricity_mix_latest"
    volume_drop = "DROP VOLUME IF EXISTS nxp.bronze.raw"
    assert table_drop in statements
    assert volume_drop in statements
    assert statements.index(table_drop) < statements.index(volume_drop)


def test_rejects_unsafe_catalog():
    cursor = MagicMock()
    with pytest.raises(ValueError, match="Unsafe"):
        list_tables(cursor, "nxp; drop", "bronze")


def test_list_volumes_returns_empty_on_error():
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("no volumes")
    assert list_volumes(cursor, "nxp", "bronze") == []
