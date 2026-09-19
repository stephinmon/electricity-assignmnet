from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AuthContract(BaseModel):
    type: Literal["api_key"]
    name: str
    location: Literal["header", "query"] = "header"
    secret_key: str


class ParamValues(BaseModel):
    type: Literal["values"]
    values: list[str]


class ResourceContract(BaseModel):
    name: str
    path: str
    method: Literal["GET", "POST"] = "GET"
    paginator: str = "single_page"
    response_mode: Literal["object", "list"] = "object"
    records_path: str | None = None
    params: dict[str, ParamValues | Any] = Field(default_factory=dict)


class SourceContract(BaseModel):
    name: str
    kind: Literal["rest_api"]
    base_url: str
    auth: AuthContract
    resources: list[ResourceContract]

    @field_validator("base_url")
    @classmethod
    def require_trailing_semantics(cls, value: str) -> str:
        return value if value.endswith("/") else f"{value}/"


class LandingZoneContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str = "nxp"
    schema_name: str = Field(alias="schema", default="landing")
    volume: str = "raw"
    file_format: Literal["jsonl"] = "jsonl"
    prefix: str | None = None
    retention_days: int = 7

    @field_validator("retention_days")
    @classmethod
    def retention_must_be_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("landing_zone.retention_days must be at least 1")
        return value


class BronzeContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str | None = "nxp"
    schema_name: str = Field(alias="schema", default="landing")
    table: str
    destination: Literal["databricks", "duckdb"] = "databricks"
    write_disposition: Literal["append", "merge", "replace"] = "append"
    file_format: Literal["jsonl", "parquet"] = "parquet"
    table_format: Literal["json", "delta", "iceberg"] = "delta"
    staging_volume_name: str | None = None
    cluster: list[str] = Field(default_factory=list)
    partition_by: list[str] = Field(default_factory=lambda: ["year", "month", "day"])
    table_comment: str | None = None


class ColumnContract(BaseModel):
    name: str
    data_type: str
    nullable: bool = True
    description: str | None = None


class SchemaContract(BaseModel):
    columns: list[ColumnContract]


class KeysContract(BaseModel):
    business_keys: list[str] = Field(default_factory=list)


class LayerTableContract(BaseModel):
    name: str | None = None
    table: str
    path: str | None = None
    partition_by: list[str] | None = None


class LayerContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    table: str | None = None
    path: str | None = None
    partition_by: list[str] = Field(default_factory=lambda: ["year", "month", "day"])
    tables: list[LayerTableContract] = Field(default_factory=list)

    def qualified_table(self, table: str | None = None) -> str:
        name = table or self.table
        if not name:
            raise ValueError("Layer table name is required")
        catalog = self.catalog or "nxp"
        schema = self.schema_name or "default"
        return f"{catalog}.{schema}.{name}"

    def parquet_path(self, table: str | None = None, path: str | None = None) -> str:
        if path:
            return path
        if self.path and (table is None or table == self.table):
            return self.path
        name = table or self.table
        if not name:
            raise ValueError("Layer table name is required to derive a parquet path")
        catalog = self.catalog or "nxp"
        schema = self.schema_name or "default"
        return f"/Volumes/{catalog}/{schema}/{name}"

    def outputs(self) -> list[LayerTableContract]:
        if self.tables:
            return self.tables
        if not self.table:
            return []
        return [LayerTableContract(name=self.table, table=self.table, path=self.path)]


class LayersContract(BaseModel):
    silver: LayerContract | None = None
    gold: LayerContract | None = None


class IngestionContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    version: str
    name: str
    domain: str | None = None
    description: str | None = None
    source: SourceContract
    landing_zone: LandingZoneContract
    bronze: BronzeContract
    keys: KeysContract | None = None
    schema_: SchemaContract = Field(alias="schema")
    layers: LayersContract | None = None

    @property
    def resource(self) -> ResourceContract:
        if not self.source.resources:
            raise ValueError("Contract source.resources must contain at least one resource")
        return self.source.resources[0]

    def landing_prefix(self) -> str:
        return self.landing_zone.prefix or self.bronze.table

    def dlt_columns(self) -> dict[str, dict[str, Any]]:
        return {
            column.name: {
                "data_type": column.data_type,
                "nullable": column.nullable,
                "description": column.description,
            }
            for column in self.schema_.columns
        }

    def iterable_param(self, name: str) -> list[str]:
        raw = self.resource.params.get(name)
        if isinstance(raw, ParamValues):
            return raw.values
        if isinstance(raw, dict) and raw.get("type") == "values":
            return list(raw["values"])
        raise KeyError(f"Resource param '{name}' is not a values iterator")
