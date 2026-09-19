from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class KeysContract(BaseModel):
    business_keys: list[str] = Field(default_factory=list)


class BronzeRef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    catalog: str | None = "nxp"
    schema_name: str = Field(alias="schema", default="bronze")
    table: str


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


class EtlContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    name: str
    keys: KeysContract | None = None
    bronze: BronzeRef
    layers: LayersContract

    @property
    def bronze_fqn(self) -> str:
        catalog = self.bronze.catalog or "nxp"
        return f"{catalog}.{self.bronze.schema_name}.{self.bronze.table}"

    @property
    def business_keys(self) -> list[str]:
        if self.keys and self.keys.business_keys:
            return self.keys.business_keys
        return ["zone", "datetime"]
