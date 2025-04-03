from typing import Any

from pydantic import BaseModel


class SourceTable(BaseModel):
    doc_id: str
    table_id: str
    project: str | None = None


class Config(BaseModel):
    api_token: str | None = None
    destination_doc_id: str
    destination_table_id: str
    source_tables: list[SourceTable] = []


class ColumnSchema(BaseModel):
    name: str
    type: str
    id: str | None = None
    display: bool | None = None


class TableRow(BaseModel):
    id: str | None = None
    values: dict[str, Any]
    source_id: str | None = None
    hash: str | None = None


class MergeResult(BaseModel):
    success: bool
    totalRowsProcessed: int
    newRows: int
    updatedRows: int
    deletedRows: int
    destinationTableId: str
