from typing import Any

from pydantic import BaseModel


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
