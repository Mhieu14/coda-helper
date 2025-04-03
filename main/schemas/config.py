from pydantic import BaseModel


class SourceTable(BaseModel):
    doc_id: str
    table_id: str
    project: str | None


class MergeTableConfig(BaseModel):
    destination_doc_id: str
    destination_table_id: str
    source_tables: list[SourceTable]
