import logging
import os

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class SourceTable(BaseModel):
    doc_id: str
    table_id: str
    project: str | None


class MergeTableConfig(BaseModel):
    destination_doc_id: str
    destination_table_id: str
    source_tables: list[SourceTable]


class Config(BaseSettings):
    ENVIRONMENT: str
    LOGGING_LEVEL: int = logging.INFO

    # Coda API configuration
    CODA_API_TOKEN: str | None

    # Merge table configuration
    MERGE_TABLE_CONFIG: MergeTableConfig

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file_encoding="utf-8",
    )


environment = os.environ.get("ENVIRONMENT", "local")
config = Config(
    ENVIRONMENT=environment,
    # ".env.{environment}" takes priority over ".env"
    _env_file=[".env", f".env.{environment}"],
)
