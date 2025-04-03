import logging
import os

from pydantic_settings import BaseSettings, SettingsConfigDict

from main.schemas.config import MergeTableConfig


class Config(BaseSettings):
    ENVIRONMENT: str
    LOGGING_LEVEL: int = logging.INFO

    # API Key for authentication
    API_KEY: str

    # Coda API configuration
    CODA_API_TOKEN: str

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
