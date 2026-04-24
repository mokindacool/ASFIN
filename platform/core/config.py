from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Central application settings loaded from environment variables or .env.
    """

    database_url: str = "postgresql://asfint:asfint@localhost:5432/asfint"
    debug: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("debug", mode="before")
    @classmethod
    def normalize_debug(cls, value):
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"release", "prod", "production", "false", "0", "no", "off"}:
                return False
            if lowered in {"debug", "dev", "development", "true", "1", "yes", "on"}:
                return True
        return value


settings = Settings()
