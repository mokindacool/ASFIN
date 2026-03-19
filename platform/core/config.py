from __future__ import annotations
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Expected:
      DATABASE_URL (postgresql://asfint:asfint@localhost:5432/asfint)
    """
    DATABASE_URL: str

    model_config = SettingsConfigDict(
        env_file=".env",  # repo root .env
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings()