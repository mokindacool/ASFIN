from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://asfint:asfint@localhost:5432/asfint"
    DATA_ROOT: str = "/data"

    class Config:
        env_file = ".env"


settings = Settings()
