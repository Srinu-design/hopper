from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process settings. Environment variables only; no config files."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str = "postgresql+asyncpg://hopper:hopper@127.0.0.1:5432/hopper"
    redis_url: str = "redis://127.0.0.1:6379/0"
    log_level: str = "INFO"
    # Handlers do not hold DB connections while running, so a small pool suffices.
    db_pool_size: int = Field(default=5, ge=1)
    # Keep replicas x pool size well under Postgres max_connections (default 100).
    db_max_overflow: int = Field(default=0, ge=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
