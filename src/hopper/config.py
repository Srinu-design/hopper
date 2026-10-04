from functools import lru_cache
from typing import Self

from pydantic import Field, model_validator
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

    # Until API keys arrive (Week 5) every request is attributed to this tenant.
    default_tenant_name: str = "default"

    # Worker
    worker_queues: str = "default"  # comma separated
    worker_slots: int = Field(default=20, ge=1)
    worker_poll_interval: float = Field(default=0.25, gt=0)
    worker_max_idle_backoff: float = Field(default=0.5, gt=0)
    # A crashed worker's jobs are back in the queue within lease + reaper interval (~35 s).
    lease_seconds: int = Field(default=30, ge=1)
    # Every in-flight lease is renewed this often, in one statement: lease / 3, so a lease
    # survives two missed heartbeats.
    heartbeat_seconds: float = Field(default=10.0, gt=0)
    # On SIGTERM: wait this long for in-flight jobs, then release the rest. Must stay under
    # Compose's stop_grace_period (30 s) so Docker's SIGKILL never arrives first.
    shutdown_grace_seconds: float = Field(default=25.0, ge=0)

    # Scheduler: the reaper requeues jobs whose lease ran out.
    reaper_interval_seconds: float = Field(default=5.0, gt=0)
    reaper_batch_size: int = Field(default=500, ge=1)

    @model_validator(mode="after")
    def _heartbeat_inside_lease(self) -> Self:
        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError("HEARTBEAT_SECONDS must be shorter than LEASE_SECONDS")
        return self

    @property
    def queues(self) -> list[str]:
        return [q.strip() for q in self.worker_queues.split(",") if q.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
