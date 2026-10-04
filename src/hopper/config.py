from functools import lru_cache
from typing import Self

from pydantic import Field, SecretStr, model_validator
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

    # Auth (API only). API keys are stored as HMAC-SHA256(pepper, secret): the pepper lives
    # here, never in the database. Admin JWTs are signed with HS256 under jwt_secret.
    api_key_pepper: SecretStr = SecretStr("")
    jwt_secret: SecretStr = SecretStr("")
    jwt_issuer: str = "hopper"
    jwt_audience: str = "hopper-admin"
    jwt_ttl_seconds: int = Field(default=900, ge=60, le=3600)
    # Verified keys are cached per process, so a revoked key works for up to this long.
    api_key_cache_seconds: float = Field(default=60.0, ge=0)

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

    # http task. Private, loopback and link-local targets are refused unless this is set,
    # which is only for tests and local demos against a server on your own machine.
    http_allow_private_networks: bool = False
    http_connect_timeout: float = Field(default=5.0, gt=0)
    http_read_timeout: float = Field(default=10.0, gt=0)

    # Scheduler: the reaper requeues jobs whose lease ran out; the cron loop fires schedules.
    reaper_interval_seconds: float = Field(default=5.0, gt=0)
    reaper_batch_size: int = Field(default=500, ge=1)
    cron_interval_seconds: float = Field(default=1.0, gt=0)
    cron_batch_size: int = Field(default=100, ge=1)

    @model_validator(mode="after")
    def _heartbeat_inside_lease(self) -> Self:
        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError("HEARTBEAT_SECONDS must be shorter than LEASE_SECONDS")
        return self

    def require_api_secrets(self) -> None:
        """The API refuses to start without real secrets; workers do not need them."""
        if len(self.api_key_pepper.get_secret_value()) < 16:
            raise RuntimeError("API_KEY_PEPPER must be set to at least 16 characters")
        if len(self.jwt_secret.get_secret_value()) < 32:
            raise RuntimeError("JWT_SECRET must be set to at least 32 characters")

    @property
    def queues(self) -> list[str]:
        return [q.strip() for q in self.worker_queues.split(",") if q.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
