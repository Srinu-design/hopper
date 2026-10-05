import re

import pytest
from pydantic import ValidationError

from hopper.config import Settings
from tests.helpers import ROOT


def test_lease_timing_defaults() -> None:
    settings = Settings()
    assert (settings.lease_seconds, settings.heartbeat_seconds) == (30, 10.0)
    assert settings.shutdown_grace_seconds == 25.0  # under Compose's 30 s stop_grace_period
    assert (settings.reaper_interval_seconds, settings.reaper_batch_size) == (5.0, 500)


def test_heartbeat_must_be_shorter_than_the_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LEASE_SECONDS", "10")
    monkeypatch.setenv("HEARTBEAT_SECONDS", "10")
    with pytest.raises(ValidationError, match="HEARTBEAT_SECONDS must be shorter"):
        Settings()


@pytest.mark.parametrize(
    ("pepper", "jwt_secret", "problem"),
    [
        ("", "x" * 32, "API_KEY_PEPPER"),
        ("short", "x" * 32, "API_KEY_PEPPER"),
        ("p" * 16, "", "JWT_SECRET"),
        ("p" * 16, "x" * 31, "JWT_SECRET"),
    ],
)
def test_the_api_refuses_to_start_without_real_secrets(
    monkeypatch: pytest.MonkeyPatch, pepper: str, jwt_secret: str, problem: str
) -> None:
    monkeypatch.setenv("API_KEY_PEPPER", pepper)
    monkeypatch.setenv("JWT_SECRET", jwt_secret)
    with pytest.raises(RuntimeError, match=problem):
        Settings().require_api_secrets()


def test_limit_and_observability_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("REDIS_TIMEOUT_SECONDS", "METRICS_PORT"):  # conftest sets these for tests
        monkeypatch.delenv(name, raising=False)
    settings = Settings()
    assert settings.metrics_port == 9100
    assert settings.global_max_queue_depth == 1_000_000
    assert settings.backpressure_retry_after_seconds == 5
    assert settings.depth_interval_seconds == 1.0
    assert settings.redis_timeout_seconds == 0.25
    assert settings.redis_retry_seconds == 5.0


def test_retention_defaults() -> None:
    settings = Settings()
    assert settings.retention_days == 7
    assert (settings.retention_interval_seconds, settings.retention_batch_size) == (3600.0, 1000)


def test_retention_keeps_at_least_a_day(monkeypatch: pytest.MonkeyPatch) -> None:
    """0 days would delete every finished job on the next pass, so it is refused."""
    monkeypatch.setenv("RETENTION_DAYS", "0")
    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("namespace", ["", "has space", "a:b", "x" * 65])
def test_redis_namespace_is_a_plain_word(monkeypatch: pytest.MonkeyPatch, namespace: str) -> None:
    monkeypatch.setenv("REDIS_NAMESPACE", namespace)
    with pytest.raises(ValidationError):
        Settings()


def test_every_setting_is_documented_in_env_example() -> None:
    """.env.example is where people look up a setting; a new one must not be missing there."""
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    missing = [
        name.upper()
        for name in Settings.model_fields
        if not re.search(rf"^#? *{name.upper()}=", example, flags=re.MULTILINE)
    ]
    assert missing == []
