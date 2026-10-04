import pytest
from pydantic import ValidationError

from hopper.config import Settings


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
