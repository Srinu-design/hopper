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
