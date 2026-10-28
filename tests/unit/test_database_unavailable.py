"""Which errors mean "Postgres is unreachable, retry" (503) and which are bugs (500)."""

import socket

import asyncpg
import httpx
import pytest
import structlog
from sqlalchemy import exc as sa_exc

from hopper.api import errors
from hopper.api.errors import database_unavailable
from hopper.api.main import create_app


def _wrapped(orig: BaseException, *, invalidated: bool = False) -> sa_exc.DBAPIError:
    """An error as SQLAlchemy raises it: the driver's error on __cause__."""
    error = sa_exc.DBAPIError("SELECT 1", {}, orig, connection_invalidated=invalidated)
    error.__cause__ = orig
    return error


@pytest.mark.parametrize(
    "error",
    [
        ConnectionRefusedError(111, "Connect call failed"),
        socket.gaierror(-3, "Temporary failure in name resolution"),
        TimeoutError("connect timed out"),
        _wrapped(Exception("connection is closed"), invalidated=True),
        _wrapped(asyncpg.exceptions.CannotConnectNowError("the database system is starting up")),
        _wrapped(asyncpg.exceptions.AdminShutdownError("terminating connection")),
        _wrapped(asyncpg.exceptions.TooManyConnectionsError("too many clients")),
        _wrapped(asyncpg.exceptions.ConnectionDoesNotExistError("connection was closed")),
        sa_exc.TimeoutError("QueuePool limit of size 5 overflow 0 reached"),
    ],
    ids=lambda e: type(e.__cause__ or e).__name__,
)
def test_an_unreachable_database_is_a_503(error: BaseException) -> None:
    assert database_unavailable(error)


@pytest.mark.parametrize(
    "error",
    [
        _wrapped(asyncpg.exceptions.UndefinedColumnError('column "nope" does not exist')),
        _wrapped(asyncpg.exceptions.UniqueViolationError("duplicate key value")),
        _wrapped(asyncpg.exceptions.DeadlockDetectedError("deadlock detected")),
        ValueError("a bug"),
        KeyError("a bug"),
    ],
    ids=lambda e: type(e.__cause__ or e).__name__,
)
def test_any_other_error_stays_a_500(error: BaseException) -> None:
    assert not database_unavailable(error)


async def test_a_bug_is_a_500_logged_with_its_request_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """The handler runs after the request-context middleware has unbound the id."""
    # structlog caches a module's logger on first use; a fresh one is seen by capture_logs.
    monkeypatch.setattr(errors, "log", structlog.get_logger())
    app = create_app()

    async def boom() -> None:
        raise RuntimeError("a bug")

    app.add_api_route("/boom", boom)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    with structlog.testing.capture_logs() as logs:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/boom", headers={"X-Request-ID": "req-500"})
    assert resp.status_code == 500
    assert resp.json()["error"]["request_id"] == "req-500"
    [entry] = [e for e in logs if e["event"] == "unhandled_error"]
    assert entry["request_id"] == "req-500"
