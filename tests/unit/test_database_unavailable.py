"""Which errors mean "Postgres is unreachable, retry" (503) and which are bugs (500)."""

import socket

import asyncpg
import pytest
from sqlalchemy import exc as sa_exc

from hopper.api.errors import database_unavailable


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
