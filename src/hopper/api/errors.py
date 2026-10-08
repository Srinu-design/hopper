"""One error shape for every response: {"error": {"code", "message", "request_id"}}.

Some errors add fields of their own inside "error", such as retry_after_ms on a 429.
"""

from typing import Any

import asyncpg
import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import exc as sa_exc
from starlette.exceptions import HTTPException as StarletteHTTPException

log = structlog.get_logger()

# How long a client is told to wait when Postgres cannot be reached.
DATABASE_RETRY_AFTER_SECONDS = 5

# asyncpg errors that mean the server is down, restarting, or out of connections, not that
# the query was wrong. SQLAlchemy wraps them, so they are found on the exception chain.
_POSTGRES_UNAVAILABLE = (
    asyncpg.exceptions.PostgresConnectionError,
    asyncpg.exceptions.CannotConnectNowError,
    asyncpg.exceptions.AdminShutdownError,
    asyncpg.exceptions.CrashShutdownError,
    asyncpg.exceptions.TooManyConnectionsError,
)


def database_unavailable(exc: BaseException) -> bool:
    """True when an error means Postgres could not be reached or dropped the connection.

    Such a request did nothing wrong and may succeed in a few seconds, so it gets 503 with
    Retry-After, not 500. A connect that fails (refused, or the host name does not resolve
    while the container is down) raises a plain OSError; a connection lost mid-query comes
    back from SQLAlchemy marked connection_invalidated; a pool with no free connection in
    time raises sqlalchemy's TimeoutError. Any other database error is a bug: 500.
    """
    if isinstance(exc, sa_exc.DBAPIError) and exc.connection_invalidated:
        return True
    if isinstance(exc, sa_exc.TimeoutError):
        return True
    seen: BaseException | None = exc
    while seen is not None:
        if isinstance(seen, (OSError, *_POSTGRES_UNAVAILABLE)):
            return True
        seen = seen.__cause__
    return False


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        headers: dict[str, str] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.headers = headers
        self.details = details


def _response(
    request: Request,
    status_code: int,
    code: str,
    message: str,
    headers: dict[str, str] | None = None,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    error = {"code": code, "message": message, "request_id": request_id, **(details or {})}
    return JSONResponse({"error": error}, status_code=status_code, headers=headers)


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError) -> JSONResponse:
        return _response(request, exc.status_code, exc.code, exc.message, exc.headers, exc.details)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "not_found", 405: "method_not_allowed", 413: "payload_too_large"}.get(
            exc.status_code, "http_error"
        )
        return _response(request, exc.status_code, code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0]
        where = ".".join(str(part) for part in first["loc"])
        return _response(request, 422, "validation_error", f"{where}: {first['msg']}")

    # Registered per class, not only under Exception: Starlette sends those to its outermost
    # middleware, which re-raises after answering and skips the X-Request-ID header.
    @app.exception_handler(OSError)
    @app.exception_handler(sa_exc.DBAPIError)
    @app.exception_handler(sa_exc.TimeoutError)
    async def database_error(request: Request, exc: Exception) -> JSONResponse:
        if not database_unavailable(exc):
            return await unexpected(request, exc)
        log.warning(
            "database_unavailable", path=request.url.path, error=f"{type(exc).__name__}: {exc}"
        )
        return _response(
            request,
            503,
            "database_unavailable",
            "the database is unavailable; retry after Retry-After seconds",
            headers={"Retry-After": str(DATABASE_RETRY_AFTER_SECONDS)},
        )

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled_error", path=request.url.path)
        return _response(request, 500, "internal_error", "internal server error")
