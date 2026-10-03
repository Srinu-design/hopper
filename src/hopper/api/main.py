import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import redis.asyncio as redis_asyncio
import structlog
from fastapi import FastAPI, Request, Response

from hopper.api import health, jobs
from hopper.api.body_limit import BodySizeLimitMiddleware
from hopper.api.errors import install_error_handlers
from hopper.config import get_settings
from hopper.db import create_engine
from hopper.logging import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Engine and Redis client are lazy: no connection is opened until first use,
    # so the process starts (and /healthz is green) even if a dependency is down.
    settings = get_settings()
    configure_logging(settings.log_level)
    app.state.engine = create_engine(settings)
    app.state.redis = redis_asyncio.from_url(settings.redis_url)
    try:
        yield
    finally:
        await app.state.redis.aclose()
        await app.state.engine.dispose()


# A client may pass its own X-Request-ID; anything long or odd is replaced, not logged.
_REQUEST_ID = re.compile(r"[A-Za-z0-9._-]{1,128}")


async def request_id_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    incoming = request.headers.get("x-request-id", "")
    request_id = incoming if _REQUEST_ID.fullmatch(incoming) else uuid.uuid4().hex
    request.state.request_id = request_id
    structlog.contextvars.bind_contextvars(request_id=request_id)
    try:
        response = await call_next(request)
    finally:
        structlog.contextvars.unbind_contextvars("request_id")
    response.headers["X-Request-ID"] = request_id
    return response


def create_app() -> FastAPI:
    app = FastAPI(title="Hopper", version="0.2.0", lifespan=lifespan)
    # Added last = outermost, so the request id exists before anything else runs.
    app.add_middleware(BodySizeLimitMiddleware)
    app.middleware("http")(request_id_middleware)
    install_error_handlers(app)
    app.include_router(health.router)
    app.include_router(jobs.router)
    return app


app = create_app()
