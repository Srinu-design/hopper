import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version

import structlog
from fastapi import FastAPI
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from hopper import metrics, web
from hopper.api import admin, dlq, health, jobs, schedules
from hopper.api.body_limit import BodySizeLimitMiddleware
from hopper.api.errors import install_error_handlers
from hopper.api.http_metrics import HttpMetricsMiddleware
from hopper.auth.api_keys import ApiKeyAuthenticator
from hopper.config import get_settings
from hopper.db import create_engine, create_redis
from hopper.logging import configure_logging
from hopper.ratelimit.backpressure import Backpressure
from hopper.ratelimit.limiter import RateLimiter
from hopper.ratelimit.redis_health import RedisHealth


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Engine and Redis client are lazy: no connection is opened until first use,
    # so the process starts (and /healthz is green) even if a dependency is down.
    settings = get_settings()
    settings.require_api_secrets()
    configure_logging(settings.log_level)
    app.state.engine = create_engine(settings)
    app.state.redis = create_redis(settings)
    app.state.api_keys = ApiKeyAuthenticator(
        app.state.engine,
        settings.api_key_pepper.get_secret_value().encode(),
        cache_seconds=settings.api_key_cache_seconds,
    )
    redis_health = RedisHealth(settings.redis_retry_seconds)
    app.state.limiter = RateLimiter(
        app.state.redis, namespace=settings.redis_namespace, health=redis_health
    )
    app.state.backpressure = Backpressure(
        app.state.redis,
        namespace=settings.redis_namespace,
        global_limit=settings.global_max_queue_depth,
        health=redis_health,
    )
    stop_metrics = metrics.serve(settings.metrics_port)
    try:
        yield
    finally:
        stop_metrics()
        await app.state.redis.aclose()
        await app.state.engine.dispose()


# A client may pass its own X-Request-ID; anything long or odd is replaced, not logged.
_REQUEST_ID = re.compile(r"[A-Za-z0-9._-]{1,128}")


class RequestContextMiddleware:
    """Gives every request an id for its log lines (and its job, if it enqueues one), and
    stamps the response with X-Request-ID and, where a token bucket was consulted, the
    X-RateLimit headers. Errors included: a 429 says how many tokens are left (none).

    Plain ASGI, like HttpMetricsMiddleware: @app.middleware("http") would wrap it in
    BaseHTTPMiddleware, which costs a task and a response copy on every request."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        incoming = Headers(scope=scope).get("x-request-id", "")
        request_id = incoming if _REQUEST_ID.fullmatch(incoming) else uuid.uuid4().hex
        state = scope.setdefault("state", {})  # what request.state reads and writes
        state["request_id"] = request_id

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id
                decision = state.get("rate_limit")
                if decision is not None:
                    headers["X-RateLimit-Limit"] = str(decision.limit)
                    headers["X-RateLimit-Remaining"] = str(decision.remaining)
            await send(message)

        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            await self.app(scope, receive, send_with_headers)
        finally:
            # The routes run in this task now, so the tenant they bound is unbound here too.
            structlog.contextvars.unbind_contextvars("request_id", "tenant_id")


def create_app() -> FastAPI:
    # One version, from pyproject.toml: a number written here as well drifted from it.
    app = FastAPI(title="Hopper", version=version("hopper"), lifespan=lifespan)
    # Added last = outermost: metrics time everything, then the request id exists before
    # anything else runs.
    app.add_middleware(BodySizeLimitMiddleware)
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(HttpMetricsMiddleware)
    install_error_handlers(app)
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(dlq.router)
    app.include_router(schedules.router)
    app.include_router(admin.router)
    web.install(app)  # the page at /, after the API's routes
    return app


app = create_app()
