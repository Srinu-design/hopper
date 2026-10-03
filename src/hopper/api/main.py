from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as redis_asyncio
from fastapi import FastAPI

from hopper.api import health
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


def create_app() -> FastAPI:
    app = FastAPI(title="Hopper", version="0.1.0", lifespan=lifespan)
    app.include_router(health.router)
    return app


app = create_app()
