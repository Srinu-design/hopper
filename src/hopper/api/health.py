import asyncio
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, Request, Response, status
from sqlalchemy import text

from hopper.config import get_settings

router = APIRouter(tags=["health"])
log = structlog.get_logger()

_PROBE_TIMEOUT_SECONDS = 2.0


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness: the process is up. Touches no backing service."""
    return {"status": "ok"}


async def _check_postgres(request: Request) -> None:
    async with request.app.state.engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def _check_redis(request: Request) -> None:
    await request.app.state.redis.ping()


@router.get("/readyz")
async def readyz(request: Request, response: Response) -> dict[str, Any]:
    """Readiness: not draining, and Postgres is reachable. Redis is checked and reported, but
    without it the API still serves (rate limits fall back to in-process buckets, ADR-0008),
    so a Redis outage answers 200 "degraded" instead of taking every replica out of rotation.

    Draining (the drain file exists) answers 503 while every other route keeps working:
    deploy.sh drains a replica so Caddy stops sending it requests before it is replaced.
    """
    # One stat() of a local file: microseconds, cheaper than a hop to a thread.
    if Path(get_settings().drain_file).exists():  # noqa: ASYNC240
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "draining", "checks": {}}
    checks = {"postgres": _check_postgres, "redis": _check_redis}
    results: dict[str, str] = {}
    for name, check in checks.items():
        try:
            await asyncio.wait_for(check(request), timeout=_PROBE_TIMEOUT_SECONDS)
            results[name] = "ok"
        except Exception as exc:
            log.warning("readiness_check_failed", dependency=name, error=repr(exc))
            results[name] = "unavailable"
    if results["postgres"] != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        overall = "unavailable"
    else:
        overall = "ok" if results["redis"] == "ok" else "degraded"
    return {"status": overall, "checks": results}
