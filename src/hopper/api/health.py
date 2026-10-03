import asyncio
from typing import Any

import structlog
from fastapi import APIRouter, Request, Response, status
from sqlalchemy import text

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
    """Readiness: Postgres and Redis are reachable."""
    checks = {"postgres": _check_postgres, "redis": _check_redis}
    results: dict[str, str] = {}
    for name, check in checks.items():
        try:
            await asyncio.wait_for(check(request), timeout=_PROBE_TIMEOUT_SECONDS)
            results[name] = "ok"
        except Exception as exc:
            log.warning("readiness_check_failed", dependency=name, error=repr(exc))
            results[name] = "unavailable"
    ready = all(v == "ok" for v in results.values())
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if ready else "unavailable", "checks": results}
