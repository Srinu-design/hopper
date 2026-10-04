"""Backpressure: refuse new work when a tenant's queue, or the whole system, is too deep.

The scheduler counts queued jobs once a second and publishes the counts to one Redis hash
(see scheduler/depth.py). The API only reads that hash, so a deep queue costs no extra query
on Postgres, at the price of a soft limit: it is up to about a second stale, and a burst that
arrives inside that second can overshoot it (ADR-0009).
"""

from enum import StrEnum
from uuid import UUID

import redis.asyncio as redis_asyncio

from hopper.ratelimit.limiter import REDIS_DOWN
from hopper.ratelimit.redis_health import RedisHealth

TOTAL_FIELD = "total"


def depth_key(namespace: str) -> str:
    """The hash the scheduler writes: field "total", plus one field per tenant id with
    queued jobs. A tenant with no field has none queued."""
    return f"{namespace}:depth"


class Verdict(StrEnum):
    QUOTA = "quota"  # 429: the tenant is over its own max_queue_depth
    OVERLOADED = "overloaded"  # 503: queued jobs across all tenants are over the global limit


class Backpressure:
    def __init__(
        self,
        redis: redis_asyncio.Redis,
        *,
        namespace: str,
        global_limit: int,
        health: RedisHealth,
    ) -> None:
        self._redis = redis
        self._key = depth_key(namespace)
        self._global_limit = global_limit
        self._health = health
        # The last counts read, used while Redis is unavailable: stale, but still a limit.
        self._last_total: int | None = None
        self._last_by_tenant: dict[UUID, int] = {}

    async def check(self, tenant_id: UUID, tenant_limit: int) -> Verdict | None:
        """None means go ahead. Unknown depth (no scheduler has published) also goes ahead."""
        total, queued = await self._read(tenant_id)
        if queued is not None and queued >= tenant_limit:
            return Verdict.QUOTA
        if total is not None and total >= self._global_limit:
            return Verdict.OVERLOADED
        return None

    async def _read(self, tenant_id: UUID) -> tuple[int | None, int | None]:
        if self._health.usable():
            try:
                raw_total, raw_queued = await self._redis.hmget(
                    self._key, [TOTAL_FIELD, str(tenant_id)]
                )
            except REDIS_DOWN as exc:
                self._health.failed("queue_depth", exc)
            else:
                if raw_total is None:
                    # The snapshot expired: no scheduler has counted for a while. Fail open.
                    self._last_total = None
                    self._last_by_tenant.clear()
                    return None, None
                total, queued = int(raw_total), int(raw_queued or 0)
                self._last_total = total
                self._last_by_tenant[tenant_id] = queued
                return total, queued
        return self._last_total, self._last_by_tenant.get(tenant_id)
