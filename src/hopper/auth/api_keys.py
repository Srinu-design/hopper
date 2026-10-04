import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.auth import keys, store

log = structlog.get_logger()

_TOUCH_EVERY_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class Caller:
    """The tenant behind a verified API key, with the limits the API enforces on it."""

    tenant_id: UUID
    rate_per_sec: float  # token bucket refill, per route class
    burst: int  # token bucket capacity
    max_queue_depth: int  # backpressure: queued jobs allowed before enqueues get 429


class ApiKeyAuthenticator:
    """Turns a presented API key into its Caller (tenant and limits), or None.

    A key record is cached per process for `cache_seconds`, so a busy client costs one
    database read a minute instead of one per request. The cache holds the stored hash, not
    the secret, so every request is still verified with a constant-time compare. Trade-off:
    a revoked key keeps working on each API replica until its cache entry expires, and a
    change to the tenant's limits takes as long to apply.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        pepper: bytes,
        *,
        cache_seconds: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._engine = engine
        self._pepper = pepper
        self._cache_seconds = cache_seconds
        self._clock = clock
        self._cache: dict[str, tuple[store.KeyRecord, float]] = {}
        self._touched: dict[UUID, float] = {}

    async def authenticate(self, presented: str) -> Caller | None:
        parsed = keys.parse(presented)
        if parsed is None:
            return None
        prefix, secret = parsed
        now = self._clock()
        cached = self._cache.get(prefix)
        if cached is not None and cached[1] > now:
            record = cached[0]
        else:
            found = await store.get_api_key(self._engine, prefix)
            if found is None:
                self._cache.pop(prefix, None)
                return None
            record = found
            self._cache[prefix] = (record, now + self._cache_seconds)
        if record.revoked or not keys.matches(self._pepper, secret, record.key_hash):
            return None
        await self._touch(record.id, now)
        return Caller(
            record.tenant_id, float(record.rate_per_sec), record.burst, record.max_queue_depth
        )

    def evict(self, prefix: str) -> None:
        """Forget a cached key now, so a revocation applies on this replica at once."""
        self._cache.pop(prefix, None)

    async def _touch(self, key_id: UUID, now: float) -> None:
        """Record last_used_at at most once a minute per key: not a write per request."""
        if now - self._touched.get(key_id, -math.inf) < _TOUCH_EVERY_SECONDS:
            return
        self._touched[key_id] = now
        try:
            await store.touch_api_key(self._engine, key_id)
        except Exception:
            log.warning("api_key_touch_failed", key_id=str(key_id), exc_info=True)
