"""Per-tenant rate limiting: a token bucket in Redis, one atomic Lua script per request.

When Redis cannot be reached the limiter fails open to an in-process bucket with the same
maths (ADR-0008): the API keeps serving, each replica enforces the limit on its own, and the
queue-depth backpressure still protects Postgres.
"""

import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from importlib.resources import files

import redis.asyncio as redis_asyncio
from redis import exceptions as redis_errors

from hopper.metrics import RATELIMIT_FALLBACK
from hopper.ratelimit.redis_health import RedisHealth

TOKEN_BUCKET_LUA = files("hopper.ratelimit").joinpath("token_bucket.lua").read_text()

# Only "Redis is unreachable or slow" falls back. A script error is a bug and must surface.
REDIS_DOWN = (redis_errors.ConnectionError, redis_errors.TimeoutError, OSError, TimeoutError)


@dataclass(frozen=True, slots=True)
class Decision:
    allowed: bool
    limit: int  # the bucket's capacity (burst): X-RateLimit-Limit
    remaining: int  # whole tokens left: X-RateLimit-Remaining
    retry_after_ms: int  # 0 when allowed

    @property
    def retry_after_seconds(self) -> int:
        """The Retry-After header: whole seconds, at least 1."""
        return max(1, math.ceil(self.retry_after_ms / 1000))


class LocalBuckets:
    """The Lua script's maths in process, used only while Redis is unavailable.

    Each API replica keeps its own buckets, so during an outage a tenant can get up to
    (replicas x) its limit. There is one bucket per tenant and route class, so memory is
    bounded by the number of tenants.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}  # key -> (tokens, last ms)

    def take(self, key: str, *, capacity: int, rate: float, requested: int = 1) -> Decision:
        now = self._clock() * 1000
        tokens, ts = self._buckets.get(key, (float(capacity), now))
        tokens = min(capacity, tokens + max(0.0, now - ts) * rate / 1000)
        retry_ms = 0
        if tokens >= requested:
            tokens -= requested
        else:
            retry_ms = math.ceil((requested - tokens) * 1000 / rate)
        self._buckets[key] = (tokens, now)
        return Decision(retry_ms == 0, capacity, math.floor(tokens), retry_ms)


class RateLimiter:
    def __init__(
        self,
        redis: redis_asyncio.Redis,
        *,
        namespace: str,
        health: RedisHealth,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        # register_script calls EVALSHA, and loads the script again if Redis lost it.
        self._script = redis.register_script(TOKEN_BUCKET_LUA)
        self._namespace = namespace
        self._health = health
        self._local = LocalBuckets(clock)

    async def take(
        self,
        bucket: str,
        *,
        capacity: int,
        rate: float,
        requested: int = 1,
        now_ms: int | None = None,
    ) -> Decision:
        """Take `requested` tokens from the bucket, which holds `capacity` and refills at
        `rate` per second. `now_ms` pins the clock, for tests only."""
        if capacity < 1 or rate <= 0:
            raise ValueError("a bucket needs capacity >= 1 and rate > 0")
        key = f"{self._namespace}:rl:{bucket}"
        if self._health.usable():
            args: list[float | int] = [capacity, rate, requested]
            if now_ms is not None:
                args.append(now_ms)
            try:
                allowed, remaining, retry_ms = await self._script(keys=[key], args=args)
            except REDIS_DOWN as exc:
                self._health.failed("rate_limit", exc)
            else:
                return Decision(bool(allowed), capacity, int(remaining), int(retry_ms))
        RATELIMIT_FALLBACK.inc()
        return self._local.take(key, capacity=capacity, rate=rate, requested=requested)
