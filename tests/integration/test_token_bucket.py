"""The Lua token bucket against a real Redis: exact under concurrency, Retry-After from the refill
maths, refill, expiry, the Redis clock, and failing open when Redis is down."""

import asyncio
import math
import os
import uuid
from collections.abc import AsyncIterator, Callable

import pytest
import redis.asyncio as redis_asyncio
from prometheus_client import REGISTRY
from redis import exceptions as redis_errors

from hopper.config import Settings, get_settings
from hopper.db import create_redis
from hopper.ratelimit.limiter import RateLimiter
from hopper.ratelimit.redis_health import RedisHealth


@pytest.fixture
async def namespace() -> AsyncIterator[str]:
    ns = f"test-{uuid.uuid4().hex[:12]}"
    yield ns
    client = redis_asyncio.from_url(os.environ["REDIS_URL"])
    try:
        async for key in client.scan_iter(match=f"{ns}:*"):
            await client.delete(key)
    finally:
        await client.aclose()


@pytest.fixture
async def make_limiter(namespace: str) -> AsyncIterator[Callable[[], RateLimiter]]:
    """Each call is one more API replica: its own Redis client and limiter, one shared Redis."""
    clients: list[redis_asyncio.Redis] = []

    def make() -> RateLimiter:
        client = create_redis(get_settings())  # the production client: blocking pool, timeouts
        clients.append(client)
        return RateLimiter(client, namespace=namespace, health=RedisHealth(5.0))

    yield make
    for client in clients:
        await client.aclose()


def fallbacks() -> float:
    return REGISTRY.get_sample_value("hopper_ratelimit_fallback_total") or 0.0


async def test_burst_is_exact_under_concurrency_across_replicas(
    make_limiter: Callable[[], RateLimiter],
) -> None:
    """500 concurrent takes from three replicas on one bucket of 100 with almost no refill:
    exactly 100 succeed. A read-then-write limiter lets more through here."""
    replicas = [make_limiter() for _ in range(3)]
    before = fallbacks()
    decisions = await asyncio.gather(
        *(replicas[i % 3].take("tenant:enqueue", capacity=100, rate=0.001) for i in range(500))
    )
    assert fallbacks() == before  # every decision came from the Lua script
    assert sum(d.allowed for d in decisions) == 100
    assert sorted(d.remaining for d in decisions if d.allowed) == list(range(100))


async def test_retry_after_matches_the_refill_maths(
    make_limiter: Callable[[], RateLimiter],
) -> None:
    limiter = make_limiter()
    for _ in range(10):
        assert (await limiter.take("b", capacity=10, rate=2, now_ms=0)).allowed
    empty = await limiter.take("b", capacity=10, rate=2, now_ms=0)
    assert (empty.allowed, empty.remaining, empty.retry_after_ms) == (False, 0, 500)
    assert empty.retry_after_seconds == 1
    later = await limiter.take("b", capacity=10, rate=2, now_ms=300)  # 0.6 back, 0.4 to go
    assert (later.allowed, later.retry_after_ms) == (False, 200)
    assert (await limiter.take("b", capacity=10, rate=2, now_ms=500)).allowed


async def test_a_slow_refill_gives_a_long_retry_after(
    make_limiter: Callable[[], RateLimiter],
) -> None:
    limiter = make_limiter()
    await limiter.take("slow", capacity=1, rate=0.25, now_ms=0)
    refused = await limiter.take("slow", capacity=1, rate=0.25, now_ms=0)
    assert (refused.retry_after_ms, refused.retry_after_seconds) == (4000, 4)


async def test_tokens_refill_at_the_rate_and_stop_at_capacity(
    make_limiter: Callable[[], RateLimiter],
) -> None:
    limiter = make_limiter()
    for _ in range(10):
        await limiter.take("r", capacity=10, rate=5, now_ms=0)
    after_one_second = [
        (await limiter.take("r", capacity=10, rate=5, now_ms=1000)).allowed for _ in range(6)
    ]
    assert after_one_second == [True] * 5 + [False]
    after_an_hour = [
        (await limiter.take("r", capacity=10, rate=5, now_ms=3_601_000)).allowed for _ in range(11)
    ]
    assert after_an_hour == [True] * 10 + [False]


async def test_buckets_are_separate_per_key(make_limiter: Callable[[], RateLimiter]) -> None:
    limiter = make_limiter()
    assert (await limiter.take("a:enqueue", capacity=1, rate=0.001)).allowed
    assert not (await limiter.take("a:enqueue", capacity=1, rate=0.001)).allowed
    assert (await limiter.take("a:read", capacity=1, rate=0.001)).allowed
    assert (await limiter.take("b:enqueue", capacity=1, rate=0.001)).allowed


async def test_an_idle_bucket_expires_once_it_would_be_full_again(
    make_limiter: Callable[[], RateLimiter], namespace: str
) -> None:
    await make_limiter().take("e", capacity=100, rate=50)
    client = redis_asyncio.from_url(os.environ["REDIS_URL"])
    try:
        ttl_ms = await client.pttl(f"{namespace}:rl:e")
    finally:
        await client.aclose()
    assert 0 < ttl_ms <= math.ceil(100 / 50 * 1000) * 2  # twice the 2 s full refill


async def test_the_bucket_runs_on_the_redis_clock(
    make_limiter: Callable[[], RateLimiter], namespace: str
) -> None:
    await make_limiter().take("clock", capacity=5, rate=1)
    client = redis_asyncio.from_url(os.environ["REDIS_URL"])
    try:
        stored = int(float(await client.hget(f"{namespace}:rl:clock", "ts")))
        seconds, micros = await client.time()
    finally:
        await client.aclose()
    assert abs(seconds * 1000 + micros // 1000 - stored) < 1000


async def test_redis_down_fails_open_to_an_in_process_bucket(namespace: str) -> None:
    unreachable = create_redis(Settings(redis_url="redis://127.0.0.1:1/0"))
    limiter = RateLimiter(unreachable, namespace=namespace, health=RedisHealth(5.0))
    before = fallbacks()
    try:
        decisions = [await limiter.take("down", capacity=3, rate=0.001) for _ in range(4)]
    finally:
        await unreachable.aclose()
    # Still limited, just locally: the burst passes, then 429 as usual.
    assert [d.allowed for d in decisions] == [True, True, True, False]
    assert fallbacks() - before == 4


class FakeScript:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls = 0

    async def __call__(self, **_: object) -> list[int]:
        self.calls += 1
        raise self.error


async def test_a_down_redis_is_tried_once_per_retry_period(namespace: str) -> None:
    now = [0.0]
    client = redis_asyncio.from_url(os.environ["REDIS_URL"])
    limiter = RateLimiter(
        client, namespace=namespace, health=RedisHealth(5.0, clock=lambda: now[0])
    )
    script = FakeScript(redis_errors.ConnectionError("refused"))
    limiter._script = script  # type: ignore[assignment]
    try:
        for _ in range(20):
            await limiter.take("x", capacity=100, rate=1)
        assert script.calls == 1  # not one failed call per request
        now[0] += 5.0
        await limiter.take("x", capacity=100, rate=1)
        assert script.calls == 2
    finally:
        await client.aclose()


async def test_a_script_error_is_a_bug_and_is_not_swallowed(namespace: str) -> None:
    client = redis_asyncio.from_url(os.environ["REDIS_URL"])
    limiter = RateLimiter(client, namespace=namespace, health=RedisHealth(5.0))
    limiter._script = FakeScript(redis_errors.ResponseError("ERR bad script"))  # type: ignore[assignment]
    try:
        with pytest.raises(redis_errors.ResponseError):
            await limiter.take("x", capacity=1, rate=1)
    finally:
        await client.aclose()


@pytest.mark.parametrize(("capacity", "rate"), [(0, 1.0), (1, 0.0), (1, -1.0)])
async def test_nonsense_limits_are_refused_before_redis(
    make_limiter: Callable[[], RateLimiter], capacity: int, rate: float
) -> None:
    with pytest.raises(ValueError, match="capacity"):
        await make_limiter().take("x", capacity=capacity, rate=rate)
