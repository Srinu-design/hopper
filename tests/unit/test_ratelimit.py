"""The in-process fallback bucket and the Redis cool-down, with a fake clock."""

from typing import Any, cast

import pytest
from redis import exceptions as redis_errors

from hopper.ratelimit.limiter import Decision, LocalBuckets, RateLimiter
from hopper.ratelimit.redis_health import RedisHealth


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.mark.parametrize(
    ("retry_ms", "header"), [(1, 1), (500, 1), (1000, 1), (1001, 2), (4000, 4), (4001, 5)]
)
def test_retry_after_header_rounds_up_to_whole_seconds(retry_ms: int, header: int) -> None:
    assert Decision(False, 10, 0, retry_ms).retry_after_seconds == header


def test_a_new_bucket_starts_full_and_allows_exactly_the_burst() -> None:
    buckets = LocalBuckets(Clock())
    decisions = [buckets.take("k", capacity=5, rate=1) for _ in range(6)]
    assert [d.allowed for d in decisions] == [True] * 5 + [False]
    assert [d.remaining for d in decisions] == [4, 3, 2, 1, 0, 0]


def test_retry_after_is_the_time_to_refill_one_token() -> None:
    clock = Clock()
    buckets = LocalBuckets(clock)
    for _ in range(10):
        buckets.take("k", capacity=10, rate=2)
    assert buckets.take("k", capacity=10, rate=2).retry_after_ms == 500  # 1 token / 2 per s
    clock.now += 0.3  # 0.6 tokens back: 0.4 to go at 2 per s
    assert buckets.take("k", capacity=10, rate=2).retry_after_ms == 200


def test_tokens_refill_at_the_rate_and_stop_at_capacity() -> None:
    clock = Clock()
    buckets = LocalBuckets(clock)
    for _ in range(10):
        buckets.take("k", capacity=10, rate=5)
    clock.now += 1.0  # 5 tokens back
    assert [buckets.take("k", capacity=10, rate=5).allowed for _ in range(6)] == [True] * 5 + [
        False
    ]
    clock.now += 3600  # an hour idle refills to capacity, not beyond
    assert sum(buckets.take("k", capacity=10, rate=5).allowed for _ in range(20)) == 10


def test_buckets_are_independent() -> None:
    buckets = LocalBuckets(Clock())
    assert not [buckets.take("a", capacity=1, rate=1) for _ in range(2)][-1].allowed
    assert buckets.take("b", capacity=1, rate=1).allowed


def test_redis_is_skipped_for_the_retry_period_after_a_failure() -> None:
    clock = Clock()
    health = RedisHealth(5.0, clock)
    assert health.usable()
    health.failed("test", ConnectionError("down"))
    assert not health.usable()
    clock.now += 4.9
    assert not health.usable()
    clock.now += 0.1
    assert health.usable()


def test_refilled_buckets_are_forgotten_once_there_are_many() -> None:
    """Login buckets are per email, so anyone can make new ones during a Redis outage."""
    clock = Clock()
    buckets = LocalBuckets(clock, prune_at=3)
    for key in ("a", "b", "c"):
        buckets.take(key, capacity=2, rate=1)  # one token short: full again in 1 s
    clock.now += 2
    buckets.take("d", capacity=2, rate=1)
    assert len(buckets) == 1  # a, b and c had refilled, which is the same as no bucket


def test_buckets_still_refilling_are_kept() -> None:
    clock = Clock()
    buckets = LocalBuckets(clock, prune_at=2)
    for key in ("a", "b", "c"):
        buckets.take(key, capacity=2, rate=1)
        buckets.take(key, capacity=2, rate=1)  # empty
    assert len(buckets) == 3
    assert not buckets.take("a", capacity=2, rate=1).allowed  # still empty, not forgotten


class FlakyRedis:
    """Just enough of redis.asyncio.Redis for RateLimiter: a script that fails while down."""

    def __init__(self) -> None:
        self.down = True

    def register_script(self, _: str) -> Any:
        async def run(*, keys: list[str], args: list[float]) -> list[int]:
            if self.down:
                raise redis_errors.ConnectionError("down")
            return [1, 0, 0]

        return run


async def test_the_fallback_starts_afresh_after_redis_comes_back() -> None:
    redis = FlakyRedis()
    limiter = RateLimiter(cast(Any, redis), namespace="t", health=RedisHealth(0.0, Clock()))
    bucket = {"capacity": 1, "rate": 0.001}  # one token, and it takes 1000 s to come back
    assert (await limiter.take("k", **bucket)).allowed  # Redis down: the in-process bucket
    assert not (await limiter.take("k", **bucket)).allowed
    redis.down = False
    assert (await limiter.take("k", **bucket)).allowed  # Redis decides again
    redis.down = True
    assert (await limiter.take("k", **bucket)).allowed  # a new outage starts with full buckets
