import math
import time
from collections.abc import Callable

import structlog

log = structlog.get_logger()


class RedisHealth:
    """Remembers a recent Redis failure, shared by everything in the API that reads Redis.

    After an error, callers skip Redis for `retry_seconds` and decide in process instead, so
    a dead Redis costs one failed call per period per process, not one per request.
    """

    def __init__(self, retry_seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._retry_seconds = retry_seconds
        self._clock = clock
        self._down_until = -math.inf

    def usable(self) -> bool:
        return self._clock() >= self._down_until

    def failed(self, what: str, exc: BaseException) -> None:
        log.warning("redis_unavailable", during=what, error=repr(exc), retry_in=self._retry_seconds)
        self._down_until = self._clock() + self._retry_seconds
