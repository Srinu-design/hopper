import asyncio
import contextlib
from collections import defaultdict
from dataclasses import dataclass
from uuid import UUID

import redis.asyncio as redis_asyncio
import structlog
from redis.typing import EncodableT, FieldT
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.metrics import DEPTH_STATES, OLDEST_READY_AGE, QUEUE_DEPTH, known_queues, queue_label
from hopper.queue import sql
from hopper.ratelimit.backpressure import TOTAL_FIELD, depth_key
from hopper.ratelimit.limiter import REDIS_DOWN

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class Depth:
    by_queue: dict[tuple[str, str], int]  # (queue label, state) -> jobs
    oldest_ready_seconds: dict[str, float]  # queue label -> age of its oldest ready job
    queued_by_tenant: dict[UUID, int]  # ready + delayed, the jobs backpressure counts
    total_queued: int


async def count_depth(engine: AsyncEngine) -> Depth:
    """Count every queue, by state and by tenant. Every known queue and state is present,
    zero when empty, so a gauge drops to 0 instead of keeping its last value."""
    async with engine.connect() as conn:
        queued = (await conn.execute(text(sql.QUEUED_DEPTH))).all()
        others = (await conn.execute(text(sql.RUNNING_AND_DEAD_DEPTH))).all()
    queues = known_queues()
    by_queue = {(q, state): 0 for q in queues for state in DEPTH_STATES}
    oldest = dict.fromkeys(queues, 0.0)
    by_tenant: defaultdict[UUID, int] = defaultdict(int)
    for row in queued:
        q = queue_label(row.queue)
        by_queue[(q, "ready")] += row.ready
        by_queue[(q, "delayed")] += row.delayed
        oldest[q] = max(oldest[q], float(row.oldest_ready_seconds))
        by_tenant[row.tenant_id] += row.ready + row.delayed
    for row in others:
        by_queue[(queue_label(row.queue), row.state)] += row.jobs
    return Depth(by_queue, oldest, dict(by_tenant), sum(by_tenant.values()))


class DepthLoop:
    """Counts queue depth every `interval` seconds, sets the depth gauges, and publishes
    queued jobs per tenant (and in total) to Redis for the API's backpressure (ADR-0009).

    Both scheduler replicas run it and both report: the dashboard takes max by queue and
    state. The Redis hash is replaced in one MULTI, so a tenant whose queue drained loses its
    field at once, and it expires if no scheduler refreshes it, after which the API stops
    applying depth limits (fail open) rather than trusting a count that no longer moves.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        redis: redis_asyncio.Redis,
        *,
        namespace: str,
        interval: float = 1.0,
    ) -> None:
        self._engine = engine
        self._redis = redis
        self._key = depth_key(namespace)
        self._interval = interval
        self._ttl_ms = int(max(15.0, 3 * interval) * 1000)
        self._stopping = asyncio.Event()
        self._redis_down = False

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        log.info("depth_started", interval=self._interval)
        while not self._stopping.is_set():
            try:
                await self.tick()
            except Exception:
                log.exception("depth_tick_failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=self._interval)
        log.info("depth_stopped")

    async def tick(self) -> Depth:
        depth = await count_depth(self._engine)
        for (queue, state), jobs in depth.by_queue.items():
            QUEUE_DEPTH.labels(queue, state).set(jobs)
        for queue, seconds in depth.oldest_ready_seconds.items():
            OLDEST_READY_AGE.labels(queue).set(seconds)
        await self._publish(depth)
        return depth

    async def _publish(self, depth: Depth) -> None:
        fields: dict[FieldT, EncodableT] = {TOTAL_FIELD: depth.total_queued}
        fields |= {str(tenant): jobs for tenant, jobs in depth.queued_by_tenant.items()}
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.delete(self._key)
                pipe.hset(self._key, mapping=fields)
                pipe.pexpire(self._key, self._ttl_ms)
                await pipe.execute()
        except REDIS_DOWN as exc:
            # The gauges are already set; only the API's view goes stale. Log once per outage.
            if not self._redis_down:
                log.warning("depth_publish_failed", error=repr(exc))
            self._redis_down = True
        else:
            if self._redis_down:
                log.info("depth_publish_recovered")
            self._redis_down = False
