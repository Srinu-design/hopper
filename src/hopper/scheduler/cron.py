import asyncio
import contextlib

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.metrics import JOBS_ENQUEUED, queue_label, task_label
from hopper.queue.schedules import fire_due

log = structlog.get_logger()


class CronLoop:
    """Turns due schedules into jobs, every `interval` seconds.

    Safe to run in several scheduler replicas at once with no leader election: each due
    schedule row is locked by SKIP LOCKED and handed to exactly one of them.
    """

    def __init__(
        self, engine: AsyncEngine, *, interval: float = 1.0, batch_size: int = 100
    ) -> None:
        self._engine = engine
        self._interval = interval
        self._batch_size = batch_size
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        log.info("cron_started", interval=self._interval, batch_size=self._batch_size)
        while not self._stopping.is_set():
            try:
                await self.tick()
            except Exception:
                log.exception("cron_tick_failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=self._interval)
        log.info("cron_stopped")

    async def tick(self) -> int:
        """Fire everything due now, in batches. Returns how many schedules fired."""
        total = 0
        while True:
            fired = await fire_due(self._engine, limit=self._batch_size)
            for f in fired:
                if f.job_id is not None:
                    JOBS_ENQUEUED.labels(queue_label(f.queue), task_label(f.task)).inc()
                log.info(
                    "schedule_fired",
                    schedule_id=str(f.schedule_id),
                    tenant_id=str(f.tenant_id),
                    fire_time=f.fire_time.isoformat(),
                    job_id=str(f.job_id) if f.job_id else None,
                    next_run_at=f.next_run_at.isoformat() if f.next_run_at else None,
                )
            total += len(fired)
            if len(fired) < self._batch_size:
                return total
