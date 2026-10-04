import asyncio
import contextlib

import structlog

from hopper.metrics import JOB_ATTEMPTS, JOBS_DEAD, LEASES_RECLAIMED, queue_label, task_label
from hopper.queue.postgres import PostgresBroker

log = structlog.get_logger()


class Reaper:
    """Requeues jobs whose lease ran out: their worker crashed, was killed or stalled.

    Runs every `interval` seconds, so a dead worker's job is claimable again within
    lease + interval (30 s + 5 s by default). Each batch is one short transaction with
    SKIP LOCKED, so running two schedulers is safe: each expired job is reaped once.
    """

    def __init__(
        self, broker: PostgresBroker, *, interval: float = 5.0, batch_size: int = 500
    ) -> None:
        self._broker = broker
        self._interval = interval
        self._batch_size = batch_size
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        log.info("reaper_started", interval=self._interval, batch_size=self._batch_size)
        while not self._stopping.is_set():
            try:
                await self.reap_once()
            except Exception:
                log.exception("reap_failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=self._interval)
        log.info("reaper_stopped")

    async def reap_once(self) -> int:
        """Reclaim every lease that has expired by now, in batches. Returns how many."""
        total = 0
        while True:
            reaped = await self._broker.reap(self._batch_size)
            for job in reaped:
                queue, task = queue_label(job.queue), task_label(job.task)
                JOB_ATTEMPTS.labels(queue, task, "lease_expired").inc()
                LEASES_RECLAIMED.labels(queue).inc()
                if job.status == "dead":
                    JOBS_DEAD.labels(queue, task).inc()
                # dead here means a poison pill: it took its worker down on every attempt.
                emit = log.warning if job.status == "dead" else log.info
                emit(
                    "lease_reclaimed",
                    job_id=str(job.id),
                    tenant_id=str(job.tenant_id),
                    request_id=job.request_id,
                    queue=job.queue,
                    task=job.task,
                    attempt=job.attempt,
                    worker_id=job.lease_owner,  # the worker that lost the lease
                    status=job.status,
                )
            total += len(reaped)
            if len(reaped) < self._batch_size:
                return total
