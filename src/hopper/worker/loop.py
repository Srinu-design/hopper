import asyncio
import contextlib
import random
import time

import structlog

from hopper.queue.broker import Broker, ClaimedJob
from hopper.tasks import registry

log = structlog.get_logger()


class Worker:
    """Claims jobs in batches, runs each as its own asyncio task, acks only after success.

    Intake is bounded by `slots`: the claim LIMIT is always the number of free slots, so the
    worker never holds more jobs than it can run. A failed handler is never acked (at-least-once);
    retries, leases and release-on-shutdown arrive in later weeks.
    """

    def __init__(
        self,
        broker: Broker,
        *,
        worker_id: str,
        queues: list[str],
        slots: int = 20,
        poll_interval: float = 0.25,
        max_idle_backoff: float = 0.5,
        lease_seconds: float = 30.0,
    ) -> None:
        if not queues:
            raise ValueError("a worker needs at least one queue")
        self._broker = broker
        self.worker_id = worker_id
        self._queues = queues
        self._slots = slots
        self._poll_interval = poll_interval
        self._max_idle_backoff = max_idle_backoff
        self._lease_seconds = lease_seconds
        self._inflight: set[asyncio.Task[None]] = set()
        self._stopping = asyncio.Event()
        self._slot_freed = asyncio.Event()

    @property
    def inflight(self) -> int:
        return len(self._inflight)

    def stop(self) -> None:
        """Stop claiming. run() returns once in-flight jobs have finished."""
        self._stopping.set()

    async def run(self) -> None:
        log.info("worker_started", worker_id=self.worker_id, slots=self._slots)
        idle_delay = self._poll_interval
        while not self._stopping.is_set():
            free = self._slots - len(self._inflight)
            if free <= 0:
                await self._wait_for_free_slot()
                continue
            try:
                claimed, saturated = await self._claim(free)
            except Exception:
                log.exception("claim_failed")
                await self._sleep(min(self._max_idle_backoff, 1.0))
                continue
            if claimed and saturated:
                idle_delay = self._poll_interval
                continue  # the queue still has work: claim again at once
            if claimed:
                idle_delay = self._poll_interval
            else:
                # Back off while idle so N workers do not hammer an empty queue.
                idle_delay = min(self._max_idle_backoff, idle_delay * 1.5)
            await self._sleep(idle_delay)
        if self._inflight:
            log.info("worker_draining", inflight=len(self._inflight))
            await asyncio.gather(*self._inflight, return_exceptions=True)
        log.info("worker_stopped", worker_id=self.worker_id)

    async def _claim(self, free: int) -> tuple[int, bool]:
        """Claim across queues. Returns (jobs claimed, whether any queue filled its batch).

        Queues are tried in the configured order, so an always-busy first queue can starve
        the later ones on this worker; run separate workers per queue if that matters.
        """
        total, saturated = 0, False
        for queue in self._queues:
            if free <= 0:
                break
            requested = free
            jobs = await self._broker.claim(queue, self.worker_id, requested, self._lease_seconds)
            for job in jobs:
                self._spawn(job)
            total += len(jobs)
            free -= len(jobs)
            # A full batch means the queue probably has more ready work.
            saturated = saturated or len(jobs) == requested
        return total, saturated

    def _spawn(self, job: ClaimedJob) -> None:
        task = asyncio.create_task(self._run_job(job), name=f"job-{job.id}")
        self._inflight.add(task)
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        self._inflight.discard(task)
        self._slot_freed.set()

    async def _wait_for_free_slot(self) -> None:
        self._slot_freed.clear()
        if len(self._inflight) < self._slots:
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._slot_freed.wait(), timeout=self._poll_interval * 4)

    async def _sleep(self, delay: float) -> None:
        """Sleep with jitter; wakes early on stop."""
        jittered = delay * random.uniform(0.5, 1.5)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=jittered)

    async def _run_job(self, job: ClaimedJob) -> None:
        jlog = log.bind(
            job_id=str(job.id),
            tenant_id=str(job.tenant_id),
            attempt=job.attempt,
            worker_id=self.worker_id,
        )
        spec = registry.get_task(job.task)
        if spec is None:
            jlog.error("unknown_task", task=job.task)
            return
        started = time.monotonic()
        try:
            payload = spec.payload_model.model_validate(job.payload)
            result = await asyncio.wait_for(spec.handler(payload), timeout=job.timeout_seconds)
        except Exception as exc:
            # Never ack a failed run. Week 3 turns this into a retry or a dead-letter.
            jlog.warning("job_failed", task=job.task, error=repr(exc))
            return
        try:
            acked = await self._broker.ack(job, self.worker_id, result)
        except Exception:
            jlog.exception("ack_failed", task=job.task)
            return
        elapsed = round(time.monotonic() - started, 4)
        if acked:
            jlog.info("job_succeeded", task=job.task, seconds=elapsed)
        else:
            jlog.warning("ack_rejected_lease_lost", task=job.task, seconds=elapsed)
