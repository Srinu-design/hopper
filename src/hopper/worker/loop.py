import asyncio
import contextlib
import random
import time
from typing import Any

import structlog
from pydantic import ValidationError
from structlog.typing import FilteringBoundLogger

from hopper.queue.broker import Broker, ClaimedJob, FailureOutcome
from hopper.tasks import registry
from hopper.tasks.errors import PermanentError, RetryableError
from hopper.worker.backoff import full_jitter_delay

log = structlog.get_logger()

_MAX_ERROR_CHARS = 2000


class UnknownTaskError(Exception):
    pass


class JobTimedOut(Exception):
    pass


class Worker:
    """Claims jobs in batches, runs each as its own asyncio task, acks only after success.

    Intake is bounded by `slots`: the claim LIMIT is always the number of free slots, so the
    worker never holds more jobs than it can run. A failed run is never acked (at-least-once):
    it is nacked, which retries it after a full-jitter backoff delay or, for a permanent error
    or the last attempt, moves it to the dead-letter queue. Leases and release-on-shutdown
    arrive in Week 4.

    Retryable: timeouts, unknown tasks, RetryableError and any unexpected exception.
    Permanent: PermanentError and a payload that fails the task's validation.
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
        task = asyncio.create_task(self.process(job), name=f"job-{job.id}")
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

    async def process(self, job: ClaimedJob) -> None:
        """Run one claimed job to the end: ack on success, nack (retry or DLQ) on failure."""
        jlog = log.bind(
            job_id=str(job.id),
            tenant_id=str(job.tenant_id),
            attempt=job.attempt,
            worker_id=self.worker_id,
            task=job.task,
        )
        spec = registry.get_task(job.task)
        started = time.monotonic()
        try:
            result = await self._execute(job, spec)
        except Exception as exc:
            await self._fail(job, spec, exc, jlog)
            return
        try:
            acked = await self._broker.ack(job, self.worker_id, result)
        except Exception:
            # The job stays running; lease expiry (Week 4) hands it to another worker.
            jlog.exception("ack_failed")
            return
        elapsed = round(time.monotonic() - started, 4)
        if acked:
            jlog.info("job_succeeded", seconds=elapsed)
        else:
            jlog.warning("ack_rejected_lease_lost", seconds=elapsed)

    async def _execute(
        self, job: ClaimedJob, spec: registry.TaskSpec | None
    ) -> dict[str, Any] | None:
        if spec is None:
            # Retryable: during a rolling deploy an older worker can claim a newer task.
            raise UnknownTaskError(f"no handler registered for task {job.task!r}")
        try:
            payload = spec.payload_model.model_validate(job.payload)
        except ValidationError as exc:
            first = exc.errors()[0]
            where = ".".join(str(part) for part in first["loc"]) or "payload"
            raise PermanentError(f"invalid payload: {where}: {first['msg']}") from exc
        deadline = asyncio.timeout(job.timeout_seconds)
        try:
            async with deadline:
                return await spec.handler(payload)
        except TimeoutError as exc:
            if deadline.expired():  # our deadline, not a TimeoutError raised by the handler
                raise JobTimedOut(f"timed out after {job.timeout_seconds}s") from exc
            raise

    async def _fail(
        self,
        job: ClaimedJob,
        spec: registry.TaskSpec | None,
        exc: Exception,
        jlog: FilteringBoundLogger,
    ) -> None:
        permanent = isinstance(exc, PermanentError)
        outcome: FailureOutcome = "timed_out" if isinstance(exc, JobTimedOut) else "failed"
        base = spec.backoff_base if spec else registry.DEFAULT_BACKOFF_BASE
        cap = spec.backoff_cap if spec else registry.DEFAULT_BACKOFF_CAP
        delay = full_jitter_delay(job.attempt, base, cap)
        if isinstance(exc, RetryableError) and exc.retry_after is not None:
            delay = max(delay, exc.retry_after)
        error = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_CHARS]
        try:
            status = await self._broker.nack(
                job,
                self.worker_id,
                error=error,
                outcome=outcome,
                delay_seconds=delay,
                permanent=permanent,
            )
        except Exception:
            jlog.exception("nack_failed", error=error)
            return
        if status is None:
            jlog.warning("nack_rejected_lease_lost", error=error)
        elif status == "dead":
            jlog.warning("job_dead", outcome=outcome, permanent=permanent, error=error)
        else:
            jlog.info("job_retry_scheduled", outcome=outcome, delay=round(delay, 3), error=error)
