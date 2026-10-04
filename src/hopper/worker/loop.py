import asyncio
import contextlib
import random
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import structlog
from pydantic import ValidationError
from structlog.typing import FilteringBoundLogger

from hopper.metrics import (
    ACKS_REJECTED,
    JOB_ATTEMPTS,
    JOB_RUN_SECONDS,
    JOB_WAIT_SECONDS,
    JOBS_DEAD,
    WORKER_INFLIGHT,
    queue_label,
    task_label,
)
from hopper.queue.broker import Broker, ClaimedJob, FailureOutcome
from hopper.tasks import registry
from hopper.tasks.context import JobContext, running
from hopper.tasks.errors import PermanentError, RetryableError
from hopper.worker.backoff import full_jitter_delay

log = structlog.get_logger()

_MAX_ERROR_CHARS = 2000


class UnknownTaskError(Exception):
    pass


class JobTimedOut(Exception):
    pass


@dataclass(eq=False, slots=True)
class _Running:
    """One in-flight job and the asyncio task running it."""

    job: ClaimedJob
    task: asyncio.Task[None]
    # Set once the handler has returned or raised: the ack or nack is under way and the
    # task must not be cancelled any more, or a finished job could be run a second time.
    finishing: bool = False


class Worker:
    """Claims jobs in batches, runs each as its own asyncio task, acks only after success.

    Intake is bounded by `slots`: the claim LIMIT is always the number of free slots, so the
    worker never holds more jobs than it can run. A failed run is never acked (at-least-once):
    it is nacked, which retries it after a full-jitter backoff delay or, for a permanent error
    or the last attempt, moves it to the dead-letter queue.

    Retryable: timeouts, unknown tasks, RetryableError and any unexpected exception.
    Permanent: PermanentError and a payload that fails the task's validation.

    Leases: every `heartbeat_interval` seconds one statement renews the lease on every job
    in flight. A job missing from the answer was reclaimed by the reaper, so its handler is
    cancelled and it is never acked. Handlers must not block the event loop (CPU-bound work
    belongs in asyncio.to_thread): a blocked loop stops heartbeats and the job runs twice.

    Shutdown: stop() stops claiming. run() then waits up to `shutdown_grace` seconds for
    in-flight jobs, cancels the rest and releases them back to the queue without using up
    an attempt.
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
        heartbeat_interval: float | None = None,
        shutdown_grace: float = 25.0,
    ) -> None:
        if not queues:
            raise ValueError("a worker needs at least one queue")
        if heartbeat_interval is None:
            heartbeat_interval = lease_seconds / 3
        if not 0 < heartbeat_interval < lease_seconds:
            raise ValueError("the heartbeat interval must be shorter than the lease")
        self._broker = broker
        self.worker_id = worker_id
        self._queues = queues
        self._slots = slots
        self._poll_interval = poll_interval
        self._max_idle_backoff = max_idle_backoff
        self._lease_seconds = lease_seconds
        self._heartbeat_interval = heartbeat_interval
        self._shutdown_grace = shutdown_grace
        self._running: dict[UUID, _Running] = {}
        self._stopping = asyncio.Event()
        self._slot_freed = asyncio.Event()

    @property
    def inflight(self) -> int:
        return len(self._running)

    def stop(self) -> None:
        """Stop claiming. run() returns once in-flight jobs have finished or been released."""
        self._stopping.set()

    async def run(self) -> None:
        log.info(
            "worker_started",
            worker_id=self.worker_id,
            slots=self._slots,
            lease_seconds=self._lease_seconds,
            heartbeat_seconds=self._heartbeat_interval,
        )
        heartbeats = asyncio.create_task(self._heartbeat_loop(), name="heartbeats")
        try:
            await self._claim_until_stopped()
            await self._drain()
        finally:
            # Heartbeats run until here, so leases stay valid for the whole drain.
            heartbeats.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeats
        log.info("worker_stopped", worker_id=self.worker_id)

    async def _claim_until_stopped(self) -> None:
        idle_delay = self._poll_interval
        while not self._stopping.is_set():
            free = self._slots - len(self._running)
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

    async def _drain(self) -> None:
        """Wait up to the grace period for in-flight jobs, then cancel and release the rest."""
        if not self._running:
            return
        log.info("worker_draining", inflight=len(self._running), grace=self._shutdown_grace)
        tasks = [r.task for r in self._running.values()]
        _, pending = await asyncio.wait(tasks, timeout=self._shutdown_grace)
        if not pending:
            return
        # Jobs already acking or nacking are left to finish; only running handlers are cut off.
        unfinished = [r for r in self._running.values() if not r.finishing and not r.task.done()]
        for r in unfinished:
            r.task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        jobs = [r.job for r in unfinished]
        try:
            released = await self._broker.release(jobs, self.worker_id)
        except Exception:
            # Nothing is lost: the leases run out and the reaper requeues these jobs.
            log.exception("release_failed", jobs=len(jobs))
            return
        for job in jobs:
            jlog = _job_logger(job, self.worker_id)
            if job.id in released:
                JOB_ATTEMPTS.labels(queue_label(job.queue), task_label(job.task), "released").inc()
                jlog.info("job_released")
            else:
                jlog.warning("release_rejected_lease_lost")

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                await self.heartbeat()
            except Exception:
                log.exception("heartbeat_loop_error")

    async def heartbeat(self) -> None:
        """Renew every in-flight lease in one statement; cancel the jobs whose lease was lost."""
        held = list(self._running.values())
        if not held:
            return
        try:
            renewed = await self._broker.heartbeat([r.job for r in held], self._lease_seconds)
        except Exception:
            # Try again next tick; a lease survives two missed heartbeats (30 s vs 10 s).
            log.exception("heartbeat_failed", jobs=len(held))
            return
        for r in held:
            if r.job.id in renewed or r.finishing or r.task.done():
                continue
            # Reclaimed by the reaper, maybe already running elsewhere: stop work, never ack.
            r.task.cancel()
            log.warning(
                "lease_lost_job_cancelled",
                job_id=str(r.job.id),
                attempt=r.job.attempt,
                worker_id=self.worker_id,
                task=r.job.task,
            )

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
        self._running[job.id] = _Running(job, task)
        WORKER_INFLIGHT.set(len(self._running))
        task.add_done_callback(lambda _: self._on_done(job.id))

    def _on_done(self, job_id: UUID) -> None:
        self._running.pop(job_id, None)
        WORKER_INFLIGHT.set(len(self._running))
        self._slot_freed.set()

    def _mark_finishing(self, job: ClaimedJob) -> None:
        running = self._running.get(job.id)
        if running is not None:
            running.finishing = True

    async def _wait_for_free_slot(self) -> None:
        self._slot_freed.clear()
        if len(self._running) < self._slots:
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
        jlog = _job_logger(job, self.worker_id)
        queue, task = queue_label(job.queue), task_label(job.task)
        JOB_WAIT_SECONDS.labels(queue).observe(job.wait_seconds)
        jlog.info("job_claimed", waited=round(job.wait_seconds, 4))
        spec = registry.get_task(job.task)
        started = time.monotonic()
        try:
            result = await self._execute(job, spec)
        except Exception as exc:
            self._mark_finishing(job)
            JOB_RUN_SECONDS.labels(queue, task).observe(time.monotonic() - started)
            await self._fail(job, spec, exc, jlog)
            return
        self._mark_finishing(job)
        elapsed = time.monotonic() - started
        JOB_RUN_SECONDS.labels(queue, task).observe(elapsed)
        try:
            acked = await self._broker.ack(job, self.worker_id, result)
        except Exception:
            # The job stays running without heartbeats; the reaper requeues it.
            jlog.exception("ack_failed")
            return
        if acked:
            JOB_ATTEMPTS.labels(queue, task, "succeeded").inc()
            jlog.info("job_succeeded", seconds=round(elapsed, 4))
        else:
            ACKS_REJECTED.inc()
            jlog.warning("ack_rejected_lease_lost", seconds=round(elapsed, 4))

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
        context = JobContext(
            job.id, job.tenant_id, job.attempt, job.max_attempts, job.signing_secret
        )
        deadline = asyncio.timeout(job.timeout_seconds)
        try:
            with running(context):
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
            ACKS_REJECTED.inc()
            jlog.warning("nack_rejected_lease_lost", error=error)
            return
        queue, task = queue_label(job.queue), task_label(job.task)
        JOB_ATTEMPTS.labels(queue, task, outcome).inc()
        if status == "dead":
            JOBS_DEAD.labels(queue, task).inc()
            jlog.warning("job_dead", outcome=outcome, permanent=permanent, error=error)
        else:
            jlog.info("job_retry_scheduled", outcome=outcome, delay=round(delay, 3), error=error)


def _job_logger(job: ClaimedJob, worker_id: str) -> FilteringBoundLogger:
    """Every line about a job carries the ids needed to follow it: the job, its tenant, the
    attempt, this worker, and the request id of the API call that enqueued it."""
    bound: FilteringBoundLogger = log.bind(
        job_id=str(job.id),
        tenant_id=str(job.tenant_id),
        attempt=job.attempt,
        worker_id=worker_id,
        queue=job.queue,
        task=job.task,
        request_id=job.request_id,
    )
    return bound
