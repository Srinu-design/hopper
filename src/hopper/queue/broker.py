from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from uuid import UUID

FailureOutcome = Literal["failed", "timed_out"]


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    """A job a worker holds under a lease. lease_token fences every later write for this run."""

    id: UUID
    tenant_id: UUID
    queue: str
    task: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    timeout_seconds: int
    lease_token: UUID
    # The tenant's key for signing http task requests; never logged or printed.
    signing_secret: bytes = field(default=b"", repr=False)
    # The enqueue call's X-Request-ID (None for cron jobs), for log correlation.
    request_id: str | None = None
    # How long the job waited between becoming ready (run_at) and this claim.
    wait_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class ReapedJob:
    """A job whose lease ran out, as the reaper left it: 'queued' again, or 'dead'."""

    id: UUID
    tenant_id: UUID
    queue: str
    task: str
    status: str
    attempt: int
    lease_owner: str
    request_id: str | None = None


class Broker(Protocol):
    """What a worker needs from a queue backend."""

    async def claim(
        self, queue: str, worker_id: str, limit: int, lease_seconds: float
    ) -> list[ClaimedJob]:
        """Atomically take up to `limit` ready jobs; no job is returned to two callers."""
        ...

    async def heartbeat(self, jobs: Sequence[ClaimedJob], lease_seconds: float) -> set[UUID]:
        """Extend the lease on each job. Returns the ids whose lease is still held."""
        ...

    async def ack(self, job: ClaimedJob, worker_id: str, result: dict[str, Any] | None) -> bool:
        """Mark the job succeeded. False means the lease was lost and nothing changed."""
        ...

    async def nack(
        self,
        job: ClaimedJob,
        worker_id: str,
        *,
        error: str,
        outcome: FailureOutcome,
        delay_seconds: float,
        permanent: bool,
    ) -> str | None:
        """Record a failed run. Returns the new status ('queued' to retry after delay_seconds,
        or 'dead'), or None if the lease was lost and nothing changed."""
        ...

    async def release(self, jobs: Sequence[ClaimedJob], worker_id: str) -> set[UUID]:
        """Hand unfinished jobs back to the queue without using up an attempt.
        Returns the ids released; a job whose lease was already lost is left alone."""
        ...
