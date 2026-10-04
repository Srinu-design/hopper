from dataclasses import dataclass
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


class Broker(Protocol):
    """What a worker needs from a queue backend.

    Heartbeat and release join this interface in Week 4.
    """

    async def claim(
        self, queue: str, worker_id: str, limit: int, lease_seconds: float
    ) -> list[ClaimedJob]:
        """Atomically take up to `limit` ready jobs; no job is returned to two callers."""
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
