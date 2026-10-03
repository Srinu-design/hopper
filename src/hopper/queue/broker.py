from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID


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

    Heartbeat, nack and release join this interface in the weeks that introduce them.
    """

    async def claim(
        self, queue: str, worker_id: str, limit: int, lease_seconds: float
    ) -> list[ClaimedJob]:
        """Atomically take up to `limit` ready jobs; no job is returned to two callers."""
        ...

    async def ack(self, job: ClaimedJob, worker_id: str, result: dict[str, Any] | None) -> bool:
        """Mark the job succeeded. False means the lease was lost and nothing changed."""
        ...
