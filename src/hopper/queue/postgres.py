import json
from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue import sql
from hopper.queue.broker import ClaimedJob, FailureOutcome, ReapedJob

# Parameters go in as JSON text with CAST(... AS jsonb); the asyncpg driver decodes jsonb
# results into Python objects on the way out, so no json.loads is needed on reads.


class PostgresBroker:
    """Broker implementation on a Postgres table claimed with FOR UPDATE SKIP LOCKED."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def claim(
        self, queue: str, worker_id: str, limit: int, lease_seconds: float
    ) -> list[ClaimedJob]:
        if limit <= 0:
            return []
        async with self._engine.begin() as conn:
            rows = await conn.execute(
                text(sql.CLAIM),
                {
                    "queue": queue,
                    "worker_id": worker_id,
                    "limit": limit,
                    "lease_seconds": float(lease_seconds),
                },
            )
            return [
                ClaimedJob(
                    id=r.id,
                    tenant_id=r.tenant_id,
                    queue=r.queue,
                    task=r.task,
                    payload=r.payload,
                    attempt=r.attempts,
                    max_attempts=r.max_attempts,
                    timeout_seconds=r.timeout_seconds,
                    lease_token=r.lease_token,
                    signing_secret=r.signing_secret,
                )
                for r in rows
            ]

    async def ack(self, job: ClaimedJob, worker_id: str, result: dict[str, Any] | None) -> bool:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(sql.ACK),
                    {
                        "id": job.id,
                        "token": job.lease_token,
                        "worker_id": worker_id,
                        "result": json.dumps(result) if result is not None else None,
                    },
                )
            ).first()
        return row is not None

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
        async with self._engine.begin() as conn:
            status: str | None = (
                await conn.execute(
                    text(sql.NACK),
                    {
                        "id": job.id,
                        "token": job.lease_token,
                        "worker_id": worker_id,
                        "error": error,
                        "outcome": outcome,
                        "delay_seconds": float(delay_seconds),
                        "permanent": permanent,
                    },
                )
            ).scalar_one_or_none()
        return status

    async def heartbeat(self, jobs: Sequence[ClaimedJob], lease_seconds: float) -> set[UUID]:
        if not jobs:
            return set()
        async with self._engine.begin() as conn:
            rows = await conn.execute(
                text(sql.HEARTBEAT),
                {
                    "ids": [job.id for job in jobs],
                    "tokens": [job.lease_token for job in jobs],
                    "lease_seconds": float(lease_seconds),
                },
            )
            return set(rows.scalars())

    async def release(self, jobs: Sequence[ClaimedJob], worker_id: str) -> set[UUID]:
        if not jobs:
            return set()
        async with self._engine.begin() as conn:
            rows = await conn.execute(
                text(sql.RELEASE),
                {
                    "ids": [job.id for job in jobs],
                    "tokens": [job.lease_token for job in jobs],
                    "worker_id": worker_id,
                },
            )
            return set(rows.scalars())

    async def reap(self, limit: int) -> list[ReapedJob]:
        """Reclaim up to `limit` jobs whose lease expired. Not part of Broker: the reaper in
        the scheduler process calls it, never a worker."""
        async with self._engine.begin() as conn:
            rows = await conn.execute(text(sql.REAP), {"limit": limit})
            return [
                ReapedJob(
                    id=r.id,
                    queue=r.queue,
                    task=r.task,
                    status=r.status,
                    attempt=r.attempts,
                    lease_owner=r.lease_owner,
                )
                for r in rows
            ]
