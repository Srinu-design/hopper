import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue import sql
from hopper.queue.broker import ClaimedJob

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
