"""Dead-letter queue: list and replay jobs with status = 'dead'. Every function takes tenant_id."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue import sql

REPLAY_BATCH_LIMIT = 1000


@dataclass(frozen=True, slots=True)
class DeadFilter:
    queue: str | None = None
    task: str | None = None
    since: datetime | None = None  # dead_at >= since
    ids: list[UUID] | None = None

    def clauses(self) -> tuple[str, dict[str, Any]]:
        """SQL conditions from fixed fragments; values only ever travel as bind parameters."""
        parts: list[str] = []
        params: dict[str, Any] = {}
        if self.queue is not None:
            parts.append("AND queue = :f_queue")
            params["f_queue"] = self.queue
        if self.task is not None:
            parts.append("AND task = :f_task")
            params["f_task"] = self.task
        if self.since is not None:
            parts.append("AND dead_at >= :f_since")
            params["f_since"] = self.since
        if self.ids is not None:
            parts.append("AND id = ANY(:f_ids)")
            params["f_ids"] = self.ids
        return " ".join(parts), params


async def list_dead(
    engine: AsyncEngine,
    *,
    tenant_id: UUID,
    where: DeadFilter,
    limit: int,
    after: tuple[datetime, UUID] | None = None,
) -> list[dict[str, Any]]:
    """Newest deaths first, keyset-paginated on (dead_at, id)."""
    filters, params = where.clauses()
    if after is not None:
        filters += " AND (dead_at, id) < (:after_dead_at, :after_id)"
        params |= {"after_dead_at": after[0], "after_id": after[1]}
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(sql.DLQ_LIST.format(filters=filters)),
            {"tenant_id": tenant_id, "limit": limit, **params},
        )
        return [dict(r._mapping) for r in rows]


async def replay(
    engine: AsyncEngine,
    *,
    tenant_id: UUID,
    where: DeadFilter,
    spread_seconds: float,
    limit: int = REPLAY_BATCH_LIMIT,
) -> tuple[list[UUID], bool]:
    """Requeue up to `limit` matching dead jobs. Returns (replayed ids, whether more remain)."""
    filters, params = where.clauses()
    bind = {"tenant_id": tenant_id, **params}
    async with engine.begin() as conn:
        rows = await conn.execute(
            text(sql.DLQ_REPLAY.format(filters=filters)),
            {**bind, "limit": limit, "spread_seconds": float(spread_seconds)},
        )
        replayed = [r.id for r in rows]
        more = bool(
            (await conn.execute(text(sql.DLQ_ANY_LEFT.format(filters=filters)), bind)).scalar()
        )
    return replayed, more
