"""Cron schedules: tenant-scoped CRUD for the API, and fire_due for the scheduler's cron loop."""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from hopper.queue import sql
from hopper.scheduler.crontab import next_run
from hopper.tasks import registry

log = structlog.get_logger()


async def _db_now(conn: AsyncConnection) -> datetime:
    """Every "now" in the queue comes from the database, never from a process clock."""
    now: datetime = (await conn.execute(text("SELECT now()"))).scalar_one()
    return now


async def create_schedule(
    engine: AsyncEngine,
    *,
    tenant_id: UUID,
    name: str,
    cron: str,
    timezone: str,
    queue: str,
    task: str,
    payload: dict[str, Any],
    enabled: bool,
) -> dict[str, Any] | None:
    """None if this tenant already has a schedule with that name."""
    async with engine.begin() as conn:
        first = next_run(cron, timezone, await _db_now(conn))
        row = (
            await conn.execute(
                text(sql.INSERT_SCHEDULE),
                {
                    "tenant_id": tenant_id,
                    "name": name,
                    "cron": cron,
                    "timezone": timezone,
                    "queue": queue,
                    "task": task,
                    "payload": json.dumps(payload),
                    "enabled": enabled,
                    "next_run_at": first,
                },
            )
        ).first()
    return dict(row._mapping) if row else None


async def list_schedules(
    engine: AsyncEngine, *, tenant_id: UUID, limit: int, after: str = ""
) -> list[dict[str, Any]]:
    """Ordered by name; `after` is the last name of the previous page."""
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(sql.LIST_SCHEDULES), {"tenant_id": tenant_id, "limit": limit, "after": after}
        )
        return [dict(r._mapping) for r in rows]


async def get_schedule(
    engine: AsyncEngine, *, tenant_id: UUID, schedule_id: UUID
) -> dict[str, Any] | None:
    async with engine.connect() as conn:
        row = (
            await conn.execute(text(sql.GET_SCHEDULE), {"id": schedule_id, "tenant_id": tenant_id})
        ).first()
    return dict(row._mapping) if row else None


async def set_enabled(
    engine: AsyncEngine, *, tenant_id: UUID, schedule_id: UUID, enabled: bool
) -> dict[str, Any] | None:
    async with engine.begin() as conn:
        current = (
            await conn.execute(text(sql.GET_SCHEDULE), {"id": schedule_id, "tenant_id": tenant_id})
        ).first()
        if current is None:
            return None
        restart = next_run(current.cron, current.timezone, await _db_now(conn))
        row = (
            await conn.execute(
                text(sql.SET_SCHEDULE_ENABLED),
                {
                    "id": schedule_id,
                    "tenant_id": tenant_id,
                    "enabled": enabled,
                    "next_run_at": restart,
                },
            )
        ).first()
    return dict(row._mapping) if row else None


async def delete_schedule(engine: AsyncEngine, *, tenant_id: UUID, schedule_id: UUID) -> bool:
    """Jobs the schedule already created stay; their schedule_id becomes NULL."""
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(sql.DELETE_SCHEDULE), {"id": schedule_id, "tenant_id": tenant_id}
            )
        ).first()
    return row is not None


@dataclass(frozen=True, slots=True)
class Fired:
    schedule_id: UUID
    tenant_id: UUID
    fire_time: datetime
    job_id: UUID | None  # None: this tick's job already existed (the idempotency key held)
    next_run_at: datetime | None  # None: the schedule was switched off


def cron_idempotency_key(schedule_id: UUID, fire_time: datetime) -> str:
    """One key per schedule per tick: a second insert for the same tick does nothing."""
    return f"cron:{schedule_id}:{fire_time.isoformat()}"


async def fire_due(engine: AsyncEngine, *, limit: int) -> list[Fired]:
    """Fire up to `limit` due schedules in one transaction.

    The schedule rows are locked with SKIP LOCKED, so two schedulers take disjoint sets.
    Each due schedule fires once, even after downtime (misfire policy: fire once, then jump
    to the next time after now; no flood of catch-up runs). The job insert and the move of
    next_run_at commit together, so a tick is never lost or doubled; the deterministic
    idempotency key is a second safety net.
    """
    fired: list[Fired] = []
    async with engine.begin() as conn:
        now = await _db_now(conn)
        due = (await conn.execute(text(sql.DUE_SCHEDULES), {"limit": limit})).all()
        for s in due:
            try:
                upcoming = next_run(s.cron, s.timezone, now)
            except Exception:
                log.exception("schedule_disabled_bad_expression", schedule_id=str(s.id))
                await conn.execute(text(sql.DISABLE_SCHEDULE), {"id": s.id})
                fired.append(Fired(s.id, s.tenant_id, s.next_run_at, None, None))
                continue
            spec = registry.get_task(s.task)
            inserted = (
                await conn.execute(
                    text(sql.INSERT_JOB),
                    {
                        "tenant_id": s.tenant_id,
                        "queue": s.queue,
                        "task": s.task,
                        "payload": json.dumps(s.payload),
                        "priority": 0,
                        "max_attempts": spec.max_attempts
                        if spec
                        else registry.DEFAULT_MAX_ATTEMPTS,
                        "timeout_seconds": (
                            spec.timeout_seconds if spec else registry.DEFAULT_TIMEOUT_SECONDS
                        ),
                        "idempotency_key": cron_idempotency_key(s.id, s.next_run_at),
                        "request_hash": None,
                        "run_at": None,
                        "delay_seconds": 0.0,
                        "schedule_id": s.id,
                    },
                )
            ).first()
            await conn.execute(text(sql.ADVANCE_SCHEDULE), {"id": s.id, "next_run_at": upcoming})
            job_id = inserted.id if inserted else None
            fired.append(Fired(s.id, s.tenant_id, s.next_run_at, job_id, upcoming))
    return fired
