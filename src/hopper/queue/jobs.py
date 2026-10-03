"""Tenant-scoped job reads and writes for the API. Every function requires tenant_id."""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue import sql


def _job(row: Any) -> dict[str, Any]:
    return dict(row._mapping)


async def insert_job(
    engine: AsyncEngine,
    *,
    tenant_id: UUID,
    queue: str,
    task: str,
    payload: dict[str, Any],
    priority: int,
    max_attempts: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(sql.INSERT_JOB),
                {
                    "tenant_id": tenant_id,
                    "queue": queue,
                    "task": task,
                    "payload": json.dumps(payload),
                    "priority": priority,
                    "max_attempts": max_attempts,
                    "timeout_seconds": timeout_seconds,
                },
            )
        ).one()
    return _job(row)


async def get_job(
    engine: AsyncEngine, *, tenant_id: UUID, job_id: UUID
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    """The job and its attempt history, or None if it does not exist for this tenant."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(text(sql.GET_JOB), {"id": job_id, "tenant_id": tenant_id})
        ).first()
        if row is None:
            return None
        attempts = await conn.execute(text(sql.GET_ATTEMPTS), {"job_id": job_id})
        return _job(row), [dict(a._mapping) for a in attempts]


async def get_or_create_tenant(engine: AsyncEngine, name: str) -> UUID:
    async with engine.begin() as conn:
        result = await conn.execute(text(sql.UPSERT_TENANT), {"name": name})
        tenant_id: UUID = result.scalar_one()
    return tenant_id
