"""The effect task: a stand-in for a real side effect, for the chaos test.

It sleeps a random 50 to 500 ms, so a worker killed at a random moment is usually mid-job,
then writes two rows in one transaction:

- job_executions: one row per run, so duplicate runs can be counted (at-least-once at work);
- job_effects: one row per job, INSERT ... ON CONFLICT (job_id) DO NOTHING, the shape of an
  idempotent side effect, so running a job twice changes nothing the second time.

A worker killed during the sleep leaves nothing behind and the job runs again elsewhere. A
worker killed after the commit but before its ack leaves both rows, and the rerun adds a
second execution but no second effect: a duplicate run, shown to be harmless.

The worker passes in its database engine at start (configure). The rows go with their job
(ON DELETE CASCADE), so retention removes them with it.

Its timeout (60 s) is longer than the 30 s lease on purpose. The timeout counts from the
claim, paused or not, so with a shorter one a worker frozen past its lease would time out the
moment it woke up. With this one it carries on and finishes a job that was meanwhile reclaimed
and run elsewhere: the duplicate run the chaos test needs to show, and the fencing token's
refusal of its ack.
"""

import asyncio
import random
from typing import Any, Self

from pydantic import Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.tasks.context import current_job
from hopper.tasks.registry import TaskPayload, task

RECORD = """
WITH execution AS (
  INSERT INTO job_executions (job_id, attempt, worker_id)
  VALUES (:job_id, :attempt, :worker_id)
)
INSERT INTO job_effects (job_id) VALUES (:job_id) ON CONFLICT (job_id) DO NOTHING
"""


class EffectPayload(TaskPayload):
    min_ms: int = Field(default=50, ge=0, le=10_000)
    max_ms: int = Field(default=500, ge=0, le=10_000)
    # A label for one chaos run, so its checks look only at its own jobs.
    run: str | None = Field(default=None, max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.max_ms < self.min_ms:
            raise ValueError("max_ms must not be less than min_ms")
        return self


class _Effect:
    def __init__(self) -> None:
        self.engine: AsyncEngine | None = None

    def configure(self, engine: AsyncEngine | None) -> None:
        self.engine = engine


_effect = _Effect()
configure = _effect.configure


@task("effect", payload=EffectPayload, timeout=60)
async def effect(payload: EffectPayload) -> dict[str, Any] | None:
    """Chaos test: sleep 50 to 500 ms, then record this run and the job's one effect."""
    engine = _effect.engine
    if engine is None:
        # Retryable: a worker that was never configured is a deployment bug, not a bad job.
        raise RuntimeError("effect task: no database configured in this worker")
    await asyncio.sleep(random.uniform(payload.min_ms, payload.max_ms) / 1000)
    job = current_job()
    async with engine.begin() as conn:
        await conn.execute(
            text(RECORD),
            {"job_id": job.job_id, "attempt": job.attempt, "worker_id": job.worker_id},
        )
    return {"recorded": True}
