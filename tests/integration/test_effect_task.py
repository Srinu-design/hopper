"""The effect task: one execution row per run, one effect row per job, whatever the reruns."""

import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.tasks import effect
from hopper.tasks.context import JobContext, running
from hopper.tasks.effect import EffectPayload
from tests.helpers import insert_job, job_row, make_worker, run_until, seed_tenant

QUICK = '{"min_ms": 0, "max_ms": 10}'


@pytest.fixture
def effect_engine(migrated_engine: AsyncEngine) -> Iterator[AsyncEngine]:
    """The worker process does this at start; tests point the task at their own database."""
    effect.configure(migrated_engine)
    yield migrated_engine
    effect.configure(None)


async def recorded(engine: AsyncEngine, job_id: uuid.UUID) -> tuple[int, int]:
    """(execution rows, effect rows) for one job."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT (SELECT count(*) FROM job_executions WHERE job_id = :j),"
                    "       (SELECT count(*) FROM job_effects WHERE job_id = :j)"
                ),
                {"j": job_id},
            )
        ).one()
    return int(row[0]), int(row[1])


async def test_one_run_records_one_execution_and_one_effect(effect_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(effect_engine)
    job_id = await insert_job(effect_engine, tenant_id, task="effect", payload=QUICK)
    await run_until([make_worker(effect_engine)], effect_engine, lambda c: c.get("succeeded"))
    assert (await job_row(effect_engine, job_id))["result"] == {"recorded": True}
    assert await recorded(effect_engine, job_id) == (1, 1)
    async with effect_engine.connect() as conn:
        rows = await conn.execute(text("SELECT attempt, worker_id FROM job_executions"))
        assert [tuple(r) for r in rows] == [(1, "w1")]


async def test_a_rerun_adds_an_execution_but_never_a_second_effect(
    effect_engine: AsyncEngine,
) -> None:
    """What a worker that dies after the effect but before its ack leads to: the job runs
    again elsewhere. The duplicate run is visible, and harmless."""
    tenant_id = await seed_tenant(effect_engine)
    job_id = await insert_job(effect_engine, tenant_id, task="effect", payload=QUICK)
    for attempt, worker in ((1, "victim"), (2, "survivor")):
        context = JobContext(job_id, tenant_id, attempt, 5, b"", worker_id=worker)
        with running(context):
            assert await effect.effect(EffectPayload(min_ms=0, max_ms=0)) == {"recorded": True}
    assert await recorded(effect_engine, job_id) == (2, 1)


async def test_effect_rows_go_with_their_job(effect_engine: AsyncEngine) -> None:
    """Retention deletes old jobs; their chaos-test rows must not outlive them."""
    tenant_id = await seed_tenant(effect_engine)
    job_id = await insert_job(effect_engine, tenant_id, task="effect", payload=QUICK)
    with running(JobContext(job_id, tenant_id, 1, 5, b"", worker_id="w")):
        await effect.effect(EffectPayload(min_ms=0, max_ms=0))
    async with effect_engine.begin() as conn:
        await conn.execute(text("DELETE FROM jobs WHERE id = :j"), {"j": job_id})
    assert await recorded(effect_engine, job_id) == (0, 0)


async def test_an_unconfigured_worker_fails_the_run_so_it_is_retried() -> None:
    effect.configure(None)
    context = JobContext(uuid.uuid4(), uuid.uuid4(), 1, 5, b"", worker_id="w")
    with running(context), pytest.raises(RuntimeError, match="no database"):
        await effect.effect(EffectPayload(min_ms=0, max_ms=0))
