import asyncio
import random
from typing import Any

from pydantic import Field

from hopper.tasks.registry import TaskPayload, task


class SleepPayload(TaskPayload):
    ms: int = Field(ge=0, le=60_000)


class FlakyPayload(TaskPayload):
    p: float = Field(ge=0, le=1)  # probability of failing
    ms: int = Field(default=0, ge=0, le=60_000)


@task("sleep", payload=SleepPayload)
async def sleep(payload: SleepPayload) -> dict[str, Any] | None:
    """Load tests: sleeps for ms milliseconds."""
    await asyncio.sleep(payload.ms / 1000)
    return {"slept_ms": payload.ms}


@task("flaky", payload=FlakyPayload)
async def flaky(payload: FlakyPayload) -> dict[str, Any] | None:
    """Demos: fails with probability p, to show retries."""
    await asyncio.sleep(payload.ms / 1000)
    if random.random() < payload.p:
        raise RuntimeError("flaky task failed")
    return {"ok": True}
