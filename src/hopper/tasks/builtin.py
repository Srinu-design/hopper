import asyncio
import random
from typing import Any

from pydantic import Field

from hopper.tasks.errors import PermanentError
from hopper.tasks.registry import TaskPayload, task


class SleepPayload(TaskPayload):
    ms: int = Field(ge=0, le=60_000)


class FlakyPayload(TaskPayload):
    p: float = Field(ge=0, le=1)  # probability of failing
    ms: int = Field(default=0, ge=0, le=60_000)


class FailAlwaysPayload(TaskPayload):
    permanent: bool = False  # true: skip retries and go straight to the DLQ


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


@task("fail_always", payload=FailAlwaysPayload)
async def fail_always(payload: FailAlwaysPayload) -> dict[str, Any] | None:
    """Demos: always fails, to fill the DLQ (after retries, or at once if permanent)."""
    if payload.permanent:
        raise PermanentError("fail_always: permanent failure")
    raise RuntimeError("fail_always: retryable failure")
