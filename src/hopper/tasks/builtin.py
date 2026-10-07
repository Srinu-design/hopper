import asyncio
import hashlib
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


class CpuPayload(TaskPayload):
    # About 2.4 million rounds a second on one core (measured in the image's Python 3.12), so
    # even the cap finishes in under 10 s, inside the 30 s timeout.
    n: int = Field(ge=1, le=20_000_000)


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


def hash_rounds(n: int) -> str:
    """SHA-256 applied n times to its own output: pure CPU, and the answer is checkable."""
    digest = b"hopper"
    for _ in range(n):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


@task("cpu", payload=CpuPayload)
async def cpu(payload: CpuPayload) -> dict[str, Any] | None:
    """Load tests: hashes n times, to show CPU-bound behaviour.

    The hashing runs in a thread, never on the event loop: a loop busy for seconds would stop
    the worker's heartbeats, its leases would expire, and its jobs would run twice. On a
    timeout the worker stops waiting, but the thread finishes its rounds (Python cannot kill
    a thread), which the n cap keeps short.
    """
    return {"n": payload.n, "digest": await asyncio.to_thread(hash_rounds, payload.n)}
