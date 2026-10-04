"""The job a handler is running, for the few handlers that need more than their payload.

The worker sets it around each handler call. It is a context variable, so each job's asyncio
task sees its own job even with many running at once.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from uuid import UUID


@dataclass(frozen=True, slots=True)
class JobContext:
    job_id: UUID
    tenant_id: UUID
    attempt: int
    max_attempts: int
    signing_secret: bytes = field(repr=False)


_current: ContextVar[JobContext] = ContextVar("hopper_job")


def current_job() -> JobContext:
    try:
        return _current.get()
    except LookupError:
        raise RuntimeError("not running inside a Hopper job") from None


@contextmanager
def running(job: JobContext) -> Iterator[None]:
    token = _current.set(job)
    try:
        yield
    finally:
        _current.reset(token)
