from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict

# Retry policy defaults; a task can override each one in @task(...).
DEFAULT_MAX_ATTEMPTS = 5  # matches the jobs.max_attempts column default
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_BACKOFF_BASE = 2.0
DEFAULT_BACKOFF_CAP = 600.0


class TaskPayload(BaseModel):
    """Base for task payload models. Unknown fields are rejected so typos fail loudly."""

    model_config = ConfigDict(extra="forbid")


Handler = Callable[[Any], Awaitable[dict[str, Any] | None]]


@dataclass(frozen=True, slots=True)
class TaskSpec:
    name: str
    handler: Handler
    payload_model: type[TaskPayload]
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    backoff_base: float = DEFAULT_BACKOFF_BASE
    backoff_cap: float = DEFAULT_BACKOFF_CAP


_REGISTRY: dict[str, TaskSpec] = {}


def task(
    name: str,
    *,
    payload: type[TaskPayload],
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    backoff_cap: float = DEFAULT_BACKOFF_CAP,
) -> Callable[[Handler], Handler]:
    """Register a handler: @task("http", payload=HttpPayload, max_attempts=8, timeout=15)."""
    if max_attempts < 1 or timeout < 1 or backoff_base < 0 or backoff_cap < backoff_base:
        raise ValueError(f"invalid retry policy for task {name!r}")

    def register(handler: Handler) -> Handler:
        if name in _REGISTRY:
            raise ValueError(f"task {name!r} is already registered")
        _REGISTRY[name] = TaskSpec(
            name, handler, payload, max_attempts, timeout, backoff_base, backoff_cap
        )
        return handler

    return register


def get_task(name: str) -> TaskSpec | None:
    return _REGISTRY.get(name)


def task_names() -> list[str]:
    return sorted(_REGISTRY)
