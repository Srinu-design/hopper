from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict


class TaskPayload(BaseModel):
    """Base for task payload models. Unknown fields are rejected so typos fail loudly."""

    model_config = ConfigDict(extra="forbid")


Handler = Callable[[Any], Awaitable[dict[str, Any] | None]]


@dataclass(frozen=True, slots=True)
class TaskSpec:
    name: str
    handler: Handler
    payload_model: type[TaskPayload]
    max_attempts: int
    timeout_seconds: int


_REGISTRY: dict[str, TaskSpec] = {}


def task(
    name: str,
    *,
    payload: type[TaskPayload],
    max_attempts: int = 5,
    timeout: int = 30,
) -> Callable[[Handler], Handler]:
    """Register a handler: @task("sleep", payload=SleepPayload, timeout=15)."""

    def register(handler: Handler) -> Handler:
        if name in _REGISTRY:
            raise ValueError(f"task {name!r} is already registered")
        _REGISTRY[name] = TaskSpec(name, handler, payload, max_attempts, timeout)
        return handler

    return register


def get_task(name: str) -> TaskSpec | None:
    return _REGISTRY.get(name)


def task_names() -> list[str]:
    return sorted(_REGISTRY)
