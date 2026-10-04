from typing import Any

from pydantic import ValidationError

from hopper.api.errors import ApiError
from hopper.tasks import registry


def checked_task(task: str, payload: dict[str, Any]) -> registry.TaskSpec:
    """The task's spec, or a 422 for an unknown task or a payload the task would reject."""
    spec = registry.get_task(task)
    if spec is None:
        raise ApiError(
            422,
            "unknown_task",
            f"unknown task {task!r}; known tasks: {', '.join(registry.task_names())}",
        )
    try:
        spec.payload_model.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        where = ".".join(str(part) for part in first["loc"]) or "payload"
        raise ApiError(422, "invalid_payload", f"{where}: {first['msg']}") from exc
    return spec
