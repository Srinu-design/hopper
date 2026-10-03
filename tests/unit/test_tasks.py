import pytest
from pydantic import ValidationError

from hopper.tasks import registry
from hopper.tasks.builtin import FlakyPayload, SleepPayload, flaky, sleep
from hopper.tasks.registry import TaskPayload


def test_builtin_tasks_are_registered() -> None:
    assert {"sleep", "flaky"} <= set(registry.task_names())
    spec = registry.get_task("sleep")
    assert spec is not None
    assert spec.payload_model is SleepPayload
    assert (spec.max_attempts, spec.timeout_seconds) == (5, 30)


def test_unknown_task_is_none() -> None:
    assert registry.get_task("nope") is None


def test_duplicate_registration_is_rejected() -> None:
    class P(TaskPayload):
        pass

    async def handler(payload: P) -> None:
        return None

    with pytest.raises(ValueError, match="already registered"):
        registry.task("sleep", payload=P)(handler)


def test_payload_models_reject_bad_input() -> None:
    with pytest.raises(ValidationError):
        SleepPayload.model_validate({"ms": -1})
    with pytest.raises(ValidationError):
        SleepPayload.model_validate({"ms": 1, "typo": 1})
    with pytest.raises(ValidationError):
        FlakyPayload.model_validate({"p": 1.5})


async def test_sleep_returns_what_it_slept() -> None:
    assert await sleep(SleepPayload(ms=0)) == {"slept_ms": 0}


async def test_flaky_never_fails_at_p0_and_always_fails_at_p1() -> None:
    for _ in range(20):
        assert await flaky(FlakyPayload(p=0)) == {"ok": True}
    with pytest.raises(RuntimeError):
        await flaky(FlakyPayload(p=1))
