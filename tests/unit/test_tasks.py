import asyncio
import hashlib
import time

import pytest
from pydantic import ValidationError

from hopper.tasks import registry
from hopper.tasks.builtin import (
    CpuPayload,
    FailAlwaysPayload,
    FlakyPayload,
    SleepPayload,
    cpu,
    fail_always,
    flaky,
    hash_rounds,
    sleep,
)
from hopper.tasks.effect import EffectPayload
from hopper.tasks.errors import PermanentError
from hopper.tasks.registry import TaskPayload


def test_builtin_tasks_are_registered() -> None:
    assert {"sleep", "flaky", "fail_always"} <= set(registry.task_names())
    spec = registry.get_task("sleep")
    assert spec is not None
    assert spec.payload_model is SleepPayload
    assert (spec.max_attempts, spec.timeout_seconds) == (5, 30)
    assert (spec.backoff_base, spec.backoff_cap) == (2.0, 600.0)


@pytest.mark.parametrize(
    "policy",
    [
        {"max_attempts": 0},
        {"timeout": 0},
        {"backoff_base": -1},
        {"backoff_base": 10, "backoff_cap": 5},
    ],
)
def test_invalid_retry_policy_is_rejected(policy: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="invalid retry policy"):
        registry.task("never_registered", payload=SleepPayload, **policy)  # type: ignore[arg-type]


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


async def test_fail_always_is_retryable_unless_permanent() -> None:
    with pytest.raises(RuntimeError):
        await fail_always(FailAlwaysPayload())
    with pytest.raises(PermanentError):
        await fail_always(FailAlwaysPayload(permanent=True))


def test_load_and_chaos_tasks_are_registered() -> None:
    assert {"cpu", "effect"} <= set(registry.task_names())


def test_hash_rounds_is_sha256_applied_n_times() -> None:
    assert hash_rounds(1) == hashlib.sha256(b"hopper").hexdigest()
    twice = hashlib.sha256(hashlib.sha256(b"hopper").digest()).hexdigest()
    assert hash_rounds(2) == twice


def test_cpu_payload_is_bounded() -> None:
    assert CpuPayload(n=1).n == 1
    for bad in (0, 20_000_001):
        with pytest.raises(ValidationError):
            CpuPayload.model_validate({"n": bad})


async def test_cpu_hashes_off_the_event_loop() -> None:
    """While the cpu task hashes, the event loop keeps turning, so heartbeats keep going.

    Hashing on the loop itself would freeze it for the whole run; in a thread it only pauses
    for the interpreter's thread switch interval.
    """
    gaps: list[float] = []
    done = asyncio.Event()

    async def ticker() -> None:
        last = time.monotonic()
        while not done.is_set():
            await asyncio.sleep(0.01)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    ticking = asyncio.create_task(ticker())
    started = time.monotonic()
    result = await cpu(CpuPayload(n=2_000_000))
    elapsed = time.monotonic() - started
    done.set()
    await ticking
    assert result is not None and result["n"] == 2_000_000 and len(result["digest"]) == 64
    assert elapsed > 0.3  # long enough that a blocked loop would have shown
    assert gaps, "the event loop never turned while the task hashed"
    assert max(gaps) < 0.25, f"the loop stalled for {max(gaps):.2f} s"


def test_effect_payload_defaults_and_bounds() -> None:
    payload = EffectPayload()
    assert (payload.min_ms, payload.max_ms, payload.run) == (50, 500, None)
    with pytest.raises(ValidationError, match="max_ms"):
        EffectPayload.model_validate({"min_ms": 100, "max_ms": 10})
    with pytest.raises(ValidationError):
        EffectPayload.model_validate({"run": "spaces are not allowed"})
