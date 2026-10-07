"""chaos/kill_workers.py: the verdicts and the report, from made-up numbers.

The script itself drives Docker and is run for real by `make chaos` and nightly-chaos.yml;
these tests pin down how it judges a run, which must never pass a run that lost a job.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from chaos import kill_workers as chaos
from hopper.tasks.effect import EffectPayload

CLEAN = {
    "jobs": 10_000,
    "succeeded": 10_000,
    "dead": 0,
    "unfinished": 0,
    "effects": 10_000,
    "succeeded_without_effect": 0,
    "dead_with_effect": 0,
    "executions": 10_012,
    "executed_jobs": 10_000,
    "lease_expired_attempts": 90,
    "jobs_reclaimed": 88,
    "released_attempts": 0,
}
FENCED = {"acks_refused": 5, "nacks_refused": 0, "cancelled_lost_lease": 0}


def events() -> list[chaos.Event]:
    return [
        chaos.Event(4.0, "2026-10-07 10:00:04+00", "sigkill", "aaa", ["j1", "j2"]),
        chaos.Event(9.0, "2026-10-07 10:00:09+00", "sigterm", "bbb", ["j3"]),
        chaos.Event(12.0, "2026-10-07 10:00:12+00", "freeze", "ccc", ["j4"]),
        chaos.Event(20.0, "2026-10-07 10:00:20+00", "redis-restart"),
        chaos.Event(52.0, "2026-10-07 10:00:52+00", "thaw", "ccc"),
    ]


def test_a_clean_run_passes_and_counts_the_duplicates() -> None:
    summary = chaos.summarize(CLEAN, events(), [31.0, 34.5], "normal", FENCED)
    assert summary["passed"] and summary["lost"] == 0
    assert summary["duplicate_runs"] == 12
    assert (summary["sigkills"], summary["sigterms"], summary["freezes"]) == (1, 1, 1)
    assert summary["jobs_in_flight_at_sigkill"] == 2
    assert (summary["recovery_max_s"], summary["recovery_median_s"]) == (34.5, 32.8)


@pytest.mark.parametrize(
    "damage",
    [
        {"unfinished": 1},  # a job neither succeeded nor dead
        {"succeeded_without_effect": 1},  # "succeeded", but its effect never happened
        {"jobs": 0, "succeeded": 0, "effects": 0, "executions": 0, "executed_jobs": 0},
    ],
)
def test_any_loss_fails_a_normal_run(damage: dict[str, int]) -> None:
    summary = chaos.summarize({**CLEAN, **damage}, events(), [], "normal", FENCED)
    assert not summary["passed"] and summary["verdict"].startswith("FAIL")


def test_dead_jobs_are_visible_not_lost() -> None:
    summary = chaos.summarize(
        {**CLEAN, "succeeded": 9_998, "dead": 2}, events(), [], "normal", FENCED
    )
    assert summary["passed"] and summary["lost"] == 0


def test_the_negative_control_passes_only_when_it_loses_jobs() -> None:
    lossy = {**CLEAN, "effects": 9_960, "succeeded_without_effect": 40}
    caught = chaos.summarize(lossy, events(), [], "ack-before-run", FENCED)
    assert caught["passed"] and caught["lost"] == 40
    blind = chaos.summarize(CLEAN, events(), [], "ack-before-run", FENCED)
    assert not blind["passed"]  # a control that loses nothing proves nothing


def test_the_report_states_the_verdict_and_the_guide_checks() -> None:
    summary = chaos.summarize(CLEAN, events(), [31.0], "normal", FENCED)
    data = {
        "run": "chaos-20261007T100000Z-normal",
        "started": "2026-10-07 10:00:00 UTC",
        "finished": "2026-10-07 10:07:00 UTC",
        "host": "test host",
        "git": "abc1234",
        "args": {
            "mode": "normal",
            "compose": "docker compose",
            "workers": 4,
            "jobs": 10_000,
            "enqueue_seconds": 300.0,
            "duration": 300.0,
            "min_gap": 3.0,
            "max_gap": 10.0,
            "freeze_seconds": 40.0,
        },
        "enqueue": {"accepted": 10_000, "retried": 0, "failed": 0, "statuses": {}},
        "checks": CLEAN,
        "summary": summary,
        "events": [vars(e) for e in events()],
    }
    report = chaos.render_report(data)
    assert "**PASS: no job lost**" in report
    assert "| **Lost** (not succeeded or dead, or succeeded without its effect) | **0** |" in report
    assert "`status NOT IN ('succeeded', 'dead')`: **0**" in report
    assert "| Duplicate runs (runs minus distinct jobs) | 12 |" in report
    assert "| 4.0 | sigkill | aaa | 2 |" in report
    assert "| 20.0 | redis-restart |  |  |" in report


def test_run_labels_are_valid_effect_payload_labels() -> None:
    """The script tags every job's payload with its run label, which the task validates."""
    for mode in ("normal", "ack-before-run"):
        label = chaos.run_label(mode, datetime(2026, 10, 7, 10, 0, 0, tzinfo=UTC))
        assert EffectPayload(run=label).run == label


def test_env_file_values(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "# comment\nPOSTGRES_USER=hopper\nGRAFANA_ADMIN_PASSWORD=x-y  # trailing note\n\nBAD\n",
        encoding="utf-8",
    )
    assert chaos.read_env_file(env) == {"POSTGRES_USER": "hopper", "GRAFANA_ADMIN_PASSWORD": "x-y"}
    assert chaos.read_env_file(tmp_path / "missing") == {}


def test_the_freeze_signals_the_worker_process_not_the_container_init() -> None:
    """With init: true, PID 1 is docker-init (argv: docker-init -- python -m hopper.worker).
    Stopping it would not stop the worker; the match must be on the worker's own argv."""
    compile(chaos.SIGNAL_WORKER, "<signal-worker>", "exec")
    assert 'argv[1:3] == [b"-m", b"hopper.worker"]' in chaos.SIGNAL_WORKER


def test_defaults_follow_the_build_guide() -> None:
    args = chaos.parse_args([])
    assert (args.jobs, args.workers, args.duration) == (10_000, 4, 300.0)
    assert (args.min_gap, args.max_gap, args.drain_timeout) == (3.0, 10.0, 600.0)
    assert args.enqueue_seconds == args.duration  # kills keep landing mid-job
    assert args.freeze_seconds > chaos.LEASE_SECONDS  # a freeze must outlast the lease
    assert args.sigterm and args.redis_restart and args.freeze


def test_the_control_override_only_flips_the_switch() -> None:
    override = (chaos.ROOT / chaos.CONTROL_OVERRIDE).read_text(encoding="utf-8")
    body = [line for line in override.splitlines() if line and not line.startswith("#")]
    assert body == [
        "services:",
        "  worker:",
        "    environment:",
        '      CHAOS_ACK_BEFORE_RUN: "true"',
    ]
