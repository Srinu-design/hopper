"""loadtest/bench.py: how it reads k6, Postgres and docker stats, and judges scenario D.

The bench itself drives Docker and is run by `make load` / `make bench`; these tests pin down
the parts that turn raw output into the published numbers.
"""

from pathlib import Path

import pytest

from loadtest import bench

ROOT = Path(__file__).resolve().parents[2]


def k6_summary(*, count: int = 1000, rate: float = 100.0, failed: float = 0.0) -> dict:
    """The shape of k6 2.x's handleSummary data, trimmed to what the bench reads."""
    return {
        "metrics": {
            "http_reqs": {"values": {"count": count, "rate": rate}},
            "http_req_duration": {
                "values": {"med": 7.0, "p(95)": 15.9, "p(99)": 30.0, "max": 250.0, "avg": 9.0}
            },
            "http_req_failed": {"values": {"rate": failed}},
            "checks": {"values": {"rate": 1.0 - failed}},
        }
    }


def test_k6_numbers_come_from_its_summary() -> None:
    numbers = bench.k6_numbers(k6_summary())
    assert numbers["achieved_rps"] == 100.0 and numbers["requests"] == 1000
    assert (numbers["p50_ms"], numbers["p95_ms"], numbers["p99_ms"]) == (7.0, 15.9, 30.0)
    assert numbers["dropped"] == 0  # k6 leaves the metric out when nothing was dropped
    with_drops = k6_summary()
    with_drops["metrics"]["dropped_iterations"] = {"values": {"count": 12}}
    assert bench.k6_numbers(with_drops)["dropped"] == 12


def test_postgres_float_arrays() -> None:
    assert bench.parse_pg_array("{0.004,0.0125,0.5}") == [0.004, 0.0125, 0.5]
    assert bench.parse_pg_array("{NULL,1}") == [None, 1.0]
    assert bench.parse_pg_array("") == []  # no rows in the window


def test_docker_stats_are_grouped_by_role() -> None:
    raw = "\n".join(
        [
            "hopper-api-1\t98.50%",
            "hopper-postgres-1\t150.00%",
            "hopper-postgres-exporter-1\t0.50%",
            "hopper-worker-1\t40.00%",
            "hopper-worker-2\t60.00%",
            "hopper-scheduler-1\t1.00%",
            "hopper-k6\t30.00%",
            "garbage line",
        ]
    )
    cpu = bench.group_cpu(bench.parse_docker_stats(raw))
    assert cpu["api"] == 98.5
    assert cpu["postgres"] == 150.0  # the exporter is not Postgres
    assert (cpu["workers"], cpu["worker_max"]) == (100.0, 60.0)
    assert cpu["k6"] == 30.0


def test_median_and_spread_formatting() -> None:
    assert bench.fmt(bench.median_spread([10.0, 12.0, 11.0]), 0) == "11 (10-12)"
    assert bench.fmt(bench.median_spread([5.0, 5.0, 5.0]), 1) == "5.0"
    assert bench.fmt(bench.median_spread([0.001, 0.002]), 2, 100) == "0.15 (0.10-0.20)"
    assert bench.fmt(bench.median_spread([])) == "n/a"


@pytest.mark.parametrize(
    ("duration", "seconds"), [("90s", 90), ("2m", 120), ("1m30s", 90), ("10m", 600)]
)
def test_k6_durations(duration: str, seconds: float) -> None:
    assert bench.seconds(duration) == seconds


def step(rate: int, *, wait95: float = 0.1, failed: float = 0.0, achieved: float | None = None):
    api = bench.k6_numbers(k6_summary(count=rate * 60, rate=achieved or rate, failed=failed))
    return {"rate": rate, "api": api, "jobs": {"wait_s": [0.01, wait95, wait95]}}


def test_scenario_d_breaks_on_the_guides_two_rules_and_on_an_api_falling_behind() -> None:
    assert bench.breaking_reason(step(500), 5.0, 0.01) == ""
    assert "queue wait" in bench.breaking_reason(step(500, wait95=6.2), 5.0, 0.01)
    assert "errors" in bench.breaking_reason(step(500, failed=0.02), 5.0, 0.01)
    assert "kept up" in bench.breaking_reason(step(2000, achieved=1500.0), 5.0, 0.01)


def test_the_summary_tables_from_made_up_runs() -> None:
    env = {
        "host": "h",
        "os": "o",
        "docker": "d",
        "stack": "s",
        "settings": "x",
        "k6": "k",
        "git": "abc",
        "started": "now",
    }
    a_run = {
        "scenario": "A",
        "rate": 100,
        "duration": "2m",
        "api": bench.k6_numbers(k6_summary()),
        "cpu": {"api": 20.0, "postgres": 10.0},
    }
    b_run = {
        "scenario": "B",
        "workers": 2,
        "jobs": 100_000,
        "jobs_per_s": 2000.0,
        "drain_s": 50.0,
        "cpu": {},
    }
    d_step = {**step(250), "backlog": 0, "broke": ""}  # no CPU or Postgres sample
    d_run = {"scenario": "D", "run": 1, "steps": [d_step]}
    text = bench.render_summary(env, {"runs": [a_run, b_run, d_run]})
    assert "| 100 | 100.0 | 7.0 | 15.9 | 30.0 | 0.00% | 20 | 10 |" in text
    assert "| 2 | 2,000 | 50.0 | n/a | n/a |" in text
    assert "Highest rate held: 250 req/s." in text


def test_the_k6_script_reads_what_the_bench_passes() -> None:
    script = (ROOT / "loadtest" / bench.K6_SCRIPT).read_text(encoding="utf-8")
    for variable in ("BASE", "KEY", "RATE", "DURATION", "MIX", "SUMMARY"):
        assert f"__ENV.{variable}" in script, variable
    assert "constant-arrival-rate" in script
    assert "task: 'sleep', payload: { ms: 50 }" in script  # the guide's job


def test_defaults_follow_the_build_guide() -> None:
    args = bench.parse_args([])
    assert args.scenarios == ["A", "B", "C", "D"] and args.runs == 3
    assert args.a_rates == [100, 250, 500, 1000] and args.a_duration == "2m"
    assert args.b_workers == [1, 2, 4, 8] and args.b_jobs == 100_000
    assert (args.c_rate, args.c_duration) == (300, "10m")
    assert (args.d_wait_limit, args.d_error_limit) == (5.0, 0.01)
    assert args.warmup == "1m"


@pytest.mark.parametrize(
    ("state", "reason"),
    [
        ({"on_mains": True, "power_profile": "balanced"}, ""),
        ({"on_mains": True, "power_profile": "performance"}, ""),
        ({"on_mains": None, "power_profile": None}, ""),  # a server: nothing to report
        ({"on_mains": False, "power_profile": "balanced"}, "battery"),
        ({"on_mains": True, "power_profile": "power-saver"}, "power-saver"),
    ],
)
def test_a_throttled_laptop_is_refused(state: dict, reason: str) -> None:
    found = bench.throttled(state)
    assert (reason in found) if reason else found == ""


def test_power_state_reads_without_failing() -> None:
    state = bench.power_state()
    assert set(state) == {"on_mains", "power_profile"}
    assert state["on_mains"] in (True, False, None)
