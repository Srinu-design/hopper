"""The mini broker's benchmark and chaos scripts: their reports, from made-up numbers, and the
client's URL parsing. The scripts themselves drive Docker and are run for real by hand."""

import pytest

from chaos import broker_chaos
from hopper.minibroker.client import parse_url
from loadtest import broker_bench


def test_the_client_takes_a_tcp_url() -> None:
    assert parse_url("tcp://minibroker:6390") == ("minibroker", 6390)
    assert parse_url("tcp://127.0.0.1") == ("127.0.0.1", 6390)
    with pytest.raises(ValueError):
        parse_url("redis://127.0.0.1:6390")


def _runs(*throughputs: float) -> list[list[dict[str, float]]]:
    return [
        [{"connections": 8, "round_trips_per_s": t, "p50_ms": 1.0, "p99_ms": 2.0, "failed": 0}]
        for t in throughputs
    ]


def test_the_benchmark_table_shows_the_median_and_spread() -> None:
    lines = broker_bench.table({"postgres": _runs(100, 120, 110), "mini-always": _runs(900)})
    assert "| Postgres (SKIP LOCKED) | 8 | 110 (100-120) | 1.00 | 2.00 |" in lines
    assert "| mini, fsync always | 8 | 900 | 1.00 | 2.00 |" in lines


def test_the_benchmark_refuses_an_unknown_fsync_mode() -> None:
    with pytest.raises(SystemExit):
        broker_bench.parse_args(["--modes", "always,sometimes"])
    assert broker_bench.parse_args(["--quick"]).runs == 1


def _chaos(lost: int, broker_lost: int) -> dict[str, object]:
    workers = broker_chaos.WorkersRun(jobs=10_000, sigkills=30, lost=lost, effects=10_000)
    broker = broker_chaos.BrokerRun("no", acknowledged=5000, lost=broker_lost)
    return {
        "run": "broker-x",
        "verdict": "PASS: nothing lost" if lost + broker_lost == 0 else "FAIL",
        "host": "h",
        "git": "abc",
        "started": "s",
        "finished": "f",
        "args": {"duration": 300.0, "push_seconds": 30.0},
        "workers": broker_chaos.asdict(workers),
        "broker": [broker_chaos.asdict(broker)],
    }


def test_the_chaos_report_names_lost_jobs_in_both_parts() -> None:
    report = broker_chaos.render(_chaos(0, 0))
    assert "**PASS: nothing lost**" in report
    assert "| **Lost** (no effect and not in the DLQ) | **0** |" in report
    assert "| no | 5,000 | 0 | **0** | 0 |" in report
    failed = broker_chaos.render(_chaos(3, 0))
    assert "| **Lost** (no effect and not in the DLQ) | **3** |" in failed
