from datetime import UTC, datetime

import pytest

from hopper.scheduler.crontab import cron_error, next_run, timezone_error

NY = "America/New_York"


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)  # type: ignore[misc]


@pytest.mark.parametrize("expression", ["* * * * *", "*/5 * * * *", "0 9 * * 1-5", "30 2 1 * *"])
def test_valid_expressions(expression: str) -> None:
    assert cron_error(expression) is None


@pytest.mark.parametrize(
    ("expression", "why"),
    [
        ("* * * * * *", "five fields"),  # a seconds field would allow more than once a minute
        ("@hourly", "five fields"),
        ("* * * *", "five fields"),
        ("61 * * * *", "not a valid"),
        ("0 0 31 2 *", "not a valid"),  # 31 February never happens: strict mode
        ("every day", "five fields"),
    ],
)
def test_invalid_expressions(expression: str, why: str) -> None:
    problem = cron_error(expression)
    assert problem is not None and why in problem


def test_timezones_are_checked() -> None:
    assert timezone_error("Europe/London") is None
    assert timezone_error("UTC") is None
    assert timezone_error("Mars/Olympus_Mons") is not None
    assert timezone_error("../etc/passwd") is not None


def test_next_run_is_strictly_after() -> None:
    assert next_run("*/5 * * * *", "UTC", utc(2026, 1, 1, 0, 5)) == utc(2026, 1, 1, 0, 10)
    assert next_run("*/5 * * * *", "UTC", utc(2026, 1, 1, 0, 5, 1)) == utc(2026, 1, 1, 0, 10)


def test_misfire_fires_once_then_jumps_to_the_future() -> None:
    """After 3 hours of downtime an hourly schedule's next time is the next future hour,
    not three catch-up runs: the cron loop computes from now, not from the missed time."""
    now = utc(2026, 6, 1, 15, 20)
    assert next_run("0 * * * *", "UTC", now) == utc(2026, 6, 1, 16, 0)


def test_daily_schedule_keeps_its_local_time_across_spring_forward() -> None:
    """09:00 New York is 14:00 UTC in winter and 13:00 UTC after clocks go forward."""
    first = next_run("0 9 * * *", NY, utc(2026, 3, 7, 0, 0))
    second = next_run("0 9 * * *", NY, first)
    third = next_run("0 9 * * *", NY, second)
    assert [first, second, third] == [
        utc(2026, 3, 7, 14, 0),
        utc(2026, 3, 8, 13, 0),  # 8 March 2026: US daylight saving starts
        utc(2026, 3, 9, 13, 0),
    ]


def test_time_inside_the_spring_gap_runs_after_the_gap() -> None:
    """02:30 does not exist on 8 March 2026 in New York; it runs at 03:30 local."""
    fire = next_run("30 2 * * *", NY, utc(2026, 3, 7, 12, 0))
    assert fire == utc(2026, 3, 8, 7, 30)  # 03:30 EDT
    assert next_run("30 2 * * *", NY, fire) == utc(2026, 3, 9, 6, 30)  # back to 02:30 EDT


def test_repeated_autumn_hour_fires_once() -> None:
    """01:30 happens twice on 1 November 2026 in New York; a daily schedule fires once."""
    first = next_run("30 1 * * *", NY, utc(2026, 10, 31, 12, 0))
    assert first == utc(2026, 11, 1, 5, 30)  # the first 01:30 (EDT)
    assert next_run("30 1 * * *", NY, first) == utc(2026, 11, 2, 6, 30)  # tomorrow, EST


def test_starting_inside_the_repeated_hour_never_returns_the_past() -> None:
    inside_second_pass = utc(2026, 11, 1, 6, 10)  # 01:10 EST, after the first 01:30 passed
    assert next_run("30 1 * * *", NY, inside_second_pass) == utc(2026, 11, 2, 6, 30)
