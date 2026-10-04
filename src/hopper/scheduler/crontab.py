"""Cron expressions: validation and the next fire time, in the schedule's own timezone."""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter


def cron_error(expression: str) -> str | None:
    """Why the expression is unacceptable, or None if it is fine.

    Exactly five fields (minute hour day month weekday): no seconds field, so nothing fires
    more often than once a minute. croniter's strict mode also rejects dates that can never
    happen, such as 31 February.
    """
    if len(expression.split()) != 5:
        return "use five fields: minute hour day-of-month month day-of-week"
    if not croniter.is_valid(expression, strict=True):
        return "not a valid cron expression"
    return None


def timezone_error(name: str) -> str | None:
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return f"unknown time zone {name!r}; use an IANA name such as Europe/London"
    return None


def next_run(expression: str, timezone: str, after: datetime) -> datetime:
    """The first fire time strictly after `after`, as an aware UTC datetime.

    The schedule steps through local wall-clock time, so each wall-clock time fires at
    most once. When clocks go back, a 01:30 schedule fires once, at the first 01:30 (aware
    iteration would fire it twice). When clocks go forward, a time inside the gap, such as
    02:30, fires as soon as the gap ends (03:30). Hourly schedules therefore skip the
    repeated hour in autumn.
    """
    zone = ZoneInfo(timezone)
    local = after.astimezone(zone).replace(tzinfo=None)
    walk = croniter(expression, local)
    while True:
        # fold=0: of two identical local times the first; inside a gap, the time after it.
        fire = walk.get_next(datetime).replace(tzinfo=zone).astimezone(UTC)
        if fire > after:  # inside a repeated hour, a wall-clock time can map to the past
            return fire
