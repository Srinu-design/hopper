import random

import pytest

from hopper.worker.backoff import full_jitter_delay


def test_ceiling_doubles_per_attempt() -> None:
    # rand() -> 1.0 gives the top of each window: base * 2^(n-1)
    ceilings = [full_jitter_delay(n, base=2, cap=600, rand=lambda: 1.0) for n in range(1, 6)]
    assert ceilings == [2, 4, 8, 16, 32]


def test_ceiling_is_capped() -> None:
    assert full_jitter_delay(20, base=2, cap=600, rand=lambda: 1.0) == 600


def test_window_starts_at_zero() -> None:
    assert full_jitter_delay(5, base=2, cap=600, rand=lambda: 0.0) == 0


def test_delays_stay_inside_the_window() -> None:
    rng = random.Random(1234)
    for attempt in range(1, 12):
        ceiling = min(600, 2 * 2 ** (attempt - 1))
        for _ in range(200):
            delay = full_jitter_delay(attempt, base=2, cap=600, rand=rng.random)
            assert 0 <= delay <= ceiling


def test_delays_are_spread_not_clustered() -> None:
    # Full jitter: across the window, not bunched at the top like "equal jitter" would be.
    rng = random.Random(99)
    delays = [full_jitter_delay(5, base=2, cap=600, rand=rng.random) for _ in range(1000)]
    assert min(delays) < 3.2 and max(delays) > 28.8  # bottom and top tenths of 0-32 s


def test_huge_attempt_numbers_do_not_overflow() -> None:
    assert full_jitter_delay(10_000, base=2, cap=600, rand=lambda: 1.0) == 600


def test_attempts_are_one_based() -> None:
    with pytest.raises(ValueError):
        full_jitter_delay(0, base=2, cap=600)
