import random
from collections.abc import Callable

# 2**62 is far beyond any cap; clamping the exponent keeps the float maths from overflowing.
_MAX_EXPONENT = 62


def full_jitter_delay(
    attempt: int,
    base: float,
    cap: float,
    rand: Callable[[], float] = random.random,
) -> float:
    """Delay after failed attempt n (1-based): uniform(0, min(cap, base * 2^(n-1))) seconds.

    "Full jitter" from Marc Brooker's AWS Architecture Blog post: spreading retries over the
    whole window stops jobs that failed together from retrying together in waves.
    """
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    ceiling = min(cap, base * 2.0 ** min(attempt - 1, _MAX_EXPONENT))
    return rand() * ceiling
