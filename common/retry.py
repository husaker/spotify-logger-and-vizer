from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

T = TypeVar("T")


def _sleep_seconds(base: float, attempt: int, jitter: float = 0.25, max_sleep: float = 30.0) -> float:
    """
    Exponential backoff with jitter:
      sleep = min(max_sleep, base * 2^(attempt-1)) * (1 +/- jitter)
    attempt: 1..N
    """
    s = min(max_sleep, base * (2 ** (attempt - 1)))
    j = 1.0 + random.uniform(-jitter, jitter)
    return max(0.0, s * j)


def with_retry(
    fn: Callable[[], T],
    *,
    should_retry: Callable[[Exception], bool],
    get_retry_after_seconds: Callable[[Exception], float | None] | None = None,
    attempts: int = 5,
    base_sleep: float = 1.0,
) -> T:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if attempt >= attempts or not should_retry(e):
                raise

            ra = get_retry_after_seconds(e) if get_retry_after_seconds else None
            if ra is not None and ra > 0:
                time.sleep(min(ra, 60.0))
            else:
                time.sleep(_sleep_seconds(base_sleep, attempt))

    # should never reach here
    assert last_exc is not None
    raise last_exc