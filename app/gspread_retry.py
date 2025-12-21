from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

from gspread.exceptions import APIError

T = TypeVar("T")


def _status_code(e: APIError) -> int | None:
    resp = getattr(e, "response", None)
    return getattr(resp, "status_code", None) if resp is not None else None


def _retry_after(e: APIError) -> float | None:
    resp = getattr(e, "response", None)
    if resp is None:
        return None
    headers = getattr(resp, "headers", {}) or {}
    ra = headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except ValueError:
        return None


def _sleep(attempt: int, base: float = 1.0, max_sleep: float = 30.0) -> None:
    s = min(max_sleep, base * (2 ** (attempt - 1)))
    s *= 1.0 + random.uniform(-0.25, 0.25)
    time.sleep(max(0.0, s))


def gcall(fn: Callable[[], T], *, attempts: int = 6) -> T:
    """
    Retry only retryable Google API errors: 429/500/502/503/504 + transient exceptions.
    """
    last: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return fn()

        except APIError as e:
            last = e
            code = _status_code(e)

            # non-retryable or last attempt
            if attempt >= attempts or code not in (429, 500, 502, 503, 504):
                raise

            ra = _retry_after(e)
            if ra is not None and ra > 0:
                time.sleep(min(ra, 60.0))
            else:
                _sleep(attempt)

        except Exception as e:
            # e.g. network hiccup
            last = e
            if attempt >= attempts:
                raise
            _sleep(attempt)

    assert last is not None
    raise last