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


def _is_retryable_api_error(code: int | None, e: APIError) -> bool:
    # стандартные временные
    if code in (429, 500, 502, 503, 504):
        return True

    # иногда status_code может быть None, но текст явно про quota/rate limit
    msg = str(e).lower()
    if "quota exceeded" in msg or "rate limit" in msg or "user-rate limit" in msg:
        return True

    return False


def _sleep_decorrelated_jitter(
    attempt: int,
    *,
    prev_sleep: float,
    base: float,
    cap: float,
) -> float:
    """
    Decorrelated jitter backoff:
    sleep = min(cap, random(base, prev_sleep*3))
    """
    if attempt <= 1:
        s = base
    else:
        s = min(cap, random.uniform(base, prev_sleep * 3.0))
    time.sleep(max(0.0, s))
    return s


def gcall(fn: Callable[[], T], *, attempts: int = 8) -> T:
    """
    Retry for Google Sheets via gspread:
      - APIError 429/5xx + quota/rate-limit text
      - transient exceptions (network hiccups)
    Uses:
      - Retry-After if present
      - decorrelated jitter backoff
      - stronger backoff for 429 (minute-based read quota)
    """
    last: Exception | None = None

    prev_sleep = 0.0

    for attempt in range(1, attempts + 1):
        try:
            return fn()

        except APIError as e:
            last = e
            code = _status_code(e)

            if attempt >= attempts or not _is_retryable_api_error(code, e):
                raise

            ra = _retry_after(e)
            if ra is not None and ra > 0:
                # Spotify/Sheets иногда дают Retry-After
                sleep_s = min(ra, 90.0)
                time.sleep(sleep_s)
                prev_sleep = max(prev_sleep, sleep_s)
                continue

            # 429 на Sheets часто требует более длинных пауз
            if code == 429:
                base = 4.0
                cap = 90.0
            else:
                base = 1.0
                cap = 30.0

            prev_sleep = _sleep_decorrelated_jitter(
                attempt,
                prev_sleep=prev_sleep if prev_sleep > 0 else base,
                base=base,
                cap=cap,
            )

        except Exception as e:
            last = e
            if attempt >= attempts:
                raise

            # network hiccup: мягче, но тоже с jitter
            prev_sleep = _sleep_decorrelated_jitter(
                attempt,
                prev_sleep=prev_sleep if prev_sleep > 0 else 1.0,
                base=1.0,
                cap=30.0,
            )

    assert last is not None
    raise last