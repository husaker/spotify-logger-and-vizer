from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo


MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


def format_spotify_played_at(
    played_at_iso_utc: str,
    timezone: str,
) -> str:
    """Format Spotify played_at (UTC ISO8601) into required string.

    Example: "November 12, 2025 at 10:42AM".
    """

    dt_utc = dt.datetime.fromisoformat(played_at_iso_utc.replace("Z", "+00:00"))
    tz = ZoneInfo(timezone)
    dt_local = dt_utc.astimezone(tz)

    month_name = MONTH_NAMES[dt_local.month - 1]
    day = dt_local.day
    year = dt_local.year

    hour_24 = dt_local.hour
    minute = dt_local.minute

    if hour_24 == 0:
        hour_12 = 12
        ampm = "AM"
    elif 1 <= hour_24 < 12:
        hour_12 = hour_24
        ampm = "AM"
    elif hour_24 == 12:
        hour_12 = 12
        ampm = "PM"
    else:
        hour_12 = hour_24 - 12
        ampm = "PM"

    return f"{month_name} {day}, {year} at {hour_12}:{minute:02d}{ampm}"


def now_iso_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def iso_to_timestamp_ms(iso_str: str) -> int:
    dt_obj = dt.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt_obj.timestamp() * 1000)
