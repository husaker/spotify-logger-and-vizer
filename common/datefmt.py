from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def format_spotify_played_at(played_at_iso: str, timezone_name: str) -> str:
    """
    Input: 2025-11-12T10:42:00.123Z
    Output: November 12, 2025 at 10:42AM
    """
    # Spotify gives UTC ISO ending with Z
    dt_utc = datetime.fromisoformat(played_at_iso.replace("Z", "+00:00"))
    tz = ZoneInfo(timezone_name)
    dt_local = dt_utc.astimezone(tz)

    # Month day, year at H:MMAM
    return dt_local.strftime("%B %-d, %Y at %-I:%M%p")