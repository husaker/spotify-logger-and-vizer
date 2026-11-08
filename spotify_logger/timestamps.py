"""Normalize event timestamps to the same UTC millisecond precision as JS Date."""
from datetime import datetime, timedelta, timezone
import re

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MONTHS = {name: index + 1 for index, name in enumerate(
    "january february march april may june july august september october november december".split())}
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T", re.I)
LEGACY = re.compile(r"^(\w+)\s+(\d{1,2}),\s+(\d{4})\s+at\s+(\d{1,2}):(\d{2})(AM|PM)$", re.I)


def parse_ms(value):
    if not isinstance(value, str):
        return None
    try:
        if ISO.match(value):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00").replace("z", "+00:00"))
            if dt.tzinfo is None:
                return None
        else:
            match = LEGACY.fullmatch(value)
            if not match:
                return None
            month, day, year, hour, minute, period = match.groups()
            hour = int(hour)
            if not 1 <= hour <= 12:
                return None
            dt = datetime(int(year), MONTHS[month.lower()], int(day),
                          hour % 12 + (12 if period.upper() == "PM" else 0), int(minute),
                          tzinfo=timezone(timedelta(hours=3)))
        delta = dt.astimezone(UTC) - EPOCH
        return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
    except (ValueError, KeyError, OverflowError):
        return None


def iso_ms(timestamp):
    return (EPOCH + timedelta(milliseconds=timestamp)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_iso():
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class TimestampGuard:
    def __init__(self, rows):
        self.exact = set()
        self.legacy_minutes = set()
        for row in rows:
            if not row:
                continue
            timestamp = parse_ms(row[0])
            if timestamp is not None:
                if ISO.match(row[0]):
                    self.exact.add(timestamp)
                else:
                    self.legacy_minutes.add(timestamp // 60_000)

    def claim(self, value):
        timestamp = parse_ms(value)
        if timestamp is None or timestamp in self.exact or timestamp // 60_000 in self.legacy_minutes:
            return None
        self.exact.add(timestamp)
        return iso_ms(timestamp)
