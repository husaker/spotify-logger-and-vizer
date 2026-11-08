import time
from .timestamps import parse_ms
from .http import ApiError


def health_status(state, now_ms=None):
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    synced = parse_ms(state.get("last_sync_at", ""))
    if state.get("reauth_required") == "true":
        return False, "Spotify needs reauthorization"
    if synced is None:
        return False, "No successful synchronization recorded"
    if synced > now_ms + 60_000:
        return False, "Invalid future synchronization timestamp"
    if now_ms - synced >= 30 * 60_000:
        return False, "No successful synchronization for at least 30 minutes"
    return True, "Synchronization is fresh"


def check_health(sheets):
    healthy, reason = health_status(sheets.read_state())
    if not healthy:
        raise ApiError(reason)
