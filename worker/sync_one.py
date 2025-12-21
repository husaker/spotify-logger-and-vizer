from __future__ import annotations

from datetime import datetime, timezone

import gspread

from app.crypto import decrypt_str
from app.spotify_api import get_recently_played
from common.datefmt import format_spotify_played_at
from worker.app_state import read_app_state, write_app_state_kv
from worker.dedupe import load_dedupe_set, append_dedupe_keys


LOG_TAB = "log"
DEDUPE_TAB = "__dedupe"


def sync_user_sheet(
    ss: gspread.Spreadsheet,
    *,
    dedup_read_rows: int,
    lookback_minutes: int,
    fernet_key: str,
    spotify_client_id: str,
    spotify_client_secret: str,
) -> int:
    """
    Returns number of appended log rows.
    """
    state = read_app_state(ss)
    if state.get("enabled", "false").lower() != "true":
        return 0

    timezone_name = state.get("timezone") or "UTC"
    refresh_token_enc = state.get("refresh_token_enc") or ""
    if not refresh_token_enc:
        # Not connected yet
        return 0

    last_after = int(state.get("last_synced_after_ts") or "0")

    # lookback: if last_after is 0, set after to now - lookback
    if last_after == 0:
        after_ms = int((datetime.now(timezone.utc).timestamp() * 1000) - lookback_minutes * 60 * 1000)
    else:
        after_ms = max(0, last_after - lookback_minutes * 60 * 1000)

    refresh_token = decrypt_str(refresh_token_enc, fernet_key)

    items = get_recently_played(
        client_id=spotify_client_id,
        client_secret=spotify_client_secret,
        refresh_token=refresh_token,
        after_ms=after_ms,
        limit=50,
    )

    if not items:
        return 0

    dedupe = load_dedupe_set(ss, max_rows=dedup_read_rows)
    ws_log = ss.worksheet(LOG_TAB)
    ws_ded = ss.worksheet(DEDUPE_TAB)

    new_rows: list[list[str]] = []
    new_keys: list[str] = []

    max_played_ms = last_after

    for it in items:
        # dedupe key: played_at + track_id
        key = f"{it.played_at}|{it.track_id}"
        if key in dedupe:
            continue

        date_str = format_spotify_played_at(it.played_at, timezone_name)
        new_rows.append([date_str, it.track_name, it.artist_name, it.track_id, it.track_url])
        new_keys.append(key)
        dedupe.add(key)

        played_ms = int(datetime.fromisoformat(it.played_at.replace("Z", "+00:00")).timestamp() * 1000)
        if played_ms > max_played_ms:
            max_played_ms = played_ms

    if new_rows:
        ws_log.append_rows(new_rows, value_input_option="RAW")
        append_dedupe_keys(ws_ded, new_keys)
        write_app_state_kv(ss, {"last_synced_after_ts": str(max_played_ms), "last_error": ""})
        return len(new_rows)

    return 0