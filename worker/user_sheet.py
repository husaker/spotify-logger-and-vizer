from __future__ import annotations

from datetime import datetime, timezone

import gspread

LOG_TAB = "log"
APP_STATE_TAB = "__app_state"
DEDUPE_TAB = "__dedupe"
CACHE_TRACKS_TAB = "__cache_tracks"
CACHE_ARTISTS_TAB = "__cache_artists"
CACHE_ALBUMS_TAB = "__cache_albums"

LOG_HEADERS = ["Date", "Track", "Artist", "Spotify ID", "URL"]
APP_STATE_HEADERS = ["key", "value"]
DEDUPE_HEADERS = ["dedupe_key"]

CACHE_TRACKS_HEADERS = [
    "track_id",
    "track_name",
    "duration_ms",
    "album_id",
    "album_cover_url",
    "primary_artist_id",
    "artist_ids",
    "track_url",
    "fetched_at",
]
CACHE_ARTISTS_HEADERS = [
    "artist_id",
    "artist_name",
    "artist_cover_url",
    "genres",
    "primary_genre",
    "fetched_at",
]
CACHE_ALBUMS_HEADERS = [
    "album_id",
    "album_name",
    "album_cover_url",
    "release_date",
    "fetched_at",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_or_create_ws(ss: gspread.Spreadsheet, title: str, rows: int = 2000, cols: int = 20) -> gspread.Worksheet:
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def ensure_headers(ws: gspread.Worksheet, headers: list[str]) -> None:
    row1 = ws.row_values(1)
    if row1 != headers:
        ws.update(f"A1:{chr(ord('A') + len(headers) - 1)}1", [headers])


def ensure_app_state_defaults(ws: gspread.Worksheet, timezone_name: str = "UTC") -> None:
    """
    __app_state is a 2-column key/value table.
    We'll upsert defaults if missing.
    """
    ensure_headers(ws, APP_STATE_HEADERS)

    values = ws.get_all_values()
    existing = {}
    for r in values[1:]:
        if len(r) >= 2 and r[0].strip():
            existing[r[0].strip()] = r[1].strip() if len(r) > 1 else ""

    defaults = {
        "enabled": "false",
        "timezone": timezone_name,
        "last_synced_after_ts": "0",
        "spotify_user_id": "",
        "refresh_token_enc": "",
        "created_at": existing.get("created_at") or _now_iso(),
        "updated_at": _now_iso(),
        "last_error": "",
    }

    # Upsert: easiest strategy = rewrite whole sheet (small table)
    rows = [["key", "value"]] + [[k, v] for k, v in defaults.items()]
    ws.clear()
    ws.update("A1:B1", [APP_STATE_HEADERS])
    ws.update("A2:B{}".format(len(rows)), rows[1:])


def ensure_user_sheet_initialized(
    ss: gspread.Spreadsheet,
    timezone_name: str = "UTC",
) -> None:
    # log
    ws_log = get_or_create_ws(ss, LOG_TAB, rows=5000, cols=10)
    ensure_headers(ws_log, LOG_HEADERS)

    # app state
    ws_state = get_or_create_ws(ss, APP_STATE_TAB, rows=200, cols=2)
    ensure_app_state_defaults(ws_state, timezone_name=timezone_name)

    # dedupe
    ws_dedupe = get_or_create_ws(ss, DEDUPE_TAB, rows=5000, cols=1)
    ensure_headers(ws_dedupe, DEDUPE_HEADERS)

    # caches
    ws_ct = get_or_create_ws(ss, CACHE_TRACKS_TAB, rows=5000, cols=20)
    ensure_headers(ws_ct, CACHE_TRACKS_HEADERS)

    ws_ca = get_or_create_ws(ss, CACHE_ARTISTS_TAB, rows=5000, cols=20)
    ensure_headers(ws_ca, CACHE_ARTISTS_HEADERS)

    ws_calb = get_or_create_ws(ss, CACHE_ALBUMS_TAB, rows=5000, cols=20)
    ensure_headers(ws_calb, CACHE_ALBUMS_HEADERS)