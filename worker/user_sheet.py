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


def ensure_headers_strict(ws: gspread.Worksheet, headers: list[str]) -> None:
    row1 = ws.row_values(1)
    if row1 != headers:
        ws.update(f"A1:{chr(ord('A') + len(headers) - 1)}1", [headers])


def ensure_ws_with_headers_versioned(
    ss: gspread.Spreadsheet,
    title: str,
    headers: list[str],
    rows: int = 2000,
    cols: int = 20,
) -> gspread.Worksheet:
    """
    Safe for user-owned sheets:
    - if sheet missing -> create and set headers
    - if headers match -> return it
    - if headers differ:
        - if sheet has no data beyond header -> overwrite header
        - else create title_v2/title_v3... and set headers there
    """
    ws = get_or_create_ws(ss, title, rows=rows, cols=cols)
    existing = ws.row_values(1)

    if existing == headers:
        return ws

    all_values = ws.get_all_values()
    has_data = any(any((c or "").strip() for c in r) for r in all_values[1:]) if len(all_values) > 1 else False

    if not has_data:
        ws.update(f"A1:{chr(ord('A') + len(headers) - 1)}1", [headers])
        return ws

    ver = 2
    while True:
        title2 = f"{title}_v{ver}"
        ws2 = get_or_create_ws(ss, title2, rows=rows, cols=cols)
        existing2 = ws2.row_values(1)
        if not existing2 or existing2 == headers:
            ws2.update(f"A1:{chr(ord('A') + len(headers) - 1)}1", [headers])
            return ws2
        ver += 1


def ensure_app_state_defaults(ws: gspread.Worksheet, timezone_name: str = "UTC") -> None:
    ensure_headers_strict(ws, APP_STATE_HEADERS)

    values = ws.get_all_values()
    existing: dict[str, str] = {}
    for r in values[1:]:
        if len(r) >= 2 and (r[0] or "").strip():
            existing[(r[0] or "").strip()] = (r[1] or "").strip()

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

    rows = [["key", "value"]] + [[k, v] for k, v in defaults.items()]
    ws.clear()
    ws.update("A1:B1", [APP_STATE_HEADERS])
    ws.update(f"A2:B{len(rows)}", rows[1:])


def ensure_user_sheet_initialized(ss: gspread.Spreadsheet, timezone_name: str = "UTC") -> None:
    # log
    ws_log = get_or_create_ws(ss, LOG_TAB, rows=5000, cols=10)
    ensure_headers_strict(ws_log, LOG_HEADERS)

    # app state
    ws_state = get_or_create_ws(ss, APP_STATE_TAB, rows=200, cols=2)
    ensure_app_state_defaults(ws_state, timezone_name=timezone_name)

    # dedupe
    ws_dedupe = get_or_create_ws(ss, DEDUPE_TAB, rows=5000, cols=1)
    ensure_headers_strict(ws_dedupe, DEDUPE_HEADERS)

    # caches (safe / versioned)
    ensure_ws_with_headers_versioned(ss, CACHE_TRACKS_TAB, CACHE_TRACKS_HEADERS, rows=5000, cols=20)
    ensure_ws_with_headers_versioned(ss, CACHE_ARTISTS_TAB, CACHE_ARTISTS_HEADERS, rows=5000, cols=20)
    ensure_ws_with_headers_versioned(ss, CACHE_ALBUMS_TAB, CACHE_ALBUMS_HEADERS, rows=5000, cols=20)