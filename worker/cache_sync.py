from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import gspread

from app.spotify_api import get_albums, get_artists, get_tracks

CACHE_TRACKS_TAB = "__cache_tracks"
CACHE_ARTISTS_TAB = "__cache_artists"
CACHE_ALBUMS_TAB = "__cache_albums"

# Your current schemas (must match user_sheet.py)
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


def _is_stale(fetched_at: str, ttl_days: int) -> bool:
    if not fetched_at:
        return True
    try:
        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    except Exception:
        return True
    return dt < (datetime.now(timezone.utc) - timedelta(days=ttl_days))


def _find_ws_by_schema(ss: gspread.Spreadsheet, base_title: str, headers: list[str]) -> gspread.Worksheet:
    """
    Finds worksheet among base_title / base_title_vN where row1 == headers.
    This is required because user_sheet.py may create versioned cache sheets.
    """
    candidates: list[gspread.Worksheet] = []
    for w in ss.worksheets():
        t = w.title
        if t == base_title or t.startswith(base_title + "_v"):
            candidates.append(w)

    # Prefer base first, then versions
    candidates.sort(key=lambda w: (w.title != base_title, w.title))

    for w in candidates:
        if w.row_values(1) == headers:
            return w

    raise RuntimeError(
        f"Cache sheet not found for {base_title} with expected headers. "
        f"Candidates: {[w.title for w in candidates]}"
    )


def _load_key_to_row_and_fetched(ws: gspread.Worksheet, fetched_col_1based: int) -> tuple[dict[str, int], dict[str, str]]:
    """
    Returns:
      key_to_row: id -> row index (1-based)
      key_to_fetched_at: id -> fetched_at string
    """
    rows = ws.get_all_values()
    key_to_row: dict[str, int] = {}
    key_to_fetched: dict[str, str] = {}

    for i, r in enumerate(rows[1:], start=2):  # skip header
        if not r:
            continue
        key = (r[0] or "").strip()
        if not key:
            continue
        key_to_row[key] = i
        fetched = r[fetched_col_1based - 1] if len(r) >= fetched_col_1based else ""
        key_to_fetched[key] = (fetched or "").strip()

    return key_to_row, key_to_fetched

def _a1_row_range(row_idx: int, ncols: int) -> str:
    end_col = chr(ord("A") + ncols - 1)
    return f"A{row_idx}:{end_col}{row_idx}"


def _batch_update_rows(ws: gspread.Worksheet, updates: list[tuple[int, list[Any]]], *, chunk_size: int = 200) -> None:
    """
    updates: list of (row_idx, values)
    Sends in chunks to avoid oversized requests.
    """
    if not updates:
        return

    for i in range(0, len(updates), chunk_size):
        chunk = updates[i : i + chunk_size]
        data = [{"range": _a1_row_range(row_idx, len(values)), "values": [values]} for row_idx, values in chunk]
        # value_input_option applies to the entire batch
        ws.batch_update(data, value_input_option="RAW")

def _upsert(ws: gspread.Worksheet, key_to_row: dict[str, int], rows: list[list[Any]]) -> None:
    """
    Efficient upsert:
      - existing rows -> batch_update (few requests)
      - new rows -> append_rows (1 request)
    """
    to_append: list[list[Any]] = []
    to_update: list[tuple[int, list[Any]]] = []

    for values in rows:
        key = (values[0] or "").strip()
        if not key:
            continue
        if key in key_to_row:
            to_update.append((key_to_row[key], values))
        else:
            to_append.append(values)

    # Batch update existing
    _batch_update_rows(ws, to_update, chunk_size=200)

    # Append new in one request
    if to_append:
        ws.append_rows(to_append, value_input_option="RAW")


def enrich_caches_for_tracks(
    ss: gspread.Spreadsheet,
    *,
    access_token: str,
    track_ids: list[str],
    ttl_days: int,
) -> None:
    """
    Enriches caches for given track_ids:
      - __cache_tracks (track metadata)
      - __cache_artists (artist cover + genres)
      - __cache_albums (album cover + release_date)

    Uses schemas above; finds the correct versioned worksheet by matching headers.
    """
    if not track_ids:
        return

    # Find correct cache sheets (base or versioned)
    ws_tracks = _find_ws_by_schema(ss, CACHE_TRACKS_TAB, CACHE_TRACKS_HEADERS)
    ws_artists = _find_ws_by_schema(ss, CACHE_ARTISTS_TAB, CACHE_ARTISTS_HEADERS)
    ws_albums = _find_ws_by_schema(ss, CACHE_ALBUMS_TAB, CACHE_ALBUMS_HEADERS)

    tracks_row, tracks_fetched = _load_key_to_row_and_fetched(ws_tracks, fetched_col_1based=9)

    # filter stale/missing tracks
    uniq_track_ids = sorted(set(tid for tid in track_ids if tid))
    need_tracks = [tid for tid in uniq_track_ids if _is_stale(tracks_fetched.get(tid, ""), ttl_days)]
    if not need_tracks:
        return

    now = _now_iso()

    # Fetch tracks (50 ids per call)
    fetched_tracks: list[dict[str, Any]] = []
    for i in range(0, len(need_tracks), 50):
        chunk = need_tracks[i : i + 50]
        fetched_tracks.extend(get_tracks(access_token, chunk))

    track_rows: list[list[Any]] = []
    artist_ids_set: set[str] = set()
    album_ids_set: set[str] = set()

    for t in fetched_tracks:
        if not t:
            continue

        track_id = (t.get("id") or "").strip()
        if not track_id:
            continue

        track_name = t.get("name") or ""
        duration_ms = t.get("duration_ms")
        duration_ms_str = str(duration_ms) if duration_ms is not None else ""

        album = t.get("album") or {}
        album_id = (album.get("id") or "").strip()
        if album_id:
            album_ids_set.add(album_id)

        images = album.get("images") or []
        album_cover_url = images[0].get("url") if images else ""

        artists = t.get("artists") or []
        artist_ids = [((a.get("id") or "").strip()) for a in artists if (a.get("id") or "").strip()]
        if artist_ids:
            artist_ids_set.update(artist_ids)
        primary_artist_id = artist_ids[0] if artist_ids else ""

        artist_ids_str = ";".join(artist_ids)

        external_urls = t.get("external_urls") or {}
        track_url = external_urls.get("spotify") or f"https://open.spotify.com/track/{track_id}"

        track_rows.append(
            [
                track_id,
                track_name,
                duration_ms_str,
                album_id,
                album_cover_url,
                primary_artist_id,
                artist_ids_str,
                track_url,
                now,
            ]
        )

    # Upsert tracks
    _upsert(ws_tracks, tracks_row, track_rows)

    # ===== artists cache =====
    artists_row, artists_fetched = _load_key_to_row_and_fetched(ws_artists, fetched_col_1based=6)
    need_artists = [aid for aid in sorted(artist_ids_set) if _is_stale(artists_fetched.get(aid, ""), ttl_days)]

    if need_artists:
        fetched_artists: list[dict[str, Any]] = []
        for i in range(0, len(need_artists), 50):
            chunk = need_artists[i : i + 50]
            fetched_artists.extend(get_artists(access_token, chunk))

        artist_rows: list[list[Any]] = []
        for a in fetched_artists:
            if not a:
                continue

            artist_id = (a.get("id") or "").strip()
            if not artist_id:
                continue

            artist_name = a.get("name") or ""
            images = a.get("images") or []
            artist_cover_url = images[0].get("url") if images else ""

            genres = a.get("genres") or []
            genres_str = "; ".join(genres)
            primary_genre = genres[0] if genres else ""

            artist_rows.append([artist_id, artist_name, artist_cover_url, genres_str, primary_genre, now])

        _upsert(ws_artists, artists_row, artist_rows)

    # ===== albums cache =====
    albums_row, albums_fetched = _load_key_to_row_and_fetched(ws_albums, fetched_col_1based=5)
    need_albums = [alb for alb in sorted(album_ids_set) if _is_stale(albums_fetched.get(alb, ""), ttl_days)]

    if need_albums:
        fetched_albums: list[dict[str, Any]] = []
        # albums endpoint supports up to 20 ids
        for i in range(0, len(need_albums), 20):
            chunk = need_albums[i : i + 20]
            fetched_albums.extend(get_albums(access_token, chunk))

        album_rows: list[list[Any]] = []
        for al in fetched_albums:
            if not al:
                continue

            album_id = (al.get("id") or "").strip()
            if not album_id:
                continue

            album_name = al.get("name") or ""
            images = al.get("images") or []
            album_cover_url = images[0].get("url") if images else ""
            release_date = al.get("release_date") or ""

            album_rows.append([album_id, album_name, album_cover_url, release_date, now])

        _upsert(ws_albums, albums_row, album_rows)