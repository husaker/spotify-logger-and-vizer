from __future__ import annotations

import argparse
import os
import sys
from typing import List

import gspread
from dotenv import load_dotenv

# Ensure project root is on sys.path so that `common`, `app`, `worker` imports work
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Load .env from project root if present
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from app.crypto import decrypt_token
from app.date_format import format_spotify_played_at, iso_to_timestamp_ms, now_iso_utc
from app.sheets_client import (
    LOG_HEADERS,
    LOG_SHEET_TITLE,
    SheetsClient,
    ensure_user_sheet_initialized,
    get_app_state,
    get_registry_client,
)
from common.config import AppConfig, load_config
from worker.cache import (
    get_albums_cache,
    get_artists_cache,
    get_tracks_cache,
    upsert_albums_cache,
    upsert_artists_cache,
    upsert_tracks_cache,
)
from worker.dedupe import append_dedupe_keys, load_recent_dedupe_keys, make_dedupe_key
from worker.spotify_api import (
    batch_fetch_albums,
    batch_fetch_artists,
    batch_fetch_tracks,
    fetch_recently_played,
)
from app.spotify_auth import refresh_access_token


def sync_user_sheet(config: AppConfig, client: SheetsClient, spreadsheet_id: str) -> None:
    ensure_user_sheet_initialized(client, spreadsheet_id)
    app_state = get_app_state(client, spreadsheet_id)

    if app_state.get("enabled", "false").lower() != "true":
        return

    refresh_token_enc = app_state.get("refresh_token_enc", "")
    spotify_user_id = app_state.get("spotify_user_id", "")
    if not refresh_token_enc or not spotify_user_id:
        raise RuntimeError("Spotify не подключён для этой таблицы (нет токена/пользователя)")

    refresh_token = decrypt_token(config.fernet_key, refresh_token_enc)
    access_token = refresh_access_token(config, refresh_token)

    last_synced_after_ts = int(app_state.get("last_synced_after_ts", "0"))
    lookback_ms = config.sync_lookback_minutes * 60 * 1000
    after_ms = max(0, last_synced_after_ts - lookback_ms)

    played_items = fetch_recently_played(
        access_token=access_token,
        after_ms=after_ms if after_ms > 0 else None,
        page_limit=config.sync_page_limit,
        max_pages=config.max_pages_per_run,
    )

    if not played_items:
        app_state["updated_at"] = now_iso_utc()
        return

    # Attach spotify_user_id to played items
    for item in played_items:
        item.spotify_user_id = spotify_user_id

    dedupe_keys_existing = load_recent_dedupe_keys(
        client,
        spreadsheet_id,
        config.dedupe_read_rows,
    )

    # Prepare log rows & dedupe keys
    ss = client.open_by_id(spreadsheet_id)
    try:
        log_ws = ss.worksheet(LOG_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        log_ws = client.get_or_create_worksheet(spreadsheet_id, LOG_SHEET_TITLE)
        client.ensure_headers(log_ws, LOG_HEADERS)

    timezone = app_state.get("timezone", "UTC")

    new_log_rows: List[List[str]] = []
    new_dedupe_keys: List[str] = []
    max_played_ts = last_synced_after_ts

    for item in played_items:
        key = make_dedupe_key(spotify_user_id, item.played_at, item.track_id)
        if key in dedupe_keys_existing:
            continue

        ts_ms = iso_to_timestamp_ms(item.played_at)
        if ts_ms <= last_synced_after_ts:
            # still keep due to lookback, but dedupe will skip duplicates
            pass
        if ts_ms > max_played_ts:
            max_played_ts = ts_ms

        date_str = format_spotify_played_at(item.played_at, timezone)
        track_name = item.track_name
        artist = ", ".join(item.artist_names)
        track_id = item.track_id
        url = f"https://open.spotify.com/track/{track_id}"

        new_log_rows.append([date_str, track_name, artist, track_id, url])
        new_dedupe_keys.append(key)

    if new_log_rows:
        client.append_rows(log_ws, new_log_rows)
        append_dedupe_keys(client, spreadsheet_id, new_dedupe_keys)

    # Metadata cache
    track_ids = list({row[3] for row in new_log_rows if len(row) >= 4})
    if track_ids:
        tracks_cache, missing_tracks = get_tracks_cache(
            client,
            spreadsheet_id,
            config.cache_ttl_days,
        )
        need_track_ids = list({tid for tid in track_ids if tid not in tracks_cache or tid in missing_tracks})

        if need_track_ids:
            tracks_data = batch_fetch_tracks(access_token, need_track_ids)
        else:
            tracks_data = {}

        # From tracks, derive albums and artists
        album_ids: List[str] = []
        artist_ids: List[str] = []
        track_records = {}
        now = now_iso_utc()
        for tid in track_ids:
            track = tracks_cache.get(tid) or tracks_data.get(tid)
            if not track:
                continue
            album = track.get("album") or {}
            artists_raw = track.get("artists") or []
            primary_artist_id = str(artists_raw[0]["id"]) if artists_raw else ""
            a_ids = [str(a.get("id", "")) for a in artists_raw if a.get("id")]
            album_id = str(album.get("id", ""))
            album_ids.append(album_id)
            artist_ids.extend(a_ids)
            images = album.get("images") or []
            album_cover_url = images[0]["url"] if images else ""
            track_records[tid] = {
                "track_id": tid,
                "track_name": str(track.get("name", "")),
                "duration_ms": str(track.get("duration_ms", "")),
                "album_id": album_id,
                "album_cover_url": album_cover_url,
                "primary_artist_id": primary_artist_id,
                "artist_ids": ",".join(a_ids),
                "track_url": f"https://open.spotify.com/track/{tid}",
                "fetched_at": now,
            }

        if track_records:
            upsert_tracks_cache(client, spreadsheet_id, track_records)

        # Albums
        album_ids_unique = list({a for a in album_ids if a})
        albums_cache, missing_albums = get_albums_cache(
            client,
            spreadsheet_id,
            config.cache_ttl_days,
        )
        need_album_ids = [
            aid for aid in album_ids_unique if aid not in albums_cache or aid in missing_albums
        ]
        album_records = {}
        if need_album_ids:
            albums_data = batch_fetch_albums(access_token, need_album_ids)
            now = now_iso_utc()
            for aid, album in albums_data.items():
                images = album.get("images") or []
                album_cover_url = images[0]["url"] if images else ""
                album_records[aid] = {
                    "album_id": aid,
                    "album_name": str(album.get("name", "")),
                    "album_cover_url": album_cover_url,
                    "release_date": str(album.get("release_date", "")),
                    "fetched_at": now,
                }
        if album_records:
            upsert_albums_cache(client, spreadsheet_id, album_records)

        # Artists
        artist_ids_unique = list({a for a in artist_ids if a})
        artists_cache, missing_artists = get_artists_cache(
            client,
            spreadsheet_id,
            config.cache_ttl_days,
        )
        need_artist_ids = [
            aid for aid in artist_ids_unique if aid not in artists_cache or aid in missing_artists
        ]
        artist_records = {}
        if need_artist_ids:
            artists_data = batch_fetch_artists(access_token, need_artist_ids)
            now = now_iso_utc()
            for aid, artist in artists_data.items():
                images = artist.get("images") or []
                artist_cover_url = images[0]["url"] if images else ""
                genres_list = [str(g) for g in (artist.get("genres") or [])]
                primary_genre = genres_list[0] if genres_list else ""
                artist_records[aid] = {
                    "artist_id": aid,
                    "artist_name": str(artist.get("name", "")),
                    "artist_cover_url": artist_cover_url,
                    "genres": ",".join(genres_list),
                    "primary_genre": primary_genre,
                    "fetched_at": now,
                }
        if artist_records:
            upsert_artists_cache(client, spreadsheet_id, artist_records)

    # Update app_state timestamps
    if max_played_ts > last_synced_after_ts:
        app_state["last_synced_after_ts"] = str(max_played_ts)
    app_state["updated_at"] = now_iso_utc()

    # Clear last_error on success
    app_state["last_error"] = ""

    from app.sheets_client import update_app_state

    update_app_state(client, spreadsheet_id, app_state)


def run_single_sheet_sync(config: AppConfig, spreadsheet_id: str) -> None:
    client = SheetsClient.from_config(config)
    sync_user_sheet(config, client, spreadsheet_id)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Spotify Track Logger worker")
    parser.add_argument("--once", action="store_true", help="Run sync once and exit")
    parser.add_argument("--sheet", type=str, help="Specific user sheet ID to sync", default="")

    args = parser.parse_args(argv)

    config = load_config()
    client = SheetsClient.from_config(config)
    registry = get_registry_client(client, config.registry_sheet_id)

    def sync_one(sheet_id: str) -> None:
        try:
            sync_user_sheet(config, client, sheet_id)
            registry.update_sync_result(sheet_id, last_sync_at=now_iso_utc(), last_error="")
        except Exception as exc:  # noqa: BLE001
            registry.update_sync_result(sheet_id, last_sync_at=None, last_error=str(exc))

    if args.sheet:
        sync_one(args.sheet)
        return 0

    if not args.once:
        # For now, support only --once explicitly; could be extended to loop.
        args.once = True

    enabled_users = registry.list_enabled_users()
    for sheet_id in enabled_users:
        sync_one(sheet_id)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
