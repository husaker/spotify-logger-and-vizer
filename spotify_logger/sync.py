"""Collection plan and commit, kept separate for a safe shadow rollout."""
from dataclasses import dataclass
import time
from .http import ApiError, SpotifyInvalidGrant
from .timestamps import TimestampGuard, iso_ms, now_iso, parse_ms
from .tokens import select_token

RANGES = ["log!A2:E", "__cache_tracks!A2:I", "__cache_artists!A2:F", "__cache_albums!A2:E"]


def integer(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def cover(images):
    return (images or [{}])[0].get("url", "")


@dataclass
class Plan:
    log_rows: list
    tracks: list
    albums: list
    artist_ids: list
    cursor: int
    first_row: int
    fetched_events: int


def build_plan(items, tables, state, now):
    existing = tables[RANGES[0]]
    guard = TimestampGuard(existing)
    logs, tracks, albums, artists = [], {}, {}, {}
    cursor = integer(state.get("last_synced_after_ts"))
    valid = sorted((item for item in items if parse_ms(item.get("played_at")) is not None),
                   key=lambda item: parse_ms(item["played_at"]))
    for item in valid:
        track = item.get("track") or {}
        if not track.get("id"):
            continue
        timestamp = parse_ms(item["played_at"])
        cursor = max(cursor, timestamp)
        track_id = track["id"]
        album = track.get("album") or {}
        artist_list = track.get("artists") or []
        artist_ids = [artist["id"] for artist in artist_list if artist.get("id")]
        url = (track.get("external_urls") or {}).get("spotify") or f"https://open.spotify.com/track/{track_id}"
        tracks[track_id] = [track_id, track.get("name", ""), track.get("duration_ms", 0), album.get("id", ""),
                            cover(album.get("images")), artist_ids[0] if artist_ids else "", ";".join(artist_ids), url, now]
        if album.get("id"):
            albums[album["id"]] = [album["id"], album.get("name", ""), cover(album.get("images")), album.get("release_date", ""), now]
        for artist_id in artist_ids:
            artists[artist_id] = None
        played_at = guard.claim(item["played_at"])
        if played_at:
            logs.append([played_at, track.get("name", ""), artist_list[0].get("name", "") if artist_list else "", track_id, url])
    return Plan(logs, list(tracks.values()), list(albums.values()), list(artists), cursor, len(existing) + 2, len(items))


def _upsert(sheets, sheet, rows, existing, width):
    positions = {row[0]: i + 2 for i, row in enumerate(existing) if row and row[0]}
    updates, additions = [], []
    for row in rows:
        if row[0] in positions:
            position = positions[row[0]]
            updates.append({"range": f"{sheet}!A{position}:{chr(64 + width)}{position}", "values": [row]})
        else:
            additions.append(row)
    sheets.batch_write(updates)
    sheets.append(f"{sheet}!A:{chr(64 + width)}", additions)


def access_token(settings, sheets, spotify, state):
    codec, refresh = select_token(settings, state)
    tokens = spotify.refresh(refresh)
    if tokens.get("refresh_token"):
        # Even shadow runs preserve an OAuth rotation, so the working collector can
        # use it. This is the only mutation permitted by the credentials-only fence.
        sheets.write_state({codec.field_name: codec.encrypt(tokens["refresh_token"])})
    return tokens["access_token"]


def collect(settings, sheets, spotify, *, dry_run=False, now=None):
    state = sheets.read_state()
    try:
        token = access_token(settings, sheets, spotify, state)
        now = now or now_iso()
        cursor = integer(state.get("last_synced_after_ts"))
        after = max(0, (cursor or int(time.time() * 1000)) - 120 * 60_000)
        items = spotify.recent(token, after)
        tables = sheets.read_ranges(RANGES)
        plan = build_plan(items, tables, state, now)
        # Match the legacy writer's ordering: durable log first, optional caches next.
        if not dry_run:
            if plan.log_rows:
                last = plan.first_row + len(plan.log_rows) - 1
                sheets.ensure_log_rows(last)
                sheets.batch_write([{"range": f"log!A{plan.first_row}:E{last}", "values": plan.log_rows}])
            _upsert(sheets, "__cache_tracks", plan.tracks, tables[RANGES[1]], 9)
            _upsert(sheets, "__cache_albums", plan.albums, tables[RANGES[3]], 5)
        known = {row[0] for row in tables[RANGES[2]] if row and row[0]}
        artist_rows = []
        for artist_id in [value for value in plan.artist_ids if value not in known][:25]:
            artist = spotify.artist(token, artist_id)
            genres = artist.get("genres") or []
            artist_rows.append([artist_id, artist.get("name", ""), cover(artist.get("images")), "; ".join(genres), genres[0] if genres else "", now])
        if not dry_run:
            _upsert(sheets, "__cache_artists", artist_rows, tables[RANGES[2]], 6)
            sheets.write_state({"last_synced_after_ts": str(plan.cursor), "last_sync_at": now,
                                "last_error": "", "consecutive_failures": "0", "reauth_required": "false"})
        return {"added": 0 if dry_run else len(plan.log_rows), "would_add": len(plan.log_rows),
                "fetched_events": plan.fetched_events, "track_metadata": len(plan.tracks),
                "album_metadata": len(plan.albums), "artist_metadata": len(artist_rows), "last_sync_at": now}
    except Exception as error:
        if not dry_run:
            try:
                current = sheets.read_state()
                expired = isinstance(error, SpotifyInvalidGrant)
                changes = {"consecutive_failures": str(integer(current.get("consecutive_failures")) + 1),
                           "last_error": "Spotify reauthorization required" if expired else str(error) if isinstance(error, ApiError) else "Synchronization failed; inspect the Actions run"}
                if expired:
                    changes["reauth_required"] = "true"
                sheets.write_state(changes)
            except Exception:
                pass  # Never hide the primary failure or pretend collection succeeded.
        raise


def check_connection(settings, sheets, spotify):
    token = access_token(settings, sheets, spotify, sheets.read_state())
    spotify.recent(token, int(time.time() * 1000) - 120 * 60_000)
