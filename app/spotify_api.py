from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import requests
from requests import Response

from common.retry import with_retry

RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"
TRACKS_URL = "https://api.spotify.com/v1/tracks"
ARTISTS_URL = "https://api.spotify.com/v1/artists"
ALBUMS_URL = "https://api.spotify.com/v1/albums"


@dataclass(frozen=True)
class PlayedItem:
    played_at: str
    track_id: str
    track_name: str
    artist_name: str
    track_url: str


def _retry_after_from_response(r: Response) -> float | None:
    ra = r.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except ValueError:
        return None


def _spotify_get_json(
    url: str,
    *,
    access_token: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Robust GET for Spotify Web API:
    - retries on 429 (uses Retry-After) and 5xx
    - retries on transient network errors
    - fails fast on 4xx (except 429)
    """

    def do() -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {access_token}"}
        r = requests.get(url, headers=headers, params=params, timeout=30)

        if 200 <= r.status_code < 300:
            return r.json() if r.text else {}

        # retryable
        if r.status_code == 429 or 500 <= r.status_code < 600:
            err = RuntimeError(f"Spotify retryable: {r.status_code} | {r.text}")
            setattr(err, "_response", r)
            raise err

        # non-retryable
        raise RuntimeError(f"Spotify failed: {r.status_code} | {r.text}")

    def should_retry(e: Exception) -> bool:
        # network/transient
        if isinstance(e, requests.RequestException):
            return True
        # our retryable marker
        return str(e).startswith("Spotify retryable:")

    def get_retry_after_seconds(e: Exception) -> float | None:
        r = getattr(e, "_response", None)
        if isinstance(r, Response):
            return _retry_after_from_response(r)
        return None

    return with_retry(
        do,
        should_retry=should_retry,
        get_retry_after_seconds=get_retry_after_seconds,
        attempts=5,
        base_sleep=1.0,
    )


def get_recently_played_with_access_token(
    access_token: str,
    after_ms: int,
    limit: int = 50,
) -> list[PlayedItem]:
    params: dict[str, Any] = {"limit": limit}
    if after_ms > 0:
        params["after"] = after_ms

    j = _spotify_get_json(RECENTLY_PLAYED_URL, access_token=access_token, params=params)

    out: list[PlayedItem] = []
    for it in (j.get("items") or []):
        track = it.get("track") or {}
        played_at = it.get("played_at") or ""
        track_id = (track.get("id") or "").strip()
        if not played_at or not track_id:
            continue

        track_name = track.get("name") or ""

        artists = track.get("artists") or []
        artist_name = artists[0].get("name") if artists else ""

        external_urls = track.get("external_urls") or {}
        track_url = external_urls.get("spotify") or f"https://open.spotify.com/track/{track_id}"

        out.append(
            PlayedItem(
                played_at=played_at,
                track_id=track_id,
                track_name=track_name,
                artist_name=artist_name,
                track_url=track_url,
            )
        )
    return out


def get_tracks(access_token: str, track_ids: list[str]) -> list[dict[str, Any]]:
    """
    Spotify supports up to 50 track ids per request.
    """
    track_ids = [t for t in track_ids if t]
    if not track_ids:
        return []

    j = _spotify_get_json(TRACKS_URL, access_token=access_token, params={"ids": ",".join(track_ids)})
    return (j.get("tracks") or [])


def get_artists(access_token: str, artist_ids: list[str]) -> list[dict[str, Any]]:
    """
    Spotify supports up to 50 artist ids per request.
    """
    artist_ids = [a for a in artist_ids if a]
    if not artist_ids:
        return []

    j = _spotify_get_json(ARTISTS_URL, access_token=access_token, params={"ids": ",".join(artist_ids)})
    return (j.get("artists") or [])


def get_albums(access_token: str, album_ids: list[str]) -> list[dict[str, Any]]:
    """
    Spotify supports up to 20 album ids per request.
    """
    album_ids = [a for a in album_ids if a]
    if not album_ids:
        return []

    j = _spotify_get_json(ALBUMS_URL, access_token=access_token, params={"ids": ",".join(album_ids)})
    return (j.get("albums") or [])