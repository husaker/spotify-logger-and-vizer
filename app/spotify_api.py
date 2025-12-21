from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import requests

RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"
TRACKS_URL = "https://api.spotify.com/v1/tracks"
ARTISTS_URL = "https://api.spotify.com/v1/artists"
ALBUMS_URL = "https://api.spotify.com/v1/albums"


@dataclass(frozen=True)
class PlayedItem:
    played_at: str          # ISO, ends with Z
    track_id: str
    track_name: str
    artist_name: str
    track_url: str


def get_recently_played_with_access_token(
    access_token: str,
    after_ms: int,
    limit: int = 50,
) -> list[PlayedItem]:
    headers = {"Authorization": f"Bearer {access_token}"}
    params: dict[str, Any] = {"limit": limit}
    if after_ms > 0:
        params["after"] = after_ms

    r = requests.get(RECENTLY_PLAYED_URL, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"recently-played failed: {r.status_code} | {r.text}")

    j: Dict[str, Any] = r.json()
    out: list[PlayedItem] = []

    for it in j.get("items", []) or []:
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
    if not track_ids:
        return []
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"ids": ",".join(track_ids)}
    r = requests.get(TRACKS_URL, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"tracks failed: {r.status_code} | {r.text}")
    return (r.json() or {}).get("tracks", []) or []


def get_artists(access_token: str, artist_ids: list[str]) -> list[dict[str, Any]]:
    """
    Spotify supports up to 50 artist ids per request.
    """
    if not artist_ids:
        return []
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"ids": ",".join(artist_ids)}
    r = requests.get(ARTISTS_URL, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"artists failed: {r.status_code} | {r.text}")
    return (r.json() or {}).get("artists", []) or []


def get_albums(access_token: str, album_ids: list[str]) -> list[dict[str, Any]]:
    """
    Spotify supports up to 20 album ids per request.
    """
    if not album_ids:
        return []
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"ids": ",".join(album_ids)}
    r = requests.get(ALBUMS_URL, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"albums failed: {r.status_code} | {r.text}")
    return (r.json() or {}).get("albums", []) or []