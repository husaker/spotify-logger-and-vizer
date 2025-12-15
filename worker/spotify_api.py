from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import requests

from common.config import AppConfig


@dataclass
class PlayedItem:
    played_at: str  # ISO8601 UTC
    track_id: str
    track_name: str
    artist_names: List[str]
    spotify_user_id: str


SPOTIFY_RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"
SPOTIFY_TRACKS_URL = "https://api.spotify.com/v1/tracks"
SPOTIFY_ARTISTS_URL = "https://api.spotify.com/v1/artists"
SPOTIFY_ALBUMS_URL = "https://api.spotify.com/v1/albums"


class SpotifyRateLimitError(Exception):
    def __init__(self, retry_after: int, message: str) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _request_with_retries(
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, object] | None = None,
    data: Dict[str, object] | None = None,
    max_retries: int = 5,
) -> requests.Response:
    backoff = 1.0
    for attempt in range(max_retries):
        resp = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=data,
            timeout=30,
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
            continue
        if 500 <= resp.status_code < 600:
            time.sleep(backoff)
            backoff *= 2
            continue
        return resp
    return resp


def fetch_recently_played(
    access_token: str,
    after_ms: int | None,
    page_limit: int,
    max_pages: int,
) -> List[PlayedItem]:
    headers = {"Authorization": f"Bearer {access_token}"}
    items: List[PlayedItem] = []
    params: Dict[str, object] = {"limit": page_limit}
    if after_ms is not None:
        params["after"] = after_ms

    url = SPOTIFY_RECENTLY_PLAYED_URL
    for _ in range(max_pages):
        resp = _request_with_retries("GET", url, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Spotify recently-played error {resp.status_code}: {resp.text}"
            )
        payload = resp.json()
        batch = payload.get("items", [])
        if not batch:
            break

        for raw in batch:
            played_at = str(raw["played_at"])
            track = raw.get("track") or {}
            track_id = str(track.get("id"))
            track_name = str(track.get("name"))
            artists_raw = track.get("artists") or []
            artist_names = [str(a.get("name", "")) for a in artists_raw]
            items.append(
                PlayedItem(
                    played_at=played_at,
                    track_id=track_id,
                    track_name=track_name,
                    artist_names=artist_names,
                    spotify_user_id="",  # Filled by caller
                )
            )

        if "next" not in payload or not payload["next"]:
            break
        url = payload["next"]
        params = {}

    return items


def batch_fetch_tracks(access_token: str, track_ids: List[str]) -> Dict[str, Dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    result: Dict[str, Dict] = {}
    for i in range(0, len(track_ids), 50):
        chunk = [tid for tid in track_ids[i : i + 50] if tid]
        if not chunk:
            continue
        params = {"ids": ",".join(chunk)}
        resp = _request_with_retries("GET", SPOTIFY_TRACKS_URL, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Spotify tracks batch error {resp.status_code}: {resp.text}"
            )
        payload = resp.json()
        for track in payload.get("tracks", []):
            if track and track.get("id"):
                result[str(track["id"])] = track
    return result


def batch_fetch_artists(access_token: str, artist_ids: List[str]) -> Dict[str, Dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    result: Dict[str, Dict] = {}
    for i in range(0, len(artist_ids), 50):
        chunk = [aid for aid in artist_ids[i : i + 50] if aid]
        if not chunk:
            continue
        params = {"ids": ",".join(chunk)}
        resp = _request_with_retries("GET", SPOTIFY_ARTISTS_URL, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Spotify artists batch error {resp.status_code}: {resp.text}"
            )
        payload = resp.json()
        for artist in payload.get("artists", []):
            if artist and artist.get("id"):
                result[str(artist["id"])] = artist
    return result


def batch_fetch_albums(access_token: str, album_ids: List[str]) -> Dict[str, Dict]:
    headers = {"Authorization": f"Bearer {access_token}"}
    result: Dict[str, Dict] = {}
    for i in range(0, len(album_ids), 20):
        chunk = [aid for aid in album_ids[i : i + 20] if aid]
        if not chunk:
            continue
        params = {"ids": ",".join(chunk)}
        resp = _request_with_retries("GET", SPOTIFY_ALBUMS_URL, headers=headers, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Spotify albums batch error {resp.status_code}: {resp.text}"
            )
        payload = resp.json()
        for album in payload.get("albums", []):
            if album and album.get("id"):
                result[str(album["id"])] = album
    return result
