from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from app.spotify_auth import refresh_access_token


RECENTLY_PLAYED_URL = "https://api.spotify.com/v1/me/player/recently-played"


@dataclass(frozen=True)
class PlayedItem:
    played_at: str          # ISO timestamp
    track_id: str
    track_name: str
    artist_name: str
    track_url: str


def get_recently_played(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    after_ms: int,
    limit: int = 50,
) -> list[PlayedItem]:
    """
    after_ms: unix epoch milliseconds (Spotify 'after' parameter).
    """
    tokens = refresh_access_token(client_id, client_secret, refresh_token)
    access_token = tokens.access_token

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"limit": limit}
    if after_ms > 0:
        params["after"] = after_ms

    r = requests.get(RECENTLY_PLAYED_URL, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"recently-played failed: {r.status_code} | {r.text}")

    j: Dict[str, Any] = r.json()
    items = []
    for it in j.get("items", []):
        track = it.get("track") or {}
        artists = track.get("artists") or []
        artist_name = artists[0].get("name") if artists else ""

        track_id = track.get("id") or ""
        track_name = track.get("name") or ""
        external_urls = track.get("external_urls") or {}
        track_url = external_urls.get("spotify") or (f"https://open.spotify.com/track/{track_id}" if track_id else "")

        played_at = it.get("played_at") or ""

        if track_id and played_at:
            items.append(
                PlayedItem(
                    played_at=played_at,
                    track_id=track_id,
                    track_name=track_name,
                    artist_name=artist_name,
                    track_url=track_url,
                )
            )

    return items