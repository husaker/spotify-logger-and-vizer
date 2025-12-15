from __future__ import annotations

import base64
import dataclasses
from typing import Dict, List

import requests

from common.config import AppConfig


@dataclasses.dataclass
class TokenData:
    access_token: str
    refresh_token: str
    expires_in: int
    spotify_user_id: str


SPOTIFY_AUTH_BASE = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_ME_URL = "https://api.spotify.com/v1/me"


def build_authorize_url(config: AppConfig) -> str:
    scopes: List[str] = [
        "user-read-recently-played",
    ]
    params = {
        "response_type": "code",
        "client_id": config.spotify_client_id,
        "redirect_uri": config.spotify_redirect_uri,
        "scope": " ".join(scopes),
    }
    from urllib.parse import urlencode

    return f"{SPOTIFY_AUTH_BASE}?{urlencode(params)}"


def _basic_auth_header(config: AppConfig) -> str:
    raw = f"{config.spotify_client_id}:{config.spotify_client_secret}".encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def exchange_code_for_tokens(config: AppConfig, code: str) -> TokenData:
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": config.spotify_redirect_uri,
    }
    headers = {
        "Authorization": f"Basic {_basic_auth_header(config)}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    resp = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Spotify token endpoint error {resp.status_code}: {resp.text}"
        )
    payload: Dict[str, object] = resp.json()

    access_token = str(payload.get("access_token"))
    refresh_token = str(payload.get("refresh_token"))
    expires_in = int(payload.get("expires_in", 3600))

    # Fetch user ID
    me_resp = requests.get(
        SPOTIFY_ME_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if me_resp.status_code != 200:
        raise RuntimeError(
            f"Spotify /me endpoint error {me_resp.status_code}: {me_resp.text}"
        )
    me_payload: Dict[str, object] = me_resp.json()
    spotify_user_id = str(me_payload.get("id"))

    return TokenData(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=expires_in,
        spotify_user_id=spotify_user_id,
    )


def refresh_access_token(config: AppConfig, refresh_token: str) -> str:
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    headers = {
        "Authorization": f"Basic {_basic_auth_header(config)}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    resp = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Spotify refresh token error {resp.status_code}: {resp.text}"
        )
    payload: Dict[str, object] = resp.json()
    access_token = str(payload.get("access_token"))
    return access_token
