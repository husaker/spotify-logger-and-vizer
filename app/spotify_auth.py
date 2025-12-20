from __future__ import annotations

import base64
import os
import secrets
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests


SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_ME_URL = "https://api.spotify.com/v1/me"


@dataclass(frozen=True)
class SpotifyTokens:
    access_token: str
    refresh_token: str | None
    expires_in: int


def build_auth_url(client_id: str, redirect_uri: str, scopes: list[str], state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(scopes),
        "state": state,
        "show_dialog": "true",
    }
    return f"{SPOTIFY_AUTH_URL}?{urlencode(params)}"


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    b64 = base64.b64encode(raw).decode("utf-8")
    return f"Basic {b64}"


def exchange_code_for_token(
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    code: str,
) -> SpotifyTokens:
    headers = {
        "Authorization": _basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    r = requests.post(SPOTIFY_TOKEN_URL, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    j: dict[str, Any] = r.json()
    return SpotifyTokens(
        access_token=j["access_token"],
        refresh_token=j.get("refresh_token"),
        expires_in=int(j["expires_in"]),
    )


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> SpotifyTokens:
    headers = {
        "Authorization": _basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    r = requests.post(SPOTIFY_TOKEN_URL, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    j: dict[str, Any] = r.json()
    return SpotifyTokens(
        access_token=j["access_token"],
        refresh_token=j.get("refresh_token"),  # sometimes absent
        expires_in=int(j["expires_in"]),
    )


def get_spotify_user_id(access_token: str) -> str:
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(SPOTIFY_ME_URL, headers=headers, timeout=30)

    if r.status_code != 200:
        # Важно: Spotify обычно возвращает подробности в JSON
        raise RuntimeError(f"/me failed: {r.status_code} | {r.text}")

    j: dict[str, Any] = r.json()
    return str(j["id"])


def make_state() -> str:
    return secrets.token_urlsafe(16)