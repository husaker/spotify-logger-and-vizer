from __future__ import annotations

import base64
import secrets
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests
from requests import Response

from common.retry import with_retry

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


def _retry_after_from_response(r: Response) -> float | None:
    ra = r.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except ValueError:
        return None


def _spotify_post_form_json(
    url: str,
    *,
    headers: dict[str, str],
    data: dict[str, str],
) -> dict[str, Any]:
    """
    Robust POST for Spotify Accounts API:
    - retries on 429 (Retry-After) and 5xx
    - retries on transient network errors
    - fails fast on 4xx except 429
    """

    def do() -> dict[str, Any]:
        r = requests.post(url, headers=headers, data=data, timeout=30)

        if 200 <= r.status_code < 300:
            return r.json() if r.text else {}

        # retryable
        if r.status_code == 429 or 500 <= r.status_code < 600:
            err = RuntimeError(f"Spotify token retryable: {r.status_code} | {r.text}")
            setattr(err, "_response", r)
            raise err

        # non-retryable
        raise RuntimeError(f"Spotify token failed: {r.status_code} | {r.text}")

    def should_retry(e: Exception) -> bool:
        if isinstance(e, requests.RequestException):
            return True
        return str(e).startswith("Spotify token retryable:")

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

    j = _spotify_post_form_json(SPOTIFY_TOKEN_URL, headers=headers, data=data)

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

    j = _spotify_post_form_json(SPOTIFY_TOKEN_URL, headers=headers, data=data)

    return SpotifyTokens(
        access_token=j["access_token"],
        refresh_token=j.get("refresh_token"),  # sometimes absent
        expires_in=int(j["expires_in"]),
    )


def get_spotify_user_id(access_token: str) -> str:
    """
    Retries only on 429/5xx/network. Fails fast on other 4xx (e.g., 403 country unavailable).
    """

    def do() -> str:
        headers = {"Authorization": f"Bearer {access_token}"}
        r = requests.get(SPOTIFY_ME_URL, headers=headers, timeout=30)

        if 200 <= r.status_code < 300:
            j: dict[str, Any] = r.json()
            return str(j["id"])

        if r.status_code == 429 or 500 <= r.status_code < 600:
            err = RuntimeError(f"Spotify /me retryable: {r.status_code} | {r.text}")
            setattr(err, "_response", r)
            raise err

        raise RuntimeError(f"/me failed: {r.status_code} | {r.text}")

    def should_retry(e: Exception) -> bool:
        if isinstance(e, requests.RequestException):
            return True
        return str(e).startswith("Spotify /me retryable:")

    def get_retry_after_seconds(e: Exception) -> float | None:
        r = getattr(e, "_response", None)
        if isinstance(r, Response):
            return _retry_after_from_response(r)
        return None

    return with_retry(
        do,
        should_retry=should_retry,
        get_retry_after_seconds=get_retry_after_seconds,
        attempts=4,
        base_sleep=1.0,
    )


def make_state() -> str:
    return secrets.token_urlsafe(16)