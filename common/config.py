from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Подхватываем переменные из .env (локально)
load_dotenv()


def _get_env_optional(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = v.strip()
    return v if v != "" else None


def _get_env_required(name: str) -> str:
    v = _get_env_optional(name)
    if v is None:
        raise ValueError(f"Missing required env var: {name}")
    return v


def _get_int(name: str, default: int) -> int:
    v = _get_env_optional(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError as e:
        raise ValueError(f"Env var {name} must be int, got: {v}") from e


def _load_service_account_json() -> str:
    """
    Prefer GOOGLE_SERVICE_ACCOUNT_FILE (local/dev), fallback to GOOGLE_SERVICE_ACCOUNT_JSON (CI/Streamlit secrets).
    Always returns a JSON string.
    """
    file_path = _get_env_optional("GOOGLE_SERVICE_ACCOUNT_FILE")
    if file_path:
        p = Path(file_path).expanduser()
        if not p.exists():
            raise ValueError(f"GOOGLE_SERVICE_ACCOUNT_FILE points to missing file: {p}")
        return p.read_text(encoding="utf-8")

    # Fallback: JSON string (can be one-line in .env, or multi-line via GitHub/Streamlit secrets)
    return _get_env_required("GOOGLE_SERVICE_ACCOUNT_JSON")


@dataclass(frozen=True)
class Settings:
    registry_sheet_id: str
    google_service_account_json: str
    fernet_key: str

    # Spotify
    spotify_client_id: str
    spotify_client_secret: str
    public_app_url: str

    sync_lookback_minutes: int = 120
    dedup_read_rows: int = 5000
    cache_ttl_days: int = 30


def load_settings() -> Settings:
    registry_sheet_id = _get_env_required("REGISTRY_SHEET_ID")
    google_service_account_json = _load_service_account_json()
    fernet_key = _get_env_required("FERNET_KEY")

    spotify_client_id = _get_env_required("SPOTIFY_CLIENT_ID")
    spotify_client_secret = _get_env_required("SPOTIFY_CLIENT_SECRET")
    public_app_url = _get_env_required("PUBLIC_APP_URL").rstrip("/")

    return Settings(
        registry_sheet_id=registry_sheet_id,
        google_service_account_json=google_service_account_json,
        fernet_key=fernet_key,
        spotify_client_id=spotify_client_id,
        spotify_client_secret=spotify_client_secret,
        public_app_url=public_app_url,
        sync_lookback_minutes=_get_int("SYNC_LOOKBACK_MINUTES", 120),
        dedup_read_rows=_get_int("DEDUP_READ_ROWS", 5000),
        cache_ttl_days=_get_int("CACHE_TTL_DAYS", 30),
    )