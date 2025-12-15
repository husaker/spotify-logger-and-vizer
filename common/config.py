from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class AppConfig:
    spotify_client_id: str
    spotify_client_secret: str
    spotify_redirect_uri: str

    google_service_account_json: str | None
    google_service_account_file: str | None

    registry_sheet_id: str
    fernet_key: str

    sync_lookback_minutes: int = 120
    dedupe_read_rows: int = 5000
    cache_ttl_days: int = 30
    sync_page_limit: int = 50
    max_pages_per_run: int = 10


def _get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:  # noqa: BLE001
        raise ValueError(f"Env {name} must be integer, got {value!r}") from exc


def load_config() -> AppConfig:
    spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
    spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    spotify_redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
    google_service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    google_service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    registry_sheet_id = os.getenv("REGISTRY_SHEET_ID")
    fernet_key = os.getenv("FERNET_KEY")

    missing = []
    if not spotify_client_id:
        missing.append("SPOTIFY_CLIENT_ID")
    if not spotify_client_secret:
        missing.append("SPOTIFY_CLIENT_SECRET")
    if not spotify_redirect_uri:
        missing.append("SPOTIFY_REDIRECT_URI")
    if not registry_sheet_id:
        missing.append("REGISTRY_SHEET_ID")
    if not fernet_key:
        missing.append("FERNET_KEY")
    if not (google_service_account_json or google_service_account_file):
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE")

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    sync_lookback_minutes = _get_env_int("SYNC_LOOKBACK_MINUTES", 120)
    dedupe_read_rows = _get_env_int("DEDUP_READ_ROWS", 5000)
    cache_ttl_days = _get_env_int("CACHE_TTL_DAYS", 30)
    sync_page_limit = _get_env_int("SYNC_PAGE_LIMIT", 50)
    max_pages_per_run = _get_env_int("MAX_PAGES_PER_RUN", 10)

    return AppConfig(
        spotify_client_id=spotify_client_id,
        spotify_client_secret=spotify_client_secret,
        spotify_redirect_uri=spotify_redirect_uri,
        google_service_account_json=google_service_account_json,
        google_service_account_file=google_service_account_file,
        registry_sheet_id=registry_sheet_id,
        fernet_key=fernet_key,
        sync_lookback_minutes=sync_lookback_minutes,
        dedupe_read_rows=dedupe_read_rows,
        cache_ttl_days=cache_ttl_days,
        sync_page_limit=sync_page_limit,
        max_pages_per_run=max_pages_per_run,
    )
