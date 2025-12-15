from __future__ import annotations

import dataclasses
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from common.config import AppConfig
from app.date_format import now_iso_utc


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


LOG_SHEET_TITLE = "log"
APP_STATE_SHEET_TITLE = "__app_state"
DEDUPE_SHEET_TITLE = "__dedupe"
CACHE_TRACKS_SHEET_TITLE = "__cache_tracks"
CACHE_ARTISTS_SHEET_TITLE = "__cache_artists"
CACHE_ALBUMS_SHEET_TITLE = "__cache_albums"
REGISTRY_SHEET_TITLE = "registry"


LOG_HEADERS = ["Date", "Track", "Artist", "Spotify ID", "URL"]
APP_STATE_HEADERS = ["key", "value"]
DEDUPE_HEADERS = ["dedupe_key"]
CACHE_TRACKS_HEADERS = [
    "track_id",
    "track_name",
    "duration_ms",
    "album_id",
    "album_cover_url",
    "primary_artist_id",
    "artist_ids",
    "track_url",
    "fetched_at",
]
CACHE_ARTISTS_HEADERS = [
    "artist_id",
    "artist_name",
    "artist_cover_url",
    "genres",
    "primary_genre",
    "fetched_at",
]
CACHE_ALBUMS_HEADERS = [
    "album_id",
    "album_name",
    "album_cover_url",
    "release_date",
    "fetched_at",
]
REGISTRY_HEADERS = [
    "user_sheet_id",
    "enabled",
    "created_at",
    "last_seen_at",
    "last_sync_at",
    "last_error",
]


@dataclasses.dataclass
class SheetsClient:
    gc: gspread.Client
    service_account_email: str

    @classmethod
    def from_config(cls, config: AppConfig) -> "SheetsClient":
        if config.google_service_account_json:
            import json

            info = json.loads(config.google_service_account_json)
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        elif config.google_service_account_file:
            creds = Credentials.from_service_account_file(
                config.google_service_account_file,
                scopes=SCOPES,
            )
        else:
            raise ValueError(
                "Не задан GOOGLE_SERVICE_ACCOUNT_JSON или GOOGLE_SERVICE_ACCOUNT_FILE"
            )

        gc = gspread.Client(auth=creds)
        gc.session = gspread.authorize(creds).session

        service_account_email = creds.service_account_email
        return cls(gc=gc, service_account_email=service_account_email)

    # --- Generic helpers ---

    def open_by_id(self, spreadsheet_id: str) -> gspread.Spreadsheet:
        return self.gc.open_by_key(spreadsheet_id)

    def get_or_create_worksheet(
        self,
        spreadsheet_id: str,
        title: str,
        rows: int = 1000,
        cols: int = 20,
    ) -> gspread.Worksheet:
        ss = self.open_by_id(spreadsheet_id)
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            return ss.add_worksheet(title=title, rows=str(rows), cols=str(cols))

    def ensure_headers(self, ws: gspread.Worksheet, headers: List[str]) -> None:
        existing = ws.row_values(1)
        if existing == headers:
            return
        # If headers differ, overwrite and warn via caller.
        ws.resize(rows=max(ws.row_count, 1))
        ws.update("1:1", [headers])

    def read_key_value_sheet(self, ws: gspread.Worksheet) -> Dict[str, str]:
        values = ws.get_all_values()
        data: Dict[str, str] = {}
        for row in values[1:]:
            if len(row) < 2:
                continue
            key, value = row[0], row[1]
            if key:
                data[key] = value
        return data

    def write_key_value_sheet(self, ws: gspread.Worksheet, data: Mapping[str, str]) -> None:
        rows = [["key", "value"]]
        for k, v in data.items():
            rows.append([str(k), str(v)])
        ws.clear()
        ws.update("A1", rows)

    def append_rows(self, ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
        if not rows:
            return
        ws.append_rows(rows, value_input_option="RAW")


# --- App state helpers ---


def ensure_user_sheet_initialized(client: SheetsClient, spreadsheet_id: str) -> None:
    """Ensure all required worksheets and headers exist for a user sheet.

    This is called both from UI "Check access" and by the worker defensively.
    """

    # log
    log_ws = client.get_or_create_worksheet(spreadsheet_id, LOG_SHEET_TITLE)
    client.ensure_headers(log_ws, LOG_HEADERS)

    # __app_state
    app_state_ws = client.get_or_create_worksheet(
        spreadsheet_id,
        APP_STATE_SHEET_TITLE,
    )
    client.ensure_headers(app_state_ws, APP_STATE_HEADERS)

    app_state = client.read_key_value_sheet(app_state_ws)
    now = now_iso_utc()
    changed = False

    defaults = {
        "enabled": "false",
        "timezone": "UTC",
        "last_synced_after_ts": "0",
        "spotify_user_id": "",
        "refresh_token_enc": "",
        "created_at": now,
        "updated_at": now,
        "last_error": "",
    }

    for k, v in defaults.items():
        if k not in app_state or app_state[k] == "":
            app_state[k] = v
            changed = True

    if changed:
        client.write_key_value_sheet(app_state_ws, app_state)

    # __dedupe
    dedupe_ws = client.get_or_create_worksheet(spreadsheet_id, DEDUPE_SHEET_TITLE)
    client.ensure_headers(dedupe_ws, DEDUPE_HEADERS)

    # caches
    cache_tracks_ws = client.get_or_create_worksheet(
        spreadsheet_id,
        CACHE_TRACKS_SHEET_TITLE,
    )
    client.ensure_headers(cache_tracks_ws, CACHE_TRACKS_HEADERS)

    cache_artists_ws = client.get_or_create_worksheet(
        spreadsheet_id,
        CACHE_ARTISTS_SHEET_TITLE,
    )
    client.ensure_headers(cache_artists_ws, CACHE_ARTISTS_HEADERS)

    cache_albums_ws = client.get_or_create_worksheet(
        spreadsheet_id,
        CACHE_ALBUMS_SHEET_TITLE,
    )
    client.ensure_headers(cache_albums_ws, CACHE_ALBUMS_HEADERS)


def get_app_state(client: SheetsClient, spreadsheet_id: str) -> Dict[str, str]:
    ss = client.open_by_id(spreadsheet_id)
    try:
        ws = ss.worksheet(APP_STATE_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ensure_user_sheet_initialized(client, spreadsheet_id)
        ws = ss.worksheet(APP_STATE_SHEET_TITLE)
    return client.read_key_value_sheet(ws)


def update_app_state(
    client: SheetsClient,
    spreadsheet_id: str,
    app_state: Mapping[str, str],
) -> None:
    ss = client.open_by_id(spreadsheet_id)
    try:
        ws = ss.worksheet(APP_STATE_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = client.get_or_create_worksheet(spreadsheet_id, APP_STATE_SHEET_TITLE)
        client.ensure_headers(ws, APP_STATE_HEADERS)
    client.write_key_value_sheet(ws, app_state)


# --- Registry helpers ---


@dataclasses.dataclass
class RegistryClient:
    client: SheetsClient
    registry_sheet_id: str

    def _get_ws(self) -> gspread.Worksheet:
        ws = self.client.get_or_create_worksheet(
            self.registry_sheet_id,
            REGISTRY_SHEET_TITLE,
        )
        self.client.ensure_headers(ws, REGISTRY_HEADERS)
        return ws

    def _load_rows(self) -> Tuple[List[List[str]], Dict[str, int]]:
        ws = self._get_ws()
        values = ws.get_all_values()
        if not values:
            return [], {}
        header = values[0]
        rows = values[1:]
        id_index = header.index("user_sheet_id")
        index_map: Dict[str, int] = {}
        for i, row in enumerate(rows, start=2):  # 1-based rows, skipping header
            if len(row) > id_index:
                sid = row[id_index]
                if sid:
                    index_map[sid] = i
        return rows, index_map

    def register_or_update(
        self,
        user_sheet_id: str,
        enabled: bool,
        last_error: str = "",
    ) -> None:
        ws = self._get_ws()
        rows, index_map = self._load_rows()
        now = now_iso_utc()

        if user_sheet_id in index_map:
            row_number = index_map[user_sheet_id]
            row_values = ws.row_values(row_number)
            # Ensure row has correct length
            while len(row_values) < len(REGISTRY_HEADERS):
                row_values.append("")
            row_values[1] = "true" if enabled else "false"
            if not row_values[2]:
                row_values[2] = now  # created_at
            row_values[3] = now  # last_seen_at
            # last_sync_at kept as is here
            row_values[5] = last_error
            ws.update(f"A{row_number}:F{row_number}", [row_values[:6]])
        else:
            row = [
                user_sheet_id,
                "true" if enabled else "false",
                now,
                now,
                "",
                last_error,
            ]
            self.client.append_rows(ws, [row])

    def set_enabled(self, user_sheet_id: str, enabled: bool, last_error: str = "") -> None:
        ws = self._get_ws()
        rows, index_map = self._load_rows()
        now = now_iso_utc()

        if user_sheet_id not in index_map:
            # create new row
            row = [
                user_sheet_id,
                "true" if enabled else "false",
                now,
                now,
                "",
                last_error,
            ]
            self.client.append_rows(ws, [row])
            return

        row_number = index_map[user_sheet_id]
        row_values = ws.row_values(row_number)
        while len(row_values) < len(REGISTRY_HEADERS):
            row_values.append("")
        row_values[1] = "true" if enabled else "false"
        if not row_values[2]:
            row_values[2] = now
        row_values[3] = now
        row_values[5] = last_error
        ws.update(f"A{row_number}:F{row_number}", [row_values[:6]])

    def list_enabled_users(self) -> List[str]:
        ws = self._get_ws()
        values = ws.get_all_values()
        if not values:
            return []
        header = values[0]
        rows = values[1:]
        try:
            idx_id = header.index("user_sheet_id")
            idx_enabled = header.index("enabled")
        except ValueError:
            return []

        result: List[str] = []
        for row in rows:
            if len(row) <= max(idx_id, idx_enabled):
                continue
            if row[idx_enabled].lower() == "true" and row[idx_id]:
                result.append(row[idx_id])
        return result

    def update_sync_result(
        self,
        user_sheet_id: str,
        last_sync_at: Optional[str],
        last_error: str,
    ) -> None:
        ws = self._get_ws()
        rows, index_map = self._load_rows()
        now = now_iso_utc()

        if user_sheet_id not in index_map:
            row = [
                user_sheet_id,
                "true",
                now,
                now,
                last_sync_at or now,
                last_error,
            ]
            self.client.append_rows(ws, [row])
            return

        row_number = index_map[user_sheet_id]
        row_values = ws.row_values(row_number)
        while len(row_values) < len(REGISTRY_HEADERS):
            row_values.append("")
        row_values[3] = now  # last_seen_at
        if last_sync_at:
            row_values[4] = last_sync_at
        row_values[5] = last_error
        ws.update(f"A{row_number}:F{row_number}", [row_values[:6]])


def get_registry_client(client: SheetsClient, registry_sheet_id: str) -> RegistryClient:
    return RegistryClient(client=client, registry_sheet_id=registry_sheet_id)
