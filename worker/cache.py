from __future__ import annotations

import datetime as dt
from typing import Dict, Iterable, List, Mapping, MutableMapping, Tuple

import gspread

from app.sheets_client import (
    CACHE_ALBUMS_HEADERS,
    CACHE_ALBUMS_SHEET_TITLE,
    CACHE_ARTISTS_HEADERS,
    CACHE_ARTISTS_SHEET_TITLE,
    CACHE_TRACKS_HEADERS,
    CACHE_TRACKS_SHEET_TITLE,
    SheetsClient,
)


def _parse_iso(s: str) -> dt.datetime | None:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_fresh(fetched_at: str, ttl_days: int) -> bool:
    ts = _parse_iso(fetched_at)
    if not ts:
        return False
    return dt.datetime.now(dt.timezone.utc) - ts < dt.timedelta(days=ttl_days)


def _load_cache_sheet(
    client: SheetsClient,
    spreadsheet_id: str,
    title: str,
    headers: List[str],
) -> Tuple[gspread.Worksheet, Dict[str, int], List[List[str]]]:
    ss = client.open_by_id(spreadsheet_id)
    try:
        ws = ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = client.get_or_create_worksheet(spreadsheet_id, title)
        client.ensure_headers(ws, headers)
    values = ws.get_all_values()
    if not values:
        return ws, {}, []
    header = values[0]
    rows = values[1:]
    try:
        key_idx = header.index(headers[0])
    except ValueError:
        return ws, {}, rows
    index_map: Dict[str, int] = {}
    for i, row in enumerate(rows, start=2):
        if len(row) > key_idx and row[key_idx]:
            index_map[row[key_idx]] = i
    return ws, index_map, rows


def get_tracks_cache(
    client: SheetsClient,
    spreadsheet_id: str,
    ttl_days: int,
) -> Tuple[Dict[str, Mapping[str, str]], List[str]]:
    ws, index_map, rows = _load_cache_sheet(
        client,
        spreadsheet_id,
        CACHE_TRACKS_SHEET_TITLE,
        CACHE_TRACKS_HEADERS,
    )
    header = CACHE_TRACKS_HEADERS
    cached: Dict[str, Mapping[str, str]] = {}
    missing_or_expired: List[str] = []

    for row in rows:
        row_dict = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        key = row_dict["track_id"]
        if not key:
            continue
        if _is_fresh(row_dict.get("fetched_at", ""), ttl_days):
            cached[key] = row_dict
        else:
            missing_or_expired.append(key)

    return cached, missing_or_expired


def upsert_tracks_cache(
    client: SheetsClient,
    spreadsheet_id: str,
    records: Mapping[str, Mapping[str, str]],
) -> None:
    if not records:
        return
    ws, index_map, _rows = _load_cache_sheet(
        client,
        spreadsheet_id,
        CACHE_TRACKS_SHEET_TITLE,
        CACHE_TRACKS_HEADERS,
    )
    header = CACHE_TRACKS_HEADERS
    updates: List[Tuple[int, List[str]]] = []
    appends: List[List[str]] = []

    for key, data in records.items():
        row = [str(data.get(col, "")) for col in header]
        if key in index_map:
            row_number = index_map[key]
            updates.append((row_number, row))
        else:
            appends.append(row)

    for row_number, row in updates:
        ws.update(f"A{row_number}:I{row_number}", [row])
    if appends:
        client.append_rows(ws, appends)


def get_artists_cache(
    client: SheetsClient,
    spreadsheet_id: str,
    ttl_days: int,
) -> Tuple[Dict[str, Mapping[str, str]], List[str]]:
    ws, index_map, rows = _load_cache_sheet(
        client,
        spreadsheet_id,
        CACHE_ARTISTS_SHEET_TITLE,
        CACHE_ARTISTS_HEADERS,
    )
    header = CACHE_ARTISTS_HEADERS
    cached: Dict[str, Mapping[str, str]] = {}
    missing_or_expired: List[str] = []

    for row in rows:
        row_dict = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        key = row_dict["artist_id"]
        if not key:
            continue
        if _is_fresh(row_dict.get("fetched_at", ""), ttl_days):
            cached[key] = row_dict
        else:
            missing_or_expired.append(key)

    return cached, missing_or_expired


def upsert_artists_cache(
    client: SheetsClient,
    spreadsheet_id: str,
    records: Mapping[str, Mapping[str, str]],
) -> None:
    if not records:
        return
    ws, index_map, _rows = _load_cache_sheet(
        client,
        spreadsheet_id,
        CACHE_ARTISTS_SHEET_TITLE,
        CACHE_ARTISTS_HEADERS,
    )
    header = CACHE_ARTISTS_HEADERS
    updates: List[Tuple[int, List[str]]] = []
    appends: List[List[str]] = []

    for key, data in records.items():
        row = [str(data.get(col, "")) for col in header]
        if key in index_map:
            row_number = index_map[key]
            updates.append((row_number, row))
        else:
            appends.append(row)

    for row_number, row in updates:
        ws.update(f"A{row_number}:F{row_number}", [row])
    if appends:
        client.append_rows(ws, appends)


def get_albums_cache(
    client: SheetsClient,
    spreadsheet_id: str,
    ttl_days: int,
) -> Tuple[Dict[str, Mapping[str, str]], List[str]]:
    ws, index_map, rows = _load_cache_sheet(
        client,
        spreadsheet_id,
        CACHE_ALBUMS_SHEET_TITLE,
        CACHE_ALBUMS_HEADERS,
    )
    header = CACHE_ALBUMS_HEADERS
    cached: Dict[str, Mapping[str, str]] = {}
    missing_or_expired: List[str] = []

    for row in rows:
        row_dict = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        key = row_dict["album_id"]
        if not key:
            continue
        if _is_fresh(row_dict.get("fetched_at", ""), ttl_days):
            cached[key] = row_dict
        else:
            missing_or_expired.append(key)

    return cached, missing_or_expired


def upsert_albums_cache(
    client: SheetsClient,
    spreadsheet_id: str,
    records: Mapping[str, Mapping[str, str]],
) -> None:
    if not records:
        return
    ws, index_map, _rows = _load_cache_sheet(
        client,
        spreadsheet_id,
        CACHE_ALBUMS_SHEET_TITLE,
        CACHE_ALBUMS_HEADERS,
    )
    header = CACHE_ALBUMS_HEADERS
    updates: List[Tuple[int, List[str]]] = []
    appends: List[List[str]] = []

    for key, data in records.items():
        row = [str(data.get(col, "")) for col in header]
        if key in index_map:
            row_number = index_map[key]
            updates.append((row_number, row))
        else:
            appends.append(row)

    for row_number, row in updates:
        ws.update(f"A{row_number}:E{row_number}", [row])
    if appends:
        client.append_rows(ws, appends)
