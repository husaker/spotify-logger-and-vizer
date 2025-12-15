from __future__ import annotations

from typing import Dict, List, Set

import gspread

from app.sheets_client import DEDUPE_HEADERS, DEDUPE_SHEET_TITLE, SheetsClient


def make_dedupe_key(spotify_user_id: str, played_at_iso: str, track_id: str) -> str:
    return f"{spotify_user_id}|{played_at_iso}|{track_id}"


def load_recent_dedupe_keys(
    client: SheetsClient,
    spreadsheet_id: str,
    read_rows: int,
) -> Set[str]:
    ss = client.open_by_id(spreadsheet_id)
    try:
        ws = ss.worksheet(DEDUPE_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = client.get_or_create_worksheet(spreadsheet_id, DEDUPE_SHEET_TITLE)
        client.ensure_headers(ws, DEDUPE_HEADERS)
        return set()

    values = ws.get_all_values()
    if not values:
        return set()
    header = values[0]
    rows = values[1:]
    if not rows:
        return set()
    try:
        idx = header.index("dedupe_key")
    except ValueError:
        return set()

    sliced = rows[-read_rows:]
    keys: Set[str] = set()
    for row in sliced:
        if len(row) > idx and row[idx]:
            keys.add(row[idx])
    return keys


def append_dedupe_keys(
    client: SheetsClient,
    spreadsheet_id: str,
    keys: List[str],
) -> None:
    if not keys:
        return
    ss = client.open_by_id(spreadsheet_id)
    try:
        ws = ss.worksheet(DEDUPE_SHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = client.get_or_create_worksheet(spreadsheet_id, DEDUPE_SHEET_TITLE)
        client.ensure_headers(ws, DEDUPE_HEADERS)

    rows = [[k] for k in keys]
    client.append_rows(ws, rows)
