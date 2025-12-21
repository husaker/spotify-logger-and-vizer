from __future__ import annotations

import gspread

from worker.cache_schema import (
    CacheSheets,
    CACHE_TRACKS_TAB,
    CACHE_ARTISTS_TAB,
    CACHE_TRACKS_HEADERS_V1,
    CACHE_ARTISTS_HEADERS_V1,
)


def _range_a1(headers_len: int) -> str:
    end_col = chr(ord("A") + headers_len - 1)
    return f"A1:{end_col}1"


def _headers_match(ws: gspread.Worksheet, headers: list[str]) -> bool:
    existing = ws.row_values(1)
    return existing == headers


def _ensure_ws_headers(ws: gspread.Worksheet, headers: list[str]) -> None:
    if not _headers_match(ws, headers):
        ws.update(_range_a1(len(headers)), [headers])


def _get_or_create_with_schema_versioning(
    ss: gspread.Spreadsheet,
    sheets_client,
    base_title: str,
    headers: list[str],
    rows: int,
    cols: int,
) -> str:
    """
    Returns the worksheet title that should be used for this schema.
    If base_title exists but has different headers, create base_title_v2, base_title_v3, ...
    """
    ws = sheets_client.get_or_create_worksheet(ss, base_title, rows=rows, cols=max(cols, len(headers)))

    existing = ws.row_values(1)
    if existing and existing != headers:
        # Schema конфликт: не трогаем существующий лист, создаем новую версию
        ver = 2
        while True:
            title2 = f"{base_title}_v{ver}"
            ws2 = sheets_client.get_or_create_worksheet(ss, title2, rows=rows, cols=max(cols, len(headers)))
            existing2 = ws2.row_values(1)
            if not existing2 or existing2 == headers:
                _ensure_ws_headers(ws2, headers)
                return title2
            ver += 1

    # либо пустой, либо совпал — приводим к нужным headers
    _ensure_ws_headers(ws, headers)
    return base_title


def ensure_cache_sheets(ss: gspread.Spreadsheet, sheets_client) -> CacheSheets:
    tracks_tab = _get_or_create_with_schema_versioning(
        ss,
        sheets_client,
        base_title=CACHE_TRACKS_TAB,
        headers=CACHE_TRACKS_HEADERS_V1,
        rows=5000,
        cols=20,
    )
    artists_tab = _get_or_create_with_schema_versioning(
        ss,
        sheets_client,
        base_title=CACHE_ARTISTS_TAB,
        headers=CACHE_ARTISTS_HEADERS_V1,
        rows=5000,
        cols=20,
    )
    return CacheSheets(tracks_tab=tracks_tab, artists_tab=artists_tab)
