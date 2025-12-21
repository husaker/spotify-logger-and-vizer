from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import gspread

from app.gspread_retry import gcall

APP_STATE_TAB = "__app_state"
APP_STATE_HEADERS = ["key", "value"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_app_state_ws(ss: gspread.Spreadsheet) -> gspread.Worksheet:
    """
    Get __app_state worksheet. If missing, create it and set headers.
    Wrapped into gcall to survive transient Google API failures.
    """
    try:
        return gcall(lambda: ss.worksheet(APP_STATE_TAB))
    except gspread.WorksheetNotFound:
        ws = gcall(lambda: ss.add_worksheet(title=APP_STATE_TAB, rows=200, cols=2))
        gcall(lambda: ws.update("A1:B1", [APP_STATE_HEADERS]))
        return ws


def read_app_state(ss: gspread.Spreadsheet) -> dict[str, str]:
    ws = _ensure_app_state_ws(ss)
    values = gcall(lambda: ws.get_all_values())

    state: dict[str, str] = {}
    for r in values[1:]:
        if len(r) >= 2 and (r[0] or "").strip():
            state[(r[0] or "").strip()] = (r[1] or "").strip()
    return state


def write_app_state_kv(ss: gspread.Spreadsheet, kv: dict[str, str]) -> None:
    """
    Upserts key/value pairs into __app_state without clearing the sheet.
    Also updates updated_at automatically.
    All gspread calls are wrapped with gcall().
    """
    ws = _ensure_app_state_ws(ss)

    # Ensure headers are correct
    row1 = gcall(lambda: ws.row_values(1))
    if row1 != APP_STATE_HEADERS:
        gcall(lambda: ws.update("A1:B1", [APP_STATE_HEADERS]))

    values = gcall(lambda: ws.get_all_values())

    key_to_row: dict[str, int] = {}
    for i, r in enumerate(values[1:], start=2):
        if len(r) >= 1 and (r[0] or "").strip():
            key_to_row[(r[0] or "").strip()] = i

    payload = dict(kv)
    payload["updated_at"] = _now_iso()

    batch: list[dict[str, Any]] = []
    to_append: list[list[str]] = []

    for k, v in payload.items():
        if k in key_to_row:
            row = key_to_row[k]
            batch.append({"range": f"A{row}:B{row}", "values": [[k, v]]})
        else:
            to_append.append([k, v])

    if batch:
        gcall(lambda: ws.batch_update(batch, value_input_option="RAW"))
    if to_append:
        gcall(lambda: ws.append_rows(to_append, value_input_option="RAW"))