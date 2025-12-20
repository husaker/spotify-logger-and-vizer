from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict

import gspread

APP_STATE_TAB = "__app_state"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_app_state(ss: gspread.Spreadsheet) -> Dict[str, str]:
    ws = ss.worksheet(APP_STATE_TAB)
    rows = ws.get_all_values()
    out: Dict[str, str] = {}
    for r in rows[1:]:
        if len(r) >= 2 and r[0].strip():
            out[r[0].strip()] = r[1].strip()
    return out


def write_app_state_kv(ss: gspread.Spreadsheet, updates: Dict[str, str]) -> None:
    ws = ss.worksheet(APP_STATE_TAB)
    rows = ws.get_all_values()

    # map key -> row index (1-based). row 1 is header.
    key_to_row = {}
    for i, r in enumerate(rows[1:], start=2):
        if len(r) >= 1 and r[0].strip():
            key_to_row[r[0].strip()] = i

    for k, v in updates.items():
        if k in key_to_row:
            row = key_to_row[k]
            ws.update(f"B{row}", [[v]])
        else:
            ws.append_row([k, v], value_input_option="RAW")

    # always bump updated_at
    if "updated_at" not in updates:
        if "updated_at" in key_to_row:
            ws.update(f"B{key_to_row['updated_at']}", [[_now_iso()]])
        else:
            ws.append_row(["updated_at", _now_iso()], value_input_option="RAW")