from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import gspread
from app.gspread_retry import gcall

REGISTRY_TAB = "registry"
REGISTRY_HEADERS = ["user_sheet_id", "enabled", "created_at", "last_seen_at", "last_sync_at", "last_error"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RegistryUser:
    user_sheet_id: str
    enabled: bool


def ensure_registry_headers(ws: gspread.Worksheet) -> None:
    values = ws.row_values(1)
    if values != REGISTRY_HEADERS:
        ws.update("A1:F1", [REGISTRY_HEADERS])


def read_registry(ws: gspread.Worksheet) -> list[RegistryUser]:
    rows: list[list[Any]] = ws.get_all_values()
    if not rows:
        return []
    header = rows[0]
    if header != REGISTRY_HEADERS:
        return []
    out: list[RegistryUser] = []
    for r in rows[1:]:
        if not r or len(r) < 2:
            continue
        sheet_id = r[0].strip()
        enabled_raw = (r[1] or "").strip().lower()
        enabled = enabled_raw in ("true", "1", "yes", "y")
        if sheet_id:
            out.append(RegistryUser(user_sheet_id=sheet_id, enabled=enabled))
    return out


def upsert_registry_user(ws: gspread.Worksheet, user_sheet_id: str, enabled: bool) -> None:
    """
    If user_sheet_id exists → update enabled + last_seen_at.
    Else → append a new row.
    """
    ensure_registry_headers(ws)

    all_values = gcall(lambda: ws.get_all_values())

    target_row = None
    for i, r in enumerate(all_values[1:], start=2):
        if len(r) >= 1 and r[0].strip() == user_sheet_id:
            target_row = i
            break

    now = _now_iso()
    enabled_str = "true" if enabled else "false"

    if target_row is None:
        row = [user_sheet_id, enabled_str, now, now, "", ""]
        gcall(lambda: ws.append_row(row, value_input_option="RAW"))
        return

    # one request instead of two
    data = [
        {"range": f"B{target_row}", "values": [[enabled_str]]},
        {"range": f"D{target_row}", "values": [[now]]},
    ]
    gcall(lambda: ws.batch_update(data, value_input_option="RAW"))

def update_registry_status(
    ws: gspread.Worksheet,
    user_sheet_id: str,
    last_sync_at: str | None,
    last_error: str | None,
) -> None:
    """
    Updates:
      D (last_seen_at) always
      E (last_sync_at) if provided
      F (last_error) if provided
    """
    all_values = ws.get_all_values()
    target_row = None
    for i, r in enumerate(all_values[1:], start=2):
        if len(r) >= 1 and r[0].strip() == user_sheet_id:
            target_row = i
            break
    if target_row is None:
        return

    now = _now_iso()
    gcall(lambda: ws.update(f"D{target_row}", [[now]]))

    if last_sync_at is not None:
        gcall(lambda: ws.update(f"E{target_row}", [[last_sync_at]]))
    if last_error is not None:
        gcall(lambda: ws.update(f"F{target_row}", [[last_error]]))