from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import gspread
from app.gspread_retry import gcall

REGISTRY_TAB = "registry"
REGISTRY_HEADERS = [
    "user_sheet_id",
    "enabled",
    "created_at",
    "last_seen_at",
    "last_sync_at",
    "last_error",
    "spotify_user_id",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RegistryUser:
    user_sheet_id: str
    enabled: bool


def ensure_registry_headers(ws: gspread.Worksheet) -> None:
    values = gcall(lambda: ws.row_values(1))
    if values != REGISTRY_HEADERS:
        raise RuntimeError(
            "Registry header mismatch. Please migrate registry sheet to V2 headers:\n"
            + " | ".join(REGISTRY_HEADERS)
        )


def read_registry(ws: gspread.Worksheet) -> list[RegistryUser]:
    ensure_registry_headers(ws)
    rows: list[list[Any]] = gcall(lambda: ws.get_all_values())
    out: list[RegistryUser] = []
    for r in rows[1:]:
        if not r or len(r) < 2:
            continue
        sheet_id = (r[0] or "").strip()
        enabled_raw = (r[1] or "").strip().lower()
        enabled = enabled_raw in ("true", "1", "yes", "y")
        if sheet_id:
            out.append(RegistryUser(user_sheet_id=sheet_id, enabled=enabled))
    return out


def upsert_registry_user(
    ws: gspread.Worksheet,
    *,
    user_sheet_id: str,
    enabled: bool,
    spotify_user_id: str | None = None,
) -> None:
    ensure_registry_headers(ws)
    all_values = gcall(lambda: ws.get_all_values())

    target_row = None
    for i, r in enumerate(all_values[1:], start=2):
        if len(r) >= 1 and (r[0] or "").strip() == user_sheet_id:
            target_row = i
            break

    now = _now_iso()
    enabled_str = "true" if enabled else "false"

    if target_row is None:
        row = [user_sheet_id, enabled_str, now, now, "", "", spotify_user_id or ""]
        gcall(lambda: ws.append_row(row, value_input_option="RAW"))
        return

    batch: list[dict[str, object]] = [
        {"range": f"B{target_row}", "values": [[enabled_str]]},
        {"range": f"D{target_row}", "values": [[now]]},
    ]
    if spotify_user_id is not None:
        batch.append({"range": f"G{target_row}", "values": [[spotify_user_id]]})

    gcall(lambda: ws.batch_update(batch, value_input_option="RAW"))


def update_registry_status(
    ws: gspread.Worksheet,
    *,
    user_sheet_id: str,
    last_sync_at: str | None,
    last_error: str | None,
) -> None:
    ensure_registry_headers(ws)
    all_values = gcall(lambda: ws.get_all_values())

    target_row = None
    for i, r in enumerate(all_values[1:], start=2):
        if len(r) >= 1 and (r[0] or "").strip() == user_sheet_id:
            target_row = i
            break
    if target_row is None:
        return

    now = _now_iso()
    batch: list[dict[str, object]] = [{"range": f"D{target_row}", "values": [[now]]}]
    if last_sync_at is not None:
        batch.append({"range": f"E{target_row}", "values": [[last_sync_at]]})
    if last_error is not None:
        batch.append({"range": f"F{target_row}", "values": [[last_error]]})

    gcall(lambda: ws.batch_update(batch, value_input_option="RAW"))


def find_sheet_by_spotify_user_id(ws: gspread.Worksheet, spotify_user_id: str) -> str | None:
    ensure_registry_headers(ws)
    rows = gcall(lambda: ws.get_all_values())
    for r in rows[1:]:
        sid = (r[0] or "").strip() if len(r) >= 1 else ""
        suid = (r[6] or "").strip() if len(r) >= 7 else ""  # col G
        if sid and suid == spotify_user_id:
            return sid
    return None