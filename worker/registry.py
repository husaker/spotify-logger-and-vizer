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


@dataclass
class RegistryEntry:
    row_idx: int
    user_sheet_id: str
    enabled: bool
    spotify_user_id: str


@dataclass
class RegistrySnapshot:
    rows: list[list[Any]]
    users: list[RegistryUser]
    by_sheet_id: dict[str, RegistryEntry]
    by_spotify_user_id: dict[str, str]


def ensure_registry_headers(ws: gspread.Worksheet) -> None:
    values = gcall(lambda: ws.row_values(1))
    if values != REGISTRY_HEADERS:
        raise RuntimeError(
            "Registry header mismatch. Please migrate registry sheet to V2 headers:\n"
            + " | ".join(REGISTRY_HEADERS)
        )


def load_registry_snapshot(ws: gspread.Worksheet) -> RegistrySnapshot:
    ensure_registry_headers(ws)
    rows: list[list[Any]] = gcall(lambda: ws.get_all_values())
    users: list[RegistryUser] = []
    by_sheet_id: dict[str, RegistryEntry] = {}
    by_spotify_user_id: dict[str, str] = {}

    for row_idx, r in enumerate(rows[1:], start=2):
        if not r or len(r) < 2:
            continue
        sheet_id = (r[0] or "").strip()
        enabled_raw = (r[1] or "").strip().lower()
        enabled = enabled_raw in ("true", "1", "yes", "y")
        if sheet_id:
            users.append(RegistryUser(user_sheet_id=sheet_id, enabled=enabled))
            spotify_user_id = (r[6] or "").strip() if len(r) >= 7 else ""
            by_sheet_id[sheet_id] = RegistryEntry(
                row_idx=row_idx,
                user_sheet_id=sheet_id,
                enabled=enabled,
                spotify_user_id=spotify_user_id,
            )
            if spotify_user_id:
                by_spotify_user_id[spotify_user_id] = sheet_id

    return RegistrySnapshot(
        rows=rows,
        users=users,
        by_sheet_id=by_sheet_id,
        by_spotify_user_id=by_spotify_user_id,
    )


def read_registry(ws: gspread.Worksheet, *, snapshot: RegistrySnapshot | None = None) -> list[RegistryUser]:
    snap = snapshot or load_registry_snapshot(ws)
    return list(snap.users)


def registry_status_from_snapshot(snapshot: RegistrySnapshot, user_sheet_id: str) -> tuple[bool, bool]:
    entry = snapshot.by_sheet_id.get(user_sheet_id)
    if entry is None:
        return False, False
    return True, entry.enabled


def upsert_registry_user(
    ws: gspread.Worksheet,
    *,
    user_sheet_id: str,
    enabled: bool,
    spotify_user_id: str | None = None,
    snapshot: RegistrySnapshot | None = None,
) -> None:
    target_row = snapshot.by_sheet_id.get(user_sheet_id).row_idx if snapshot and user_sheet_id in snapshot.by_sheet_id else None
    if target_row is None:
        ensure_registry_headers(ws)
        all_values = gcall(lambda: ws.get_all_values())
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
    snapshot: RegistrySnapshot | None = None,
) -> None:
    target_row = snapshot.by_sheet_id.get(user_sheet_id).row_idx if snapshot and user_sheet_id in snapshot.by_sheet_id else None
    if target_row is None:
        ensure_registry_headers(ws)
        all_values = gcall(lambda: ws.get_all_values())
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


def find_sheet_by_spotify_user_id(
    ws: gspread.Worksheet,
    spotify_user_id: str,
    *,
    snapshot: RegistrySnapshot | None = None,
) -> str | None:
    snap = snapshot or load_registry_snapshot(ws)
    return snap.by_spotify_user_id.get(spotify_user_id)
