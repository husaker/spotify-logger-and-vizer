from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import gspread


REGISTRY_TAB = "registry"
REGISTRY_HEADERS = ["user_sheet_id", "enabled", "created_at", "last_seen_at", "last_sync_at", "last_error"]


@dataclass
class RegistryUser:
    user_sheet_id: str
    enabled: bool


def ensure_registry_headers(ws: gspread.Worksheet) -> None:
    values = ws.row_values(1)
    if values != REGISTRY_HEADERS:
        # перезаписываем заголовки (безопасно для пустой или новой таблицы)
        ws.update("A1:F1", [REGISTRY_HEADERS])


def read_registry(ws: gspread.Worksheet) -> list[RegistryUser]:
    # ожидаем заголовок в первой строке
    rows: list[list[Any]] = ws.get_all_values()
    if not rows:
        return []
    header = rows[0]
    if header != REGISTRY_HEADERS:
        # на всякий случай: не падаем, а вернём пусто
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