from __future__ import annotations

import gspread

DEDUPE_TAB = "__dedupe"


def load_dedupe_set(ss: gspread.Spreadsheet, max_rows: int = 5000) -> set[str]:
    ws = ss.worksheet(DEDUPE_TAB)
    values = ws.col_values(1)  # includes header
    keys = [v.strip() for v in values[1:max_rows + 1] if v and v.strip()]
    return set(keys)


def append_dedupe_keys(ws: gspread.Worksheet, keys: list[str]) -> None:
    if not keys:
        return
    ws.append_rows([[k] for k in keys], value_input_option="RAW")