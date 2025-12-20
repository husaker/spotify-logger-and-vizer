from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


@dataclass
class SheetsClient:
    gc: gspread.Client

    @classmethod
    def from_service_account_json(cls, sa_json: str) -> "SheetsClient":
        info: dict[str, Any] = json.loads(sa_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        gc = gspread.authorize(creds)
        return cls(gc=gc)

    def open_by_key(self, sheet_id: str) -> gspread.Spreadsheet:
        return self.gc.open_by_key(sheet_id)

    def get_or_create_worksheet(self, ss: gspread.Spreadsheet, title: str, rows: int = 1000, cols: int = 20) -> gspread.Worksheet:
        try:
            return ss.worksheet(title)
        except gspread.WorksheetNotFound:
            return ss.add_worksheet(title=title, rows=rows, cols=cols)