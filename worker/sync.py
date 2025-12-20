from __future__ import annotations

import argparse

from app.sheets_client import SheetsClient
from common.config import load_settings
from worker.registry import ensure_registry_headers, read_registry, REGISTRY_TAB


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one sync iteration and exit")
    parser.add_argument("--sheet", type=str, default=None, help="Sync a specific user sheet id only")
    args = parser.parse_args()

    settings = load_settings()
    sheets = SheetsClient.from_service_account_json(settings.google_service_account_json)

    ss = sheets.open_by_key(settings.registry_sheet_id)
    ws = sheets.get_or_create_worksheet(ss, REGISTRY_TAB, rows=1000, cols=10)
    ensure_registry_headers(ws)

    users = read_registry(ws)

    print(f"✅ Registry loaded. Rows: {len(users)}")
    enabled_cnt = sum(1 for u in users if u.enabled)
    print(f"Enabled users: {enabled_cnt}")

    if args.sheet:
        print(f"Target sheet mode: {args.sheet} (sync later)")
    print("✅ Step 2 OK (Google Sheets access)")


if __name__ == "__main__":
    main()