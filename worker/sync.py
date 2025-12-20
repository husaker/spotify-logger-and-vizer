from __future__ import annotations

import argparse

from app.sheets_client import SheetsClient
from common.config import load_settings
from worker.registry import (
    REGISTRY_TAB,
    ensure_registry_headers,
    read_registry,
    upsert_registry_user,
)
from worker.user_sheet import ensure_user_sheet_initialized


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one sync iteration and exit")
    parser.add_argument("--sheet", type=str, default=None, help="Sync a specific user sheet id only (later)")
    parser.add_argument("--init-sheet", type=str, default=None, help="Initialize a user sheet and register it")
    parser.add_argument("--timezone", type=str, default="UTC", help="Timezone to save in __app_state when initializing")
    args = parser.parse_args()

    settings = load_settings()
    sheets = SheetsClient.from_service_account_json(settings.google_service_account_json)

    # Open registry
    registry_ss = sheets.open_by_key(settings.registry_sheet_id)
    registry_ws = sheets.get_or_create_worksheet(registry_ss, REGISTRY_TAB, rows=1000, cols=10)
    ensure_registry_headers(registry_ws)

    # Init mode (as before)
    if args.init_sheet:
        user_ss = sheets.open_by_key(args.init_sheet)
        ensure_user_sheet_initialized(user_ss, timezone_name=args.timezone)
        upsert_registry_user(registry_ws, user_sheet_id=args.init_sheet, enabled=True)
        print(f"✅ Initialized user sheet + registered: {args.init_sheet}")
        return

    # Once mode: read registry and print enabled
    users = read_registry(registry_ws)
    enabled_users = [u for u in users if u.enabled]

    print(f"✅ Registry loaded. Rows: {len(users)}")
    print(f"Enabled users: {len(enabled_users)}")
    if enabled_users:
        print("Enabled sheet_ids:")
        for u in enabled_users:
            print(" -", u.user_sheet_id)

    print("✅ Step 4 OK (registry read in --once)")


if __name__ == "__main__":
    main()