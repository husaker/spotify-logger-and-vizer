from __future__ import annotations

import argparse
from datetime import datetime, timezone

from app.sheets_client import SheetsClient
from common.config import load_settings
from worker.registry import (
    REGISTRY_TAB,
    ensure_registry_headers,
    read_registry,
    upsert_registry_user,
    update_registry_status,
)
from worker.sync_one import sync_user_sheet
from worker.user_sheet import ensure_user_sheet_initialized


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run one sync iteration and exit")
    parser.add_argument("--sheet", type=str, default=None, help="Sync a specific user sheet id only")
    parser.add_argument("--init-sheet", type=str, default=None, help="Initialize a user sheet and register it")
    parser.add_argument("--timezone", type=str, default="UTC", help="Timezone to save in __app_state when initializing")
    args = parser.parse_args()

    settings = load_settings()
    sheets = SheetsClient.from_service_account_json(settings.google_service_account_json)

    # Open registry
    registry_ss = sheets.open_by_key(settings.registry_sheet_id)
    registry_ws = sheets.get_or_create_worksheet(registry_ss, REGISTRY_TAB, rows=1000, cols=12)
    ensure_registry_headers(registry_ws)

    # Init mode
    if args.init_sheet:
        user_ss = sheets.open_by_key(args.init_sheet)
        ensure_user_sheet_initialized(user_ss, timezone_name=args.timezone)
        upsert_registry_user(registry_ws, user_sheet_id=args.init_sheet, enabled=True)
        print(f"✅ Initialized user sheet + registered: {args.init_sheet}")
        return

    # Determine which sheets to sync
    if args.sheet:
        sheet_ids = [args.sheet]
        print("🎯 Target sheet:", args.sheet)
    else:
        users = read_registry(registry_ws)
        enabled_users = [u for u in users if u.enabled]
        sheet_ids = [u.user_sheet_id for u in enabled_users]

        print(f"✅ Registry loaded. Rows: {len(users)}")
        print(f"Enabled users: {len(enabled_users)}")
        if sheet_ids:
            print("Enabled sheet_ids:")
            for sid in sheet_ids:
                print(" -", sid)

    if not sheet_ids:
        print("Nothing to sync.")
        return

    # Run sync
    total_added = 0
    for sid in sheet_ids:
        try:
            user_ss = sheets.open_by_key(sid)

            added = sync_user_sheet(
                user_ss,
                dedup_read_rows=settings.dedup_read_rows,
                lookback_minutes=settings.sync_lookback_minutes,
                fernet_key=settings.fernet_key,
                spotify_client_id=settings.spotify_client_id,
                spotify_client_secret=settings.spotify_client_secret,
                cache_ttl_days=settings.cache_ttl_days,
            )

            total_added += added
            now = datetime.now(timezone.utc).isoformat()

            # IMPORTANT: update_registry_status is keyword-only (after '*')
            update_registry_status(
                registry_ws,
                user_sheet_id=sid,
                last_sync_at=now,
                last_error="",
            )

            print(f"✅ Synced {sid}: +{added} rows")

        except Exception as e:
            update_registry_status(
                registry_ws,
                user_sheet_id=sid,
                last_sync_at=None,
                last_error=str(e),
            )
            print(f"❌ Sync failed for {sid}: {e}")

    print(f"✅ Done. Total appended rows: {total_added}")


if __name__ == "__main__":
    main()