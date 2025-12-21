from __future__ import annotations

import argparse
from typing import List

import gspread

from app.sheets_client import SheetsClient
from app.gspread_retry import gcall
from common.config import load_settings
from worker.cache_sync import enrich_caches_for_tracks

LOG_TAB = "log"


def _read_last_track_ids(ss: gspread.Spreadsheet, *, max_rows: int) -> list[str]:
    ws = ss.worksheet(LOG_TAB)

    # Read all values (simple & reliable). If huge, we can optimize later.
    rows = gcall(lambda: ws.get_all_values())
    if len(rows) <= 1:
        return []

    # rows[0] = header; log schema: Date, Track, Artist, Spotify ID, URL
    body = rows[1:]
    tail = body[-max_rows:] if max_rows > 0 else body

    ids: list[str] = []
    for r in tail:
        if len(r) >= 4:
            tid = (r[3] or "").strip()
            if tid:
                ids.append(tid)

    # unique keep order
    seen = set()
    uniq: list[str] = []
    for t in ids:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sheet", required=True, help="User sheet id")
    p.add_argument("--rows", type=int, default=5000, help="How many last log rows to scan")
    args = p.parse_args()

    settings = load_settings()
    sheets = SheetsClient.from_service_account_json(settings.google_service_account_json)

    ss = sheets.open_by_key(args.sheet)

    track_ids = _read_last_track_ids(ss, max_rows=args.rows)
    if not track_ids:
        print("No track ids found in log.")
        return

    # We need an access token; easiest is to call normal sync flow and reuse it,
    # but here we backfill only cache, so we assume you can pass an access_token from your flow.
    # For now, we do a minimal token refresh by reading refresh_token_enc from __app_state.

    from worker.app_state import read_app_state
    from app.crypto import decrypt_str
    from app.spotify_auth import refresh_access_token

    state = read_app_state(ss)
    refresh_token_enc = state.get("refresh_token_enc") or ""
    if not refresh_token_enc:
        raise RuntimeError("Sheet is not connected: refresh_token_enc missing in __app_state")

    refresh_token = decrypt_str(refresh_token_enc, settings.fernet_key)
    tokens = refresh_access_token(settings.spotify_client_id, settings.spotify_client_secret, refresh_token)
    access_token = tokens.access_token

    enrich_caches_for_tracks(
        ss,
        access_token=access_token,
        track_ids=track_ids,
        ttl_days=settings.cache_ttl_days,
    )

    print(f"✅ Backfill done. Tracks processed: {len(track_ids)}")


if __name__ == "__main__":
    main()