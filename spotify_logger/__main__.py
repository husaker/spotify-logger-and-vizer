"""Use `python -m spotify_logger --help`. Secrets never appear in CLI errors."""
import argparse
import os
import sys
from .config import Settings, load_local_env
from .google import Sheets
from .health import check_health
from .http import ApiError, HttpClient, SpotifyInvalidGrant
from .spotify import Spotify
from .sync import collect, check_connection


def main(argv=None):
    parser = argparse.ArgumentParser(description="Spotify Logger: collection, shadow validation and health checks")
    parser.add_argument("mode", choices=["sync", "dry-run", "check", "health", "authorize"])
    parser.add_argument("--env-file", help="Local dotenv configuration; never used implicitly by Actions")
    args = parser.parse_args(argv)
    if args.mode == "sync" and (os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_WORKFLOW") != "Spotify Logger Sync"):
        print("Run collection through the serialized Spotify Logger Sync workflow.", file=sys.stderr)
        return 1
    try:
        if args.env_file:
            load_local_env(args.env_file)
        settings = Settings.from_env(health=args.mode == "health")
        if args.mode == "authorize":
            from .authorize import authorize
            authorize(settings)
            return 0
        policy = "full" if args.mode == "sync" else "read_only" if args.mode == "health" else "credentials"
        http = HttpClient(seconds=30 if args.mode == "health" else 120)
        sheets = Sheets(settings, http, policy=policy)
        spotify = Spotify(settings, http)
        if args.mode == "health":
            check_health(sheets)
            print("Logger health is OK: last successful sync is less than 30 minutes old.")
        elif args.mode == "check":
            check_connection(settings, sheets, spotify)
            print("Spotify connection verified; log, caches and cursor were not changed.")
        else:
            result = collect(settings, sheets, spotify, dry_run=args.mode == "dry-run")
            if args.mode == "dry-run":
                print(f"Shadow validation OK: {result['fetched_events']} fetched; {result['would_add']} new; "
                      f"metadata: {result['track_metadata']} tracks, {result['album_metadata']} albums, {result['artist_metadata']} artists. "
                      "No log, cache or cursor writes; OAuth rotation alone may be saved.")
            else:
                print(f"Sync completed: {result['added']} new events; last success {result['last_sync_at']}.")
        return 0
    except Exception as error:
        if isinstance(error, SpotifyInvalidGrant):
            print("Stored Spotify authorization expired or was revoked; reconnect locally.", file=sys.stderr)
        elif isinstance(error, ApiError):
            print(str(error), file=sys.stderr)  # Controlled messages, no raw HTTP bodies.
        elif isinstance(error, ValueError) and str(error).startswith("Missing configuration:"):
            print(str(error), file=sys.stderr)  # Names only, never values.
        print("Spotify Logger failed. Check the Actions run, configuration and Sheet status; no secrets were printed.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
