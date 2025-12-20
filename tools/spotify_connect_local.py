from __future__ import annotations

import argparse
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from app.crypto import encrypt_str
from app.sheets_client import SheetsClient
from app.spotify_auth import (
    build_auth_url,
    exchange_code_for_token,
    get_spotify_user_id,
    make_state,
)
from common.config import load_settings
from worker.app_state import write_app_state_kv

import os
print("REDIRECT:", os.getenv("SPOTIFY_REDIRECT_URI"))


class CallbackHandler(BaseHTTPRequestHandler):
    server_version = "SpotifyOAuthCallback/1.0"
    code: str | None = None
    state: str | None = None
    error: str | None = None

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if "error" in qs:
            CallbackHandler.error = qs["error"][0]
        if "code" in qs:
            CallbackHandler.code = qs["code"][0]
        if "state" in qs:
            CallbackHandler.state = qs["state"][0]

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"<h2>OK, you can close this tab and return to terminal.</h2>")

    def log_message(self, fmt, *args):
        return  # silence


def run_local_server(host: str, port: int) -> HTTPServer:
    httpd = HTTPServer((host, port), CallbackHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet", required=True, help="User Google Sheet ID to write tokens into (__app_state)")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    settings = load_settings()

    client_id = settings_spotify("SPOTIFY_CLIENT_ID")
    client_secret = settings_spotify("SPOTIFY_CLIENT_SECRET")
    redirect_uri = settings_spotify("SPOTIFY_REDIRECT_URI")

    scopes = ["user-read-recently-played", "user-read-email", "user-read-private"]

    # start callback server
    httpd = run_local_server(args.host, args.port)

    state = make_state()
    auth_url = build_auth_url(client_id, redirect_uri, scopes, state)
    print("Opening browser for Spotify auth...")
    print(auth_url)
    webbrowser.open(auth_url)

    print("Waiting for callback on", redirect_uri, "...")
    # wait until we get code
    while CallbackHandler.code is None and CallbackHandler.error is None:
        pass

    httpd.shutdown()

    if CallbackHandler.error:
        raise SystemExit(f"Spotify auth error: {CallbackHandler.error}")

    if CallbackHandler.state != state:
        raise SystemExit("State mismatch — aborting (possible CSRF).")

    code = CallbackHandler.code
    assert code is not None

    tokens = exchange_code_for_token(client_id, client_secret, redirect_uri, code)

    if not tokens.refresh_token:
        raise SystemExit("No refresh_token returned. Make sure you use Authorization Code flow and show_dialog=true.")

    spotify_user_id = get_spotify_user_id(tokens.access_token)
    refresh_token_enc = encrypt_str(tokens.refresh_token, settings.fernet_key)

    # write into __app_state
    sheets = SheetsClient.from_service_account_json(settings.google_service_account_json)
    ss = sheets.open_by_key(args.sheet)

    write_app_state_kv(
        ss,
        {
            "enabled": "true",
            "spotify_user_id": spotify_user_id,
            "refresh_token_enc": refresh_token_enc,
            "last_error": "",
        },
    )

    print("✅ Connected Spotify and saved tokens into __app_state")
    print("spotify_user_id:", spotify_user_id)


def settings_spotify(name: str) -> str:
    import os
    v = os.getenv(name, "")
    if not v:
        raise SystemExit(f"Missing {name} in environment/.env")
    return v


if __name__ == "__main__":
    main()