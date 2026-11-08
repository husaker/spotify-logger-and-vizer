"""Local OAuth callback; no externally hosted service or plaintext refresh-token output."""
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import secrets
import time
from urllib.parse import parse_qs, urlencode, urlparse
from .google import Sheets
from .http import HttpClient, json_response
from .tokens import TokenCodec
from .timestamps import now_iso


def authorize(settings):
    if os.environ.get("GITHUB_ACTIONS") == "true":
        raise ValueError("Run authorization locally")
    redirect = os.environ.get("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8888/callback")
    url = urlparse(redirect)
    if url.scheme != "http" or url.hostname != "127.0.0.1" or url.path != "/callback" or url.query or url.fragment or url.username or url.password:
        raise ValueError("Redirect must be an HTTP 127.0.0.1 loopback URL ending in /callback")
    codec = TokenCodec("refresh_token_enc_v2", settings.aes_key) if settings.aes_key else TokenCodec("refresh_token_enc", settings.fernet_key)
    codec.encrypt("validate-key")
    state = secrets.token_urlsafe(32)
    complete, succeeded = False, False

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # Standard HTTP logs would expose OAuth codes in callback URLs.

        def do_GET(self):
            nonlocal complete, succeeded
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            if parsed.path != "/callback":
                self.send_error(404)
                return
            if not secrets.compare_digest(params.get("state", [""])[0], state) or not params.get("code"):
                self.send_error(400, "Authorization was not completed; retry the command")
                return
            try:
                http = HttpClient(seconds=45)
                tokens = json_response(http.request("POST", "https://accounts.spotify.com/api/token",
                                       auth=(settings.spotify_id, settings.spotify_secret), data={
                                           "grant_type": "authorization_code", "code": params["code"][0], "redirect_uri": redirect}), "Spotify authorization")
                if not tokens.get("refresh_token"):
                    raise ValueError("Refresh token missing")
                now = datetime.now(timezone.utc)
                month = now.month + 6
                due = now.replace(day=1, year=now.year + (month - 1) // 12, month=(month - 1) % 12 + 1) + timedelta(days=now.day - 1)
                Sheets(settings, http).write_state({codec.field_name: codec.encrypt(tokens["refresh_token"]),
                     "spotify_authorized_at": now_iso(),
                     "spotify_reauth_due_at": due.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                     "reauth_required": "false", "last_error": ""})
                self.send_response(200)
                message = b"Spotify connected. You can close this tab."
                succeeded = True
                print("Encrypted authorization saved to Google Sheets; listening history was preserved.")
            except Exception:
                self.send_response(500)
                message = b"Could not save authorization. Check local settings and retry."
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(message)
            complete = True

    with HTTPServer(("127.0.0.1", url.port or 80), Handler) as server:
        server.timeout = 1
        print("Open in your browser:\nhttps://accounts.spotify.com/authorize?" + urlencode({
            "response_type": "code", "client_id": settings.spotify_id, "redirect_uri": redirect,
            "scope": "user-read-recently-played user-read-private", "state": state}), flush=True)
        deadline = time.monotonic() + 30 * 60
        while not complete and time.monotonic() < deadline:
            server.handle_request()
    if not succeeded:
        raise ValueError("Authorization did not finish; retry locally")
