import copy
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import unittest
from unittest.mock import patch
from cryptography.fernet import Fernet, InvalidToken
from spotify_logger.config import Settings
from spotify_logger.google import Sheets
from spotify_logger.health import health_status, check_health
from spotify_logger.http import ApiError, HttpClient, SpotifyInvalidGrant
from spotify_logger.spotify import Spotify
from spotify_logger.sync import collect, build_plan, RANGES
from spotify_logger.timestamps import parse_ms, iso_ms, TimestampGuard
from spotify_logger.tokens import TokenCodec, select_token

KEY = Fernet.generate_key().decode()
NOW = "2026-11-08T06:00:00.000Z"
SETTINGS = Settings("workbook", {"client_email": "test@example.test", "private_key": "test"}, "client", "secret", KEY)


def event(second=0, artist="artist", track="track"):
    return {"played_at": f"2026-11-08T05:45:{second:02d}.123Z", "track": {
        "id": track, "name": "Track", "duration_ms": 180123,
        "artists": [{"id": artist, "name": "Artist"}],
        "album": {"id": "album", "name": "Album", "images": [{"url": "https://i.scdn.co/image/cover"}], "release_date": "2020-01-01"},
        "external_urls": {"spotify": "https://open.spotify.com/track/track"}}}


class FakeSheets:
    def __init__(self, log=None):
        self.state = {"refresh_token_enc": Fernet(KEY.encode()).encrypt(b"refresh").decode()}
        self.tables = {name: [] for name in RANGES}
        self.tables[RANGES[0]] = copy.deepcopy(log or [])
        self.writes = []
        self.lose_response = False
        self.reject_write = False

    @property
    def log(self):
        return self.tables[RANGES[0]]

    def read_state(self):
        return self.state.copy()

    def read_ranges(self, names):
        return {name: copy.deepcopy(self.tables[name]) for name in names}

    def write_state(self, changes):
        self.writes.append(("state", changes.copy()))
        self.state.update(changes)

    def ensure_log_rows(self, last):
        self.writes.append(("ensure", last))

    def batch_write(self, updates):
        for update in updates:
            sheet, cell_range = update["range"].split("!")
            if sheet == "log" and self.reject_write:
                raise ApiError("write rejected")
            table = next(name for name in self.tables if name.startswith(sheet + "!"))
            position = int(re.search(r"A(\d+)", cell_range)[1]) - 2
            for offset, row in enumerate(update["values"]):
                while len(self.tables[table]) <= position + offset:
                    self.tables[table].append([])
                self.tables[table][position + offset] = copy.deepcopy(row)
            self.writes.append(("range", update["range"]))
            if sheet == "log" and self.lose_response:
                self.lose_response = False
                raise ApiError("response lost after commit")

    def append(self, name, rows):
        if rows:
            table = next(value for value in self.tables if value.startswith(name.split("!")[0] + "!"))
            self.tables[table].extend(copy.deepcopy(rows))
            self.writes.append(("append", name))


class FakeSpotify:
    def __init__(self, items=None, rotate=False):
        self.items = items or [event(2), event(1), event(1)]
        self.rotate = rotate
        self.artist_calls = []
        self.fail_artist = False
        self.expired = False

    def refresh(self, token):
        if self.expired:
            raise SpotifyInvalidGrant("expired")
        return {"access_token": "access", **({"refresh_token": "rotated"} if self.rotate else {})}

    def recent(self, token, after):
        self.after = after
        return copy.deepcopy(self.items)

    def artist(self, token, artist):
        self.artist_calls.append(artist)
        if self.fail_artist:
            raise ApiError("metadata unavailable")
        return {"id": artist, "name": "Artist", "images": [{"url": "https://i.scdn.co/image/artist"}], "genres": ["rock", "indie"]}


class CollectionTests(unittest.TestCase):
    def test_repeat_import_filters_duplicates_and_preserves_replays(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        self.assertEqual(collect(SETTINGS, sheets, spotify, now=NOW)["added"], 2)
        self.assertEqual(collect(SETTINGS, sheets, spotify, now=NOW)["added"], 0)
        self.assertEqual([row[0] for row in sheets.log], [event(1)["played_at"], event(2)["played_at"]])
        self.assertEqual(sheets.state["last_sync_at"], NOW)

    def test_metadata_covers_duration_albums_genres_and_ids(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        collect(SETTINGS, sheets, spotify, now=NOW)
        track = sheets.tables[RANGES[1]][0]
        self.assertEqual(track, ["track", "Track", 180123, "album", "https://i.scdn.co/image/cover", "artist", "artist", "https://open.spotify.com/track/track", NOW])
        self.assertEqual(sheets.tables[RANGES[2]][0], ["artist", "Artist", "https://i.scdn.co/image/artist", "rock; indie", "rock", NOW])
        self.assertEqual(sheets.tables[RANGES[3]][0], ["album", "Album", "https://i.scdn.co/image/cover", "2020-01-01", NOW])
        collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertEqual(spotify.artist_calls, ["artist"])
        self.assertEqual(len(sheets.tables[RANGES[1]]), 1)

    def test_lost_write_response_is_recovered_without_duplicate(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        sheets.lose_response = True
        with self.assertRaises(ApiError):
            collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertNotIn("last_synced_after_ts", sheets.state)
        self.assertEqual(len(sheets.log), 2)
        self.assertEqual(collect(SETTINGS, sheets, spotify, now=NOW)["added"], 0)
        self.assertEqual(len(sheets.log), 2)

    def test_rejected_write_remains_eligible(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        sheets.reject_write = True
        with self.assertRaises(ApiError):
            collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertEqual(sheets.log, [])
        sheets.reject_write = False
        self.assertEqual(collect(SETTINGS, sheets, spotify, now=NOW)["added"], 2)

    def test_metadata_failure_after_log_write_does_not_repeat_log(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        spotify.fail_artist = True
        with self.assertRaises(ApiError):
            collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertEqual(len(sheets.log), 2)
        spotify.fail_artist = False
        self.assertEqual(collect(SETTINGS, sheets, spotify, now=NOW)["added"], 0)
        self.assertEqual(sheets.state["consecutive_failures"], "0")
        self.assertEqual(sheets.state["last_error"], "")

    def test_rotation_saved_before_later_failure(self):
        sheets, spotify = FakeSheets(), FakeSpotify(rotate=True)
        sheets.reject_write = True
        with self.assertRaises(ApiError):
            collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertEqual(Fernet(KEY.encode()).decrypt(sheets.state["refresh_token_enc"].encode()), b"rotated")

    def test_shadow_preserves_log_caches_cursor_and_last_success(self):
        sheets, spotify = FakeSheets(), FakeSpotify(rotate=True)
        sheets.state.update(last_sync_at="2026-11-08T05:00:00.000Z", last_synced_after_ts="1000")
        before = copy.deepcopy(sheets.tables)
        result = collect(SETTINGS, sheets, spotify, dry_run=True, now=NOW)
        self.assertEqual(result["would_add"], 2)
        self.assertEqual(sheets.tables, before)
        self.assertEqual(sheets.state["last_sync_at"], "2026-11-08T05:00:00.000Z")
        self.assertEqual(sheets.state["last_synced_after_ts"], "1000")
        self.assertEqual([kind for kind, _ in sheets.writes], ["state"])
        self.assertEqual(set(sheets.writes[0][1]), {"refresh_token_enc"})

    def test_shadow_failure_does_not_write_error_state(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        spotify.fail_artist = True
        before = sheets.state.copy()
        with self.assertRaises(ApiError):
            collect(SETTINGS, sheets, spotify, dry_run=True, now=NOW)
        self.assertEqual(sheets.state, before)
        self.assertEqual(sheets.writes, [])

    def test_expired_token_marks_reauthorization_without_claiming_success(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        spotify.expired = True
        with self.assertRaises(SpotifyInvalidGrant):
            collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertEqual(sheets.state["reauth_required"], "true")
        self.assertNotIn("last_sync_at", sheets.state)
        self.assertEqual(sheets.log, [])

    def test_blank_rows_keep_physical_row_offsets_and_first_equal_timestamp_wins(self):
        sheets = FakeSheets([["old", "", "", "old"], [], ["2026-11-08T05:45:00.123Z", "", "", "old"]])
        spotify = FakeSpotify([event(1, track="first"), event(1, track="second")])
        collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertIn(("range", "log!A5:E5"), sheets.writes)
        self.assertEqual(sheets.log[3][3], "first")

    def test_artist_lookup_is_capped_at_25(self):
        sheets = FakeSheets()
        spotify = FakeSpotify([event(i, artist=f"a{i}") for i in range(30)])
        collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertEqual(len(spotify.artist_calls), 25)

    def test_overlap_and_success_cursor_match_existing_contract(self):
        sheets, spotify = FakeSheets(), FakeSpotify()
        cursor = parse_ms(event(1)["played_at"])
        sheets.state["last_synced_after_ts"] = str(cursor)
        collect(SETTINGS, sheets, spotify, now=NOW)
        self.assertEqual(spotify.after, cursor - 120 * 60_000)
        self.assertEqual(sheets.state["last_synced_after_ts"], str(parse_ms(event(2)["played_at"])))


class TimestampTests(unittest.TestCase):
    def test_legacy_midnight_and_offsets(self):
        self.assertEqual(iso_ms(parse_ms("November 12, 2025 at 12:00AM")), "2025-11-11T21:00:00.000Z")
        self.assertEqual(parse_ms("2026-11-08T08:45:00.123+03:00"), parse_ms("2026-11-08T05:45:00.123Z"))
        for value in ["", "bad", "February 30, 2026 at 2:00PM", "2026-11-08T05:00:00"]:
            self.assertIsNone(parse_ms(value))

    def test_precision_and_legacy_minute_guard(self):
        guard = TimestampGuard([["November 8, 2026 at 8:45AM"], ["2026-11-08T05:46:00.123Z"]])
        self.assertIsNone(guard.claim("2026-11-08T05:45:45.999Z"))
        self.assertIsNone(guard.claim("2026-11-08T08:46:00.123+03:00"))
        self.assertEqual(guard.claim("2026-11-08T05:46:00.124999Z"), "2026-11-08T05:46:00.124Z")
        self.assertIsNone(guard.claim("2026-11-08T05:46:00.124Z"))


class TokenTests(unittest.TestCase):
    def test_existing_typescript_v2_ciphertext_is_readable(self):
        vector = json.loads(Path(__file__).with_name("token-vector.json").read_text())
        codec = TokenCodec("refresh_token_enc_v2", vector["key"])
        self.assertEqual(codec.decrypt(vector["v2"]), vector["plaintext"])
        self.assertEqual(codec.decrypt(codec.encrypt("new")), "new")
        with self.assertRaises(Exception):
            TokenCodec("refresh_token_enc_v2", Fernet.generate_key().decode()).decrypt(vector["v2"])

    def test_official_fernet_vector(self):
        codec = TokenCodec("refresh_token_enc", "cw_0x689RpI-jtRR7oE8h_eQsKImvJapLeSbXpwF4e4=")
        token = "gAAAAAAdwJ6wAAECAwQFBgcICQoLDA0ODy021cpGVWKZ_eEwCGM4BLLF_5CV9dOPmrhuVUPgJobwOz7JcbmrR64jVmpU4IwqDA=="
        self.assertEqual(codec.decrypt(token), "hello")
        self.assertEqual(codec.decrypt(codec.encrypt("rotate-🔑")), "rotate-🔑")
        with self.assertRaises(Exception):
            codec.decrypt(token[:-10] + "invalid")

    def test_bad_modern_key_never_downgrades_to_fernet(self):
        sheets = FakeSheets()
        key = Fernet.generate_key().decode()
        state = {**sheets.state, "refresh_token_enc_v2": TokenCodec("refresh_token_enc_v2", key).encrypt("new")}
        settings = Settings("s", {}, fernet_key=KEY, aes_key=Fernet.generate_key().decode())
        with self.assertRaises(Exception):
            select_token(settings, state)
        codec, value = select_token(SETTINGS, state)
        self.assertEqual(codec.field_name, "refresh_token_enc")
        self.assertEqual(value, "refresh")


class HealthTests(unittest.TestCase):
    def test_stale_missing_future_and_reauth_fail(self):
        now = parse_ms(NOW)
        for state in [{}, {"last_sync_at": "invalid"}, {"last_sync_at": iso_ms(now - 30 * 60_000)},
                      {"last_sync_at": iso_ms(now + 60_001)}, {"last_sync_at": NOW, "reauth_required": "true"}]:
            self.assertFalse(health_status(state, now)[0])
        self.assertTrue(health_status({"last_sync_at": iso_ms(now - 30 * 60_000 + 1)}, now)[0])

    def test_probe_is_read_only(self):
        sheets = FakeSheets()
        sheets.state["last_sync_at"] = iso_ms(int(__import__('time').time() * 1000))
        before = sheets.state.copy()
        check_health(sheets)
        self.assertEqual(sheets.state, before)
        self.assertEqual(sheets.writes, [])


class SafetyTests(unittest.TestCase):
    def test_credentials_only_cannot_mutate_log_cache_or_cursor(self):
        sheets = Sheets(SETTINGS, None, policy="credentials")
        for operation in [lambda: sheets.batch_write([{"range": "log!A2:E2", "values": [["x"]]}], credential=True),
                          lambda: sheets.append("log!A:E", [["x"]], credential=True),
                          lambda: sheets.write_state({"last_sync_at": NOW}),
                          lambda: sheets.ensure_log_rows(1001),
                          lambda: sheets.batch_write([{"range": "__app_state!A2:B2", "values": [["last_sync_at", NOW]]}], credential=True)]:
            with self.assertRaises(ApiError):
                operation()
        readonly = Sheets(SETTINGS, None, policy="read_only")
        with self.assertRaises(ApiError):
            readonly.write_state({"refresh_token_enc": "x"})

    def test_local_sync_is_rejected_without_credentials_or_network(self):
        env = {"PATH": os.environ["PATH"]}
        result = subprocess.run([sys.executable, "-m", "spotify_logger", "sync"], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("serialized", result.stderr)

    def test_missing_config_reports_names_not_secret_values(self):
        env = {"PATH": os.environ["PATH"], "GITHUB_ACTIONS": "true", "GITHUB_WORKFLOW": "Spotify Logger Sync", "SPOTIFY_CLIENT_SECRET": "never-print-this"}
        result = subprocess.run([sys.executable, "-m", "spotify_logger", "sync"], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("GOOGLE_SERVICE_ACCOUNT_JSON", result.stderr)
        self.assertNotIn("never-print-this", result.stderr)


class Response:
    def __init__(self, status=200, data=None, headers=None):
        self.status_code, self.data, self.headers = status, data or {}, headers or {}
    def json(self):
        return self.data


class HttpTests(unittest.TestCase):
    def test_retry_is_only_opt_in_and_obeys_deadline(self):
        class Session:
            def __init__(self, responses):
                self.responses, self.calls = responses, 0
            def request(self, *args, **kwargs):
                self.calls += 1
                return self.responses.pop(0)
        time = [0.0]
        session = Session([Response(429, headers={"Retry-After": "2"}), Response(200)])
        http = HttpClient(10, session=session, clock=lambda: time[0], sleep=lambda x: time.__setitem__(0, time[0]+x))
        self.assertEqual(http.request("GET", "https://example.test", retry=True).status_code, 200)
        self.assertEqual(time[0], 2)
        session = Session([Response(503), Response(200)])
        response = HttpClient(10, session=session).request("POST", "https://example.test")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(session.calls, 1)
        session = Session([Response(429, headers={"Retry-After": "30"})])
        with self.assertRaises(ApiError):
            HttpClient(10, session=session, clock=lambda: 0).request("GET", "https://example.test", retry=True)

    def test_pagination_stays_on_spotify_and_is_capped(self):
        class Http:
            def __init__(self): self.calls = 0
            def request(self, *args, **kwargs):
                self.calls += 1
                return Response(data={"items": [event()], "next": "https://api.spotify.com/v1/next"})
        http = Http()
        self.assertEqual(len(Spotify(SETTINGS, http).recent("token", 0)), 5)
        self.assertEqual(http.calls, 5)
        with self.assertRaises(ApiError):
            Spotify(SETTINGS, http)._get("https://evil.example/steal", "token")


if __name__ == "__main__":
    unittest.main()
