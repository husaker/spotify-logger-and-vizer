"""Google Sheets REST access. Mutation methods are fenced in shadow/health mode."""
import json
import re
import time
from urllib.parse import quote
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from .http import ApiError, json_response
from .tokens import base64
from .timestamps import now_iso

GOOGLE_TOKEN = "https://oauth2.googleapis.com/token"
SCOPES = "https://www.googleapis.com/auth/spreadsheets"


class Sheets:
    def __init__(self, settings, http, policy="full"):
        self.settings, self.http, self.policy = settings, http, policy
        self.token, self.expires = None, 0

    def access_token(self):
        if self.token and self.expires > time.time() + 60:
            return self.token
        now = int(time.time())
        account = self.settings.service_account
        header = base64(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode())
        claims = base64(json.dumps({"iss": account["client_email"], "scope": SCOPES, "aud": GOOGLE_TOKEN,
                                   "iat": now, "exp": now + 3600}, separators=(",", ":")).encode())
        message = f"{header}.{claims}".encode()
        key = serialization.load_pem_private_key(account["private_key"].encode(), password=None)
        assertion = f"{header}.{claims}.{base64(key.sign(message, padding.PKCS1v15(), hashes.SHA256()))}"
        result = json_response(self.http.request("POST", GOOGLE_TOKEN, data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion}), "Google authentication")
        self.token = result["access_token"]
        self.expires = time.time() + result["expires_in"]
        return self.token

    def call(self, method, suffix="", **kwargs):
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{quote(self.settings.sheet_id, safe='')}{suffix}"
        return json_response(self.http.request(method, url, headers={"authorization": f"Bearer {self.access_token()}"}, **kwargs), "Google Sheets")

    def read_ranges(self, ranges):
        result = self.call("GET", "/values:batchGet", params=[*(('ranges', value) for value in ranges), ("majorDimension", "ROWS")])
        values = result.get("valueRanges", [])
        if len(values) != len(ranges):
            raise ApiError("Incomplete Google Sheets response")
        return {name: value.get("values", []) for name, value in zip(ranges, values)}

    def read_state(self):
        rows = self.read_ranges(["__app_state!A2:B"])["__app_state!A2:B"]
        return {row[0]: str(row[1]) if len(row) > 1 else "" for row in rows if row and row[0]}

    def _full_write(self):
        if self.policy != "full":
            raise ApiError("Collection writes are disabled in this mode")

    @staticmethod
    def _credential_rows(rows):
        allowed = {"refresh_token_enc", "refresh_token_enc_v2", "updated_at"}
        return all(len(row) == 2 and row[0] in allowed for row in rows)

    def batch_write(self, updates, *, credential=False):
        if not updates:
            return
        if credential and self.policy == "credentials":
            if any(not re.fullmatch(r"__app_state!A\d+:B\d+", item["range"]) or not self._credential_rows(item["values"]) for item in updates):
                raise ApiError("Only credential rotation is permitted in this mode")
        else:
            self._full_write()
        self.call("POST", "/values:batchUpdate", json={"valueInputOption": "RAW", "data": updates})

    def append(self, sheet, rows, *, credential=False):
        if not rows:
            return
        if credential and self.policy == "credentials":
            if sheet != "__app_state!A:B" or not self._credential_rows(rows):
                raise ApiError("Only credential rotation is permitted in this mode")
        else:
            self._full_write()
        self.call("POST", f"/values/{quote(sheet, safe='')}:append",
                  params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"},
                  json={"majorDimension": "ROWS", "values": rows})

    def write_state(self, changes):
        credential = set(changes) <= {"refresh_token_enc", "refresh_token_enc_v2"}
        if self.policy == "read_only" or self.policy == "credentials" and not credential:
            raise ApiError("State writes are disabled in this mode")
        rows = self.read_ranges(["__app_state!A1:B"])["__app_state!A1:B"]
        positions = {row[0]: index + 2 for index, row in enumerate(rows[1:]) if row and row[0]}
        updates, additions = [], []
        for key, value in {**changes, "updated_at": now_iso()}.items():
            row = [key, str(value)]
            if key in positions:
                position = positions[key]
                updates.append({"range": f"__app_state!A{position}:B{position}", "values": [row]})
            else:
                additions.append(row)
        self.batch_write(updates, credential=credential)
        self.append("__app_state!A:B", additions, credential=credential)

    def ensure_log_rows(self, last):
        self._full_write()
        data = self.call("GET", params={"fields": "sheets.properties"})
        log = next((s["properties"] for s in data.get("sheets", []) if s["properties"]["title"] == "log"), None)
        if not log or not log.get("gridProperties", {}).get("rowCount"):
            raise ApiError("Listening log worksheet is unavailable")
        count = log["gridProperties"]["rowCount"]
        if last > count:
            self.call("POST", ":batchUpdate", json={"requests": [{"appendDimension": {
                "sheetId": log["sheetId"], "dimension": "ROWS", "length": max(1000, last - count)}}]})
