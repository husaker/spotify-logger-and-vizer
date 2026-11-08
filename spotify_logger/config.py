"""Explicit configuration; no registry discovery or secret values in errors."""
from dataclasses import dataclass, field
import json
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    sheet_id: str
    service_account: dict = field(repr=False)
    spotify_id: str = field(default="", repr=False)
    spotify_secret: str = field(default="", repr=False)
    fernet_key: str = field(default="", repr=False)
    aes_key: str = field(default="", repr=False)

    @classmethod
    def from_env(cls, *, health=False):
        required = ["USER_SHEET_ID", "GOOGLE_SERVICE_ACCOUNT_JSON"]
        if not health:
            required += ["SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET"]
        missing = [key for key in required if not os.environ.get(key)]
        if not health and not (os.environ.get("FERNET_KEY") or os.environ.get("TOKEN_ENCRYPTION_KEY")):
            missing.append("FERNET_KEY or TOKEN_ENCRYPTION_KEY")
        if missing:
            raise ValueError("Missing configuration: " + ", ".join(missing))
        try:
            account = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
            if not account.get("client_email") or not account.get("private_key"):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Invalid Google service-account configuration") from None
        return cls(os.environ["USER_SHEET_ID"], account,
                   os.environ.get("SPOTIFY_CLIENT_ID", ""), os.environ.get("SPOTIFY_CLIENT_SECRET", ""),
                   os.environ.get("FERNET_KEY", ""), os.environ.get("TOKEN_ENCRYPTION_KEY", ""))


def load_local_env(path=".env"):
    """Read simple dotenv assignments without executing shell code; env wins."""
    for raw in Path(path).read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)
    if not os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") and os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE"):
        os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"] = Path(os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"]).read_text()
