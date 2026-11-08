"""Compatible with existing cryptography.fernet and TypeScript AES-GCM v2 state."""
from base64 import urlsafe_b64decode, urlsafe_b64encode
from dataclasses import dataclass, field
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def unbase64(value):
    return urlsafe_b64decode(value + "=" * (-len(value) % 4))


def base64(value):
    return urlsafe_b64encode(value).decode().rstrip("=")


@dataclass(frozen=True)
class TokenCodec:
    field_name: str
    key: str = field(repr=False)

    def decrypt(self, value):
        if self.field_name == "refresh_token_enc":
            return Fernet(self.key.encode()).decrypt(value.encode()).decode()
        version, nonce, encrypted = value.split(".")
        if version != "v2":
            raise ValueError("Unsupported token encryption format")
        return AESGCM(unbase64(self.key)).decrypt(unbase64(nonce), unbase64(encrypted), None).decode()

    def encrypt(self, value):
        if self.field_name == "refresh_token_enc":
            return Fernet(self.key.encode()).encrypt(value.encode()).decode()
        nonce = os.urandom(12)
        encrypted = AESGCM(unbase64(self.key)).encrypt(nonce, value.encode(), None)
        return f"v2.{base64(nonce)}.{base64(encrypted)}"


def select_token(settings, state):
    if settings.aes_key and state.get("refresh_token_enc_v2"):
        codec = TokenCodec("refresh_token_enc_v2", settings.aes_key)
    elif settings.fernet_key and state.get("refresh_token_enc"):
        codec = TokenCodec("refresh_token_enc", settings.fernet_key)
    else:
        raise ValueError("No stored authorization matches the configured encryption key")
    # Authentication errors deliberately do not fall back to another stored token.
    return codec, codec.decrypt(state[codec.field_name])
