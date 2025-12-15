from __future__ import annotations

from cryptography.fernet import Fernet


def _get_fernet(key: str) -> Fernet:
    return Fernet(key.encode("utf-8"))


def encrypt_token(key: str, token: str) -> str:
    f = _get_fernet(key)
    return f.encrypt(token.encode("utf-8")).decode("utf-8")


def decrypt_token(key: str, token_enc: str) -> str:
    f = _get_fernet(key)
    return f.decrypt(token_enc.encode("utf-8")).decode("utf-8")
