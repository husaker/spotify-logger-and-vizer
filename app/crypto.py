from __future__ import annotations

from cryptography.fernet import Fernet


def encrypt_str(plaintext: str, fernet_key: str) -> str:
    f = Fernet(fernet_key.encode())
    token = f.encrypt(plaintext.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_str(ciphertext: str, fernet_key: str) -> str:
    f = Fernet(fernet_key.encode())
    pt = f.decrypt(ciphertext.encode("utf-8"))
    return pt.decode("utf-8")