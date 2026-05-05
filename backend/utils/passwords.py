# -*- coding: utf-8 -*-
import hashlib
import hmac
import os
from typing import Tuple


PBKDF2_ALGORITHM = "sha256"
PBKDF2_ITERATIONS = 390000
SALT_BYTES = 16
PBKDF2_PREFIX = "pbkdf2_sha256"


def _pbkdf2_hash(password: str, salt: bytes, iterations: int = PBKDF2_ITERATIONS) -> str:
    derived = hashlib.pbkdf2_hmac(
        PBKDF2_ALGORITHM,
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return derived.hex()


def hash_password(password: str) -> str:
    salt = os.urandom(SALT_BYTES)
    digest = _pbkdf2_hash(password, salt)
    return f"{PBKDF2_PREFIX}${PBKDF2_ITERATIONS}${salt.hex()}${digest}"


def _verify_pbkdf2(password: str, encoded_hash: str) -> bool:
    try:
        _prefix, iteration_text, salt_hex, digest_hex = encoded_hash.split("$", 3)
        iterations = int(iteration_text)
        salt = bytes.fromhex(salt_hex)
    except (TypeError, ValueError):
        return False

    expected = _pbkdf2_hash(password, salt, iterations)
    return hmac.compare_digest(expected, digest_hex)


def _verify_legacy_sha256(password: str, encoded_hash: str) -> bool:
    legacy = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return hmac.compare_digest(legacy, encoded_hash)


def verify_password(password: str, encoded_hash: str | None) -> Tuple[bool, bool]:
    if not encoded_hash:
        return False, False

    if encoded_hash.startswith(f"{PBKDF2_PREFIX}$"):
        return _verify_pbkdf2(password, encoded_hash), False

    return _verify_legacy_sha256(password, encoded_hash), True
