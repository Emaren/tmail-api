from __future__ import annotations

import base64
import hashlib
import hmac
import os
import struct
import time
from urllib.parse import quote


PBKDF2_ITERATIONS = 260_000
TOTP_PERIOD_SECONDS = 30
TOTP_DIGITS = 6


def _b64_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode('ascii').rstrip('=')


def _b64_decode(value: str) -> bytes:
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode(f'{value}{padding}')


def hash_password(password: str, *, iterations: int = PBKDF2_ITERATIONS) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, iterations)
    return f'pbkdf2_sha256${iterations}${_b64_encode(salt)}${_b64_encode(digest)}'


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, iteration_text, salt_value, digest_value = encoded.split('$', 3)
        if algorithm != 'pbkdf2_sha256':
            return False
        iterations = int(iteration_text)
        salt = _b64_decode(salt_value)
        expected = _b64_decode(digest_value)
    except (TypeError, ValueError):
        return False

    actual = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, iterations)
    return hmac.compare_digest(actual, expected)


def generate_totp_secret() -> str:
    return base64.b32encode(os.urandom(20)).decode('ascii').rstrip('=')


def _normalize_base32(secret: str) -> bytes:
    cleaned = ''.join(secret.upper().split())
    padding = '=' * (-len(cleaned) % 8)
    return base64.b32decode(f'{cleaned}{padding}', casefold=True)


def totp_code(secret: str, *, timestamp: int | None = None, period: int = TOTP_PERIOD_SECONDS, digits: int = TOTP_DIGITS) -> str:
    key = _normalize_base32(secret)
    instant = int(timestamp if timestamp is not None else time.time())
    counter = instant // period
    counter_bytes = struct.pack('>Q', counter)
    digest = hmac.new(key, counter_bytes, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    truncated = struct.unpack('>I', digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(truncated % (10 ** digits)).zfill(digits)


def verify_totp(secret: str, code: str, *, window: int = 1, period: int = TOTP_PERIOD_SECONDS) -> bool:
    normalized = ''.join(ch for ch in code if ch.isdigit())
    if len(normalized) != TOTP_DIGITS:
        return False
    now = int(time.time())
    for delta in range(-window, window + 1):
        if hmac.compare_digest(totp_code(secret, timestamp=now + (delta * period), period=period), normalized):
            return True
    return False


def build_otpauth_uri(*, secret: str, username: str, issuer: str = 'TMail') -> str:
    label = quote(f'{issuer}:{username}')
    issuer_param = quote(issuer)
    return f'otpauth://totp/{label}?secret={secret}&issuer={issuer_param}&period={TOTP_PERIOD_SECONDS}&digits={TOTP_DIGITS}'
