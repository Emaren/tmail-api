from __future__ import annotations

import os

from flask import jsonify, request


PUBLIC_API_PREFIXES = ("/api/tracking/click/",)


def is_public_api_request(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in PUBLIC_API_PREFIXES)


def enforce_internal_api_auth() -> tuple[dict[str, str], int] | None:
    path = request.path or ""
    if not path.startswith("/api/"):
        return None
    if is_public_api_request(path):
        return None

    expected = os.getenv("TMAIL_INTERNAL_API_TOKEN", "").strip()
    if not expected:
        return jsonify({"error": "TMAIL_INTERNAL_API_TOKEN is not configured."}), 503

    actual = request.headers.get("Authorization", "")
    if actual == f"Bearer {expected}":
        return None

    return jsonify({"error": "Unauthorized"}), 401
