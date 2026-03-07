from datetime import datetime, timezone

from flask import Blueprint, jsonify

from tmail_api.analytics import resolve_log_path

bp = Blueprint("health", __name__)
public_bp = Blueprint("public_health", __name__)


@bp.route("/health", methods=["GET"])
def health():
    log_path = resolve_log_path()
    return jsonify(
        {
            "status": "ok",
            "log_path": str(log_path),
            "log_exists": log_path.exists(),
        }
    )


@public_bp.route("/healthz", methods=["GET"])
def public_health():
    return jsonify(
        {
            "status": "ok",
            "service": "tmail-api",
            "time": datetime.now(timezone.utc).isoformat(),
        }
    )
