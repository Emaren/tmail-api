from flask import Blueprint, jsonify

from tmail_api.analytics import resolve_log_path

bp = Blueprint("health", __name__)


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
