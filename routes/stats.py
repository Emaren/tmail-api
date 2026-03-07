from flask import Blueprint, jsonify

from tmail_api.analytics import build_stats_payload

bp = Blueprint("stats", __name__)


@bp.route("/stats", methods=["GET"])
def get_stats():
    return jsonify(build_stats_payload())
