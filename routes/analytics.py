from flask import Blueprint, jsonify

from tmail_api.insights import build_analytics_summary

bp = Blueprint('analytics', __name__)


@bp.route('/analytics/summary', methods=['GET'])
def summary():
    return jsonify(build_analytics_summary())
