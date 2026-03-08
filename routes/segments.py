import sqlite3

from flask import Blueprint, jsonify, request

from tmail_api.repositories import SegmentRepository

bp = Blueprint('segments', __name__)
repo = SegmentRepository()


@bp.route('/segments', methods=['GET'])
def list_segments():
    limit = int(request.args.get('limit', 80))
    return jsonify({'items': repo.list(limit=limit)})


@bp.route('/segments', methods=['POST'])
def save_segment():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(repo.save(payload))
    except (ValueError, sqlite3.IntegrityError) as exc:
        return jsonify({'error': str(exc)}), 400


@bp.route('/segments/<segment_id>', methods=['GET'])
def get_segment(segment_id: str):
    segment = repo.get(segment_id)
    if not segment:
        return jsonify({'error': 'Segment not found'}), 404
    return jsonify(segment)
