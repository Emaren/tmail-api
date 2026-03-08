import sqlite3

from flask import Blueprint, jsonify, request

from tmail_api.campaigns import CampaignRepository

bp = Blueprint('campaigns', __name__)
repo = CampaignRepository()


@bp.route('/campaigns', methods=['GET'])
def list_campaigns():
    return jsonify({'items': repo.list()})


@bp.route('/campaigns', methods=['POST'])
def save_campaign():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(repo.save(payload))
    except (ValueError, sqlite3.IntegrityError) as exc:
        return jsonify({'error': str(exc)}), 400


@bp.route('/campaigns/<campaign_id>', methods=['GET'])
def get_campaign(campaign_id: str):
    campaign = repo.get(campaign_id)
    if not campaign:
        return jsonify({'error': 'Campaign not found'}), 404
    return jsonify(campaign)


@bp.route('/campaigns/<campaign_id>/launch', methods=['POST'])
def launch_campaign(campaign_id: str):
    try:
        return jsonify(repo.launch(campaign_id))
    except ValueError as exc:
        status = 404 if str(exc) == 'Campaign not found.' else 400
        return jsonify({'error': str(exc)}), status


@bp.route('/campaigns/run-due', methods=['POST'])
def run_due_campaigns():
    limit = int((request.get_json(silent=True) or {}).get('limit') or 5)
    return jsonify({'items': repo.run_due(limit=limit)})
