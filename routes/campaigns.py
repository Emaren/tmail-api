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
