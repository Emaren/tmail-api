from flask import Blueprint, jsonify, request

from tmail_api.repositories import ContactRepository

bp = Blueprint('contacts', __name__)
repo = ContactRepository()


@bp.route('/contacts', methods=['GET'])
def list_contacts():
    limit = int(request.args.get('limit', 120))
    return jsonify({'items': repo.list(limit=limit)})


@bp.route('/contacts', methods=['POST'])
def save_contact():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(repo.save(payload))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400


@bp.route('/contacts/<contact_id>', methods=['GET'])
def get_contact(contact_id: str):
    contact = repo.get(contact_id)
    if not contact:
        return jsonify({'error': 'Contact not found'}), 404
    return jsonify(contact)
