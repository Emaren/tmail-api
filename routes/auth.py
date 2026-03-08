from flask import Blueprint, jsonify, request

from tmail_api.operator_auth import OperatorRepository

bp = Blueprint('auth_routes', __name__)
operators = OperatorRepository()


@bp.route('/auth/login', methods=['POST'])
def login():
    payload = request.get_json(force=True) or {}
    username = str(payload.get('username') or '').strip().lower()
    password = str(payload.get('password') or '')
    totp_code = str(payload.get('totp_code') or '').strip()
    if not username or not password:
        return jsonify({'error': 'Username and password are required.'}), 400
    try:
        operator = operators.authenticate(username, password, totp_code or None)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 401
    return jsonify({'operator': operator})


@bp.route('/auth/operators', methods=['GET'])
def list_operators():
    return jsonify({'items': operators.list()})


@bp.route('/auth/operators', methods=['POST'])
def create_operator():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(operators.create(payload))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400


@bp.route('/auth/operators/<operator_id>/password', methods=['POST'])
def update_password(operator_id: str):
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(
            operators.change_password(
                operator_id,
                str(payload.get('current_password') or ''),
                str(payload.get('new_password') or ''),
                totp_code=str(payload.get('totp_code') or '').strip() or None,
            )
        )
    except ValueError as exc:
        status = 404 if str(exc) == 'Operator not found.' else 400
        return jsonify({'error': str(exc)}), status


@bp.route('/auth/operators/<operator_id>/totp/setup', methods=['POST'])
def start_totp_setup(operator_id: str):
    try:
        return jsonify(operators.start_totp_setup(operator_id))
    except ValueError as exc:
        status = 404 if str(exc) == 'Operator not found.' else 400
        return jsonify({'error': str(exc)}), status


@bp.route('/auth/operators/<operator_id>/totp/enable', methods=['POST'])
def enable_totp(operator_id: str):
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(operators.enable_totp(operator_id, str(payload.get('code') or '').strip()))
    except ValueError as exc:
        status = 404 if str(exc) == 'Operator not found.' else 400
        return jsonify({'error': str(exc)}), status


@bp.route('/auth/operators/<operator_id>/totp/disable', methods=['POST'])
def disable_totp(operator_id: str):
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(
            operators.disable_totp(
                operator_id,
                str(payload.get('password') or ''),
                str(payload.get('totp_code') or '').strip(),
            )
        )
    except ValueError as exc:
        status = 404 if str(exc) == 'Operator not found.' else 400
        return jsonify({'error': str(exc)}), status
