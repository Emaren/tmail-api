from flask import Blueprint, jsonify, request

from tmail_api.repositories import IdentityRepository
from tmail_api.services import MailWorkflowService

bp = Blueprint("identities", __name__)
repo = IdentityRepository()
workflow = MailWorkflowService()


@bp.route("/identities", methods=["GET"])
def list_identities():
    return jsonify({"items": repo.list()})


@bp.route("/identities", methods=["POST"])
def save_identity():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(repo.save(payload))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@bp.route("/identities/<identity_id>", methods=["GET"])
def get_identity(identity_id: str):
    identity = repo.get(identity_id)
    if not identity:
        return jsonify({"error": "Identity not found"}), 404
    return jsonify(identity)


@bp.route("/identities/<identity_id>/test", methods=["POST"])
def test_identity(identity_id: str):
    try:
        return jsonify(workflow.test_identity_connection(identity_id))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
