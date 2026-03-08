from flask import Blueprint, jsonify, request

from tmail_api.repositories import MessageRepository
from tmail_api.services import MailWorkflowService

bp = Blueprint("messages", __name__)
repo = MessageRepository()
workflow = MailWorkflowService()


@bp.route("/messages", methods=["GET"])
def list_messages():
    limit = int(request.args.get("limit", 20))
    return jsonify({"items": repo.list(limit=limit)})


@bp.route("/messages", methods=["POST"])
def save_or_send_message():
    payload = request.get_json(force=True) or {}
    try:
        result = workflow.save_or_send(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(result)


@bp.route("/messages/<message_id>", methods=["GET"])
def get_message(message_id: str):
    message = repo.get(message_id)
    if not message:
        return jsonify({"error": "Message not found"}), 404
    return jsonify(message)


@bp.route("/messages/<message_id>/outcome", methods=["POST"])
def record_message_outcome(message_id: str):
    payload = request.get_json(force=True) or {}
    try:
        result = repo.record_outcome(message_id, payload)
    except ValueError as exc:
        status = 404 if str(exc) == "Message not found" else 400
        return jsonify({"error": str(exc)}), status
    if not result:
        return jsonify({"error": "Message not found"}), 404
    return jsonify(result)
