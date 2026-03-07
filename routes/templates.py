from flask import Blueprint, jsonify, request

from tmail_api.repositories import TemplateRepository

bp = Blueprint("templates", __name__)
repo = TemplateRepository()


@bp.route("/templates", methods=["GET"])
def list_templates():
    return jsonify({"items": repo.list()})


@bp.route("/templates", methods=["POST"])
def save_template():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(repo.save(payload))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@bp.route("/templates/<template_id>", methods=["GET"])
def get_template(template_id: str):
    template = repo.get(template_id)
    if not template:
        return jsonify({"error": "Template not found"}), 404
    return jsonify(template)
