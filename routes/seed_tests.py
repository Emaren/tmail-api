from flask import Blueprint, jsonify, request

from tmail_api.repositories import SeedInboxRepository, SeedTestRepository
from tmail_api.services import SeedLabService

bp = Blueprint("seed_tests", __name__)
seed_inboxes = SeedInboxRepository()
runs = SeedTestRepository()
service = SeedLabService()


@bp.route("/seed-tests/inboxes", methods=["GET"])
def list_seed_inboxes():
    return jsonify({"items": seed_inboxes.list()})


@bp.route("/seed-tests/inboxes", methods=["POST"])
def save_seed_inbox():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(seed_inboxes.save(payload))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@bp.route("/seed-tests/runs", methods=["GET"])
def list_runs():
    limit = int(request.args.get("limit", 20))
    return jsonify({"items": runs.list_runs(limit=limit)})


@bp.route("/seed-tests/runs", methods=["POST"])
def launch_run():
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(service.launch_run(payload))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@bp.route("/seed-tests/runs/<run_id>", methods=["GET"])
def get_run(run_id: str):
    run = runs.get_run(run_id)
    if not run:
        return jsonify({"error": "Seed run not found"}), 404
    return jsonify(run)


@bp.route("/seed-tests/runs/<run_id>/results", methods=["POST"])
def save_results(run_id: str):
    payload = request.get_json(force=True) or {}
    try:
        return jsonify(service.record_results(run_id, payload))
    except ValueError as exc:
        status = 404 if str(exc) == "Seed run not found" else 400
        return jsonify({"error": str(exc)}), status
