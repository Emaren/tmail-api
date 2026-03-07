from __future__ import annotations

from flask import Blueprint, jsonify, request

from tmail_api.deliverability import DeliverabilityService
from tmail_api.preflight import analyze_preflight
from tmail_api.repositories import IdentityRepository

bp = Blueprint("deliverability", __name__)
diagnostics = DeliverabilityService()
identities = IdentityRepository()


def current_domains() -> list[str]:
    return [identity["email_address"].split("@", 1)[1] for identity in identities.list()]


@bp.route("/deliverability/domains", methods=["GET"])
def list_domains():
    return jsonify({"items": diagnostics.list_domains(current_domains())})


@bp.route("/deliverability/domains/<path:domain>", methods=["GET"])
def get_domain(domain: str):
    return jsonify(diagnostics.inspect_domain(domain))


@bp.route("/deliverability/domains/<path:domain>/refresh", methods=["POST"])
def refresh_domain(domain: str):
    return jsonify(diagnostics.inspect_domain(domain))


@bp.route("/deliverability/preflight", methods=["POST"])
def preflight():
    payload = request.get_json(force=True) or {}
    identity = None
    identity_id = payload.get("identity_id")
    if identity_id:
        identity = identities.get(str(identity_id))
    return jsonify(analyze_preflight(payload, identity=identity))
