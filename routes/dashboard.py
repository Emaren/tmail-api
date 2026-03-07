from flask import Blueprint, jsonify

from tmail_api.analytics import build_stats_payload
from tmail_api.deliverability import DeliverabilityService
from tmail_api.repositories import IdentityRepository, MessageRepository, SeedTestRepository

bp = Blueprint("dashboard", __name__)
identities = IdentityRepository()
messages = MessageRepository()
diagnostics = DeliverabilityService()
seed_runs = SeedTestRepository()


def build_domain_summary() -> list[dict[str, str]]:
    domains = [identity["email_address"].split("@", 1)[1] for identity in identities.list()]
    return diagnostics.list_domains(domains)


def build_alerts(domains: list[dict[str, str]]) -> list[dict[str, str]]:
    warnings = [domain for domain in domains if "fail" in {domain["spf"], domain["dkim"], domain["dmarc"], domain["mx"]}]
    alerts = [
        {
            "id": "api-storage",
            "level": "info",
            "title": "SQLite phase active",
            "body": "Phase 1 persistence is local-first. Postgres remains the next storage milestone.",
        },
        {
            "id": "smtp-env",
            "level": "warning",
            "title": "Live Apple SMTP depends on env-backed secrets",
            "body": "Configure the identity secret env vars before running real sends or connection tests.",
        },
    ]
    if warnings:
        alerts.append(
            {
                "id": "deliverability-gaps",
                "level": "warning",
                "title": "Authentication gaps detected",
                "body": ", ".join(domain["domain"] for domain in warnings) + " needs DNS/auth review before production sends.",
            }
        )
    return alerts


@bp.route("/dashboard/summary", methods=["GET"])
def summary():
    stats = build_stats_payload()
    domains = build_domain_summary()
    return jsonify(
        {
            "overview": stats,
            "messages": messages.list(limit=6),
            "identities": identities.list(),
            "domains": domains,
            "alerts": build_alerts(domains),
            "seed_preview": seed_runs.latest_preview(),
        }
    )
