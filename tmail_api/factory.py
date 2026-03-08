from flask import Flask, jsonify

from routes import analytics, auth, campaigns, contacts, dashboard, deliverability, health, identities, messages, seed_tests, segments, stats, templates, tracking
from tmail_api.auth import enforce_internal_api_auth
from tmail_api.db import init_db


def create_app() -> Flask:
    init_db()
    app = Flask(__name__)
    app.register_blueprint(stats.bp, url_prefix="/api")
    app.register_blueprint(health.bp, url_prefix="/api")
    app.register_blueprint(health.public_bp)
    app.register_blueprint(identities.bp, url_prefix="/api")
    app.register_blueprint(messages.bp, url_prefix="/api")
    app.register_blueprint(auth.bp, url_prefix="/api")
    app.register_blueprint(analytics.bp, url_prefix="/api")
    app.register_blueprint(campaigns.bp, url_prefix="/api")
    app.register_blueprint(contacts.bp, url_prefix="/api")
    app.register_blueprint(segments.bp, url_prefix="/api")
    app.register_blueprint(dashboard.bp, url_prefix="/api")
    app.register_blueprint(deliverability.bp, url_prefix="/api")
    app.register_blueprint(seed_tests.bp, url_prefix="/api")
    app.register_blueprint(templates.bp, url_prefix="/api")
    app.register_blueprint(tracking.api_bp, url_prefix="/api")
    app.register_blueprint(tracking.root_bp)

    @app.before_request
    def require_admin_api_token():
        return enforce_internal_api_auth()

    @app.route("/")
    def root():
        return jsonify({"msg": "TMail API is online."})

    return app
