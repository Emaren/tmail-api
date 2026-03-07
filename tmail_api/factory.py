from flask import Flask, jsonify
from flask_cors import CORS

from routes import dashboard, deliverability, health, identities, messages, stats, tracking
from tmail_api.db import init_db


def create_app() -> Flask:
    init_db()
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(stats.bp, url_prefix="/api")
    app.register_blueprint(health.bp, url_prefix="/api")
    app.register_blueprint(identities.bp, url_prefix="/api")
    app.register_blueprint(messages.bp, url_prefix="/api")
    app.register_blueprint(dashboard.bp, url_prefix="/api")
    app.register_blueprint(deliverability.bp, url_prefix="/api")
    app.register_blueprint(tracking.api_bp, url_prefix="/api")
    app.register_blueprint(tracking.root_bp)

    @app.route("/")
    def root():
        return jsonify({"msg": "TMail API is online."})

    return app
