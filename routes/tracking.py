from __future__ import annotations

import time
from pathlib import Path

from flask import Blueprint, jsonify, make_response, redirect, request, send_file

from tmail_api.analytics import resolve_log_path
from tmail_api.repositories import MessageRepository

api_bp = Blueprint("tracking_api", __name__)
root_bp = Blueprint("tracking_root", __name__)
messages = MessageRepository()
LOG_PATH = resolve_log_path()


def log(label: str, value: str, icon: str = "[log]") -> None:
    print(f"{icon} {label:20}: {value}")


def banner(title: str) -> None:
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


@root_bp.before_app_request
def log_request() -> None:
    qs = request.query_string.decode("utf-8")
    banner("Incoming Request")
    log("From IP", request.remote_addr or "unknown", "[ip]")
    log("Method", request.method)
    log("Path", request.path)
    log("Query", qs or "(none)")


@root_bp.route("/track", methods=["GET", "HEAD"])
def track():
    legacy_id = request.args.get("id")
    message_id = request.args.get("message_id")
    ts = request.args.get("ts", "")
    remote = request.remote_addr or "unknown"
    now = time.strftime("%Y-%m-%d %H:%M:%S")

    tracked_label = legacy_id or message_id or "unknown"
    entry = f"{now} - Opened by: {tracked_label} from {remote}"
    if ts:
        entry += f" · ts={ts}"

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(entry + "\n")

    if message_id:
        messages.add_event(
            message_id,
            "opened",
            {"remote_ip": remote, "legacy_id": legacy_id, "query_timestamp": ts or None},
        )

    gif_path = Path(__file__).resolve().parents[1] / "pixel.gif"
    response = make_response(send_file(gif_path, mimetype="image/gif"))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@api_bp.route("/tracking/click/<token>", methods=["GET"])
def click(token: str):
    link = messages.get_tracked_link(token)
    if not link:
        return jsonify({"error": "Tracked link not found"}), 404

    messages.add_event(
        link["message_id"],
        "clicked",
        {"token": token, "url": link["url"], "remote_ip": request.remote_addr},
    )
    return redirect(link["url"], code=302)
