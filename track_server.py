# track_server.py

import os
import time
from flask import Flask, request, send_file, make_response
from urllib.parse import unquote_plus

app = Flask(__name__)

# === COLORIZED LOGGING ===
def log(label, value, icon="🔍"):
    print(f"{icon} \033[95m{label:20}\033[0m: {value}")

def banner(title):
    print(f"\n\033[96m{'='*60}")
    print(f"🎯 {title}")
    print(f"{'='*60}\033[0m")

# === MAIN TRACKING ROUTE ===
@app.before_request
def log_request():
    qs = request.query_string.decode("utf-8")
    method = request.method
    path   = request.path
    remote = request.remote_addr

    banner("Incoming Pixel Request")
    log("From IP", remote, "🌐")
    log("Method", method)
    log("Path", path)
    log("Query", qs or "(none)")

@app.route("/track", methods=["GET", "HEAD"])
def track():
    # Parse query
    user_id = request.args.get("id", "unknown")
    ts      = request.args.get("ts", "")
    remote  = request.remote_addr
    now     = time.strftime("%Y-%m-%d %H:%M:%S")

    # Build log entry
    entry = f"{now} - Opened by: {user_id} from {remote}"
    if ts:
        entry += f" · ts={ts}"

    # Verbose logging
    banner("🎯 Pixel Tracked")
    log("User ID", user_id, "🆔")
    log("Remote IP", remote)
    log("Timestamp", now)
    if ts:
        log("Query Timestamp", ts)

    # Save to open_log.txt
    log_path = os.path.join(os.path.dirname(__file__), "open_log.txt")
    with open(log_path, "a") as f:
        f.write(entry + "\n")
    log("Logged Entry", log_path, "📝")

    # Serve tracking GIF
    gif_path = os.path.join(os.path.dirname(__file__), "pixel.gif")
    resp = make_response(send_file(gif_path, mimetype="image/gif"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"]        = "no-cache"
    resp.headers["Expires"]       = "0"

    log("Served Pixel", gif_path, "🖼️")
    return resp

# === BOOT SERVER ===
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8010))
    host = os.getenv("HOST", "0.0.0.0")
    banner("🚀 Starting Tracking Server")
    print(f"Listening on \033[93mhttp://{host}:{port}/track\033[0m for tracking pixels...\n")
    app.run(host=host, port=port)
