# routes/stats.py

from flask import Blueprint, jsonify
import os
from collections import Counter
from datetime import datetime

bp = Blueprint("stats", __name__)

# ✅ Correct relative path to open_log.txt
LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "open_log.txt")

@bp.route("/stats", methods=["GET"])  # ✅ Correct route (exclude /api)
def get_stats():
    if not os.path.exists(LOG_PATH):
        return jsonify([])

    with open(LOG_PATH) as f:
        lines = [line.strip() for line in f if "Opened by" in line]

    total_opens = len(lines)
    parsed = []

    for line in lines:
        parts = line.split(" - Opened by: ")
        if len(parts) < 2:
            continue
        timestamp, rest = parts
        try:
            ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        user = rest.split(" from ")[0]
        parsed.append({"user": user, "timestamp": ts.isoformat()})

    user_counts = Counter([p["user"] for p in parsed])
    unique_ids = len(user_counts)
    most_active = user_counts.most_common(1)[0] if user_counts else ("—", 0)
    latest_open = max(parsed, key=lambda x: x["timestamp"], default=None)

    return jsonify({
        "total_opens": total_opens,
        "unique_ids": unique_ids,
        "most_active": {"user": most_active[0], "count": most_active[1]},
        "latest_open": latest_open,
        "top_users": [{"id": user, "count": count} for user, count in user_counts.most_common(10)],
        "log": parsed[-10:]
    })
