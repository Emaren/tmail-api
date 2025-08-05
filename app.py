# app.py

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import time

app = Flask(__name__)
CORS(app)

LOG_FILE = "open_log.txt"

@app.route("/")
def root():
    return { "msg": "TMail API is online." }

@app.route("/api/stats", methods=["GET"])
def stats():
    # Initialize stats
    total_opens = 0
    user_counts = {}
    latest_user = None
    latest_time = None
    raw_log = []

    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            lines = f.readlines()

        total_opens = len(lines)
        for line in lines:
            parts = line.strip().split(" - Opened by: ")
            if len(parts) == 2:
                timestamp, user_ip = parts
                user_id, *_ = user_ip.split(" from ")
                user_counts[user_id] = user_counts.get(user_id, 0) + 1
                latest_user = user_id
                latest_time = timestamp
                raw_log.append({ "user": user_id, "timestamp": timestamp })

    # Compute most active user
    most_active = max(user_counts.items(), key=lambda x: x[1], default=(None, 0))
    
    return jsonify({
        "total_opens": total_opens,
        "unique_ids": len(user_counts),
        "most_active": {
            "user": most_active[0],
            "count": most_active[1],
        },
        "latest_open": {
            "user": latest_user,
            "timestamp": latest_time,
        },
        "top_users": sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:10],
        "log": raw_log
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8009)
