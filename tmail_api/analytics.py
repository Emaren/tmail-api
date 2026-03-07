from __future__ import annotations

import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_VPS_LOG = "/var/www/tmail-api/open_log.txt"


def resolve_log_path() -> Path:
    env_path = os.getenv("OPEN_LOG_PATH")
    if env_path:
        return Path(env_path)

    local_path = Path(__file__).resolve().parents[1] / "open_log.txt"
    if local_path.exists():
        return local_path

    return Path(DEFAULT_VPS_LOG)


def parse_log_line(line: str) -> dict[str, Any] | None:
    raw = line.strip()
    if not raw or " - Opened by: " not in raw:
        return None

    timestamp_raw, remainder = raw.split(" - Opened by: ", 1)

    try:
        timestamp = datetime.strptime(timestamp_raw, TIMESTAMP_FORMAT)
    except ValueError:
        return None

    user = remainder.split(" from ", 1)[0].strip()
    if not user:
        return None

    return {
        "user": user,
        "timestamp": timestamp,
        "timestamp_iso": timestamp.isoformat(),
    }


def load_events(log_path: Path | None = None) -> list[dict[str, Any]]:
    target = log_path or resolve_log_path()
    if not target.exists():
        return []

    events: list[dict[str, Any]] = []
    with target.open(encoding="utf-8") as handle:
        for line in handle:
            parsed = parse_log_line(line)
            if parsed:
                events.append(parsed)
    return events


def build_stats_payload(log_path: Path | None = None) -> dict[str, Any]:
    events = load_events(log_path)
    counts = Counter(event["user"] for event in events)
    latest = max(events, key=lambda event: event["timestamp"], default=None)
    per_day = Counter(event["timestamp"].strftime("%b %d") for event in events)

    return {
        "log_path": str(log_path or resolve_log_path()),
        "total_opens": len(events),
        "unique_ids": len(counts),
        "most_active": {
            "user": counts.most_common(1)[0][0],
            "count": counts.most_common(1)[0][1],
        }
        if counts
        else {"user": None, "count": 0},
        "latest_open": {
            "user": latest["user"],
            "timestamp": latest["timestamp_iso"],
        }
        if latest
        else None,
        "top_users": [
            {"id": user, "count": count}
            for user, count in counts.most_common(10)
        ],
        "log": [
            {"user": event["user"], "timestamp": event["timestamp_iso"]}
            for event in events[-10:]
        ],
        "opens_over_time": [
            {"date": day, "opens": total}
            for day, total in sorted(
                per_day.items(),
                key=lambda item: datetime.strptime(item[0], "%b %d"),
            )
        ],
    }
