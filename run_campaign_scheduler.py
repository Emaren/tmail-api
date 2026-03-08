from __future__ import annotations

import argparse
import json

from tmail_api.campaigns import CampaignRepository
from tmail_api.db import init_db


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the TMail campaign scheduler.")
    parser.add_argument("--limit", type=int, default=5, help="Maximum scheduled campaigns to execute in one run.")
    parser.add_argument(
        "--trigger",
        default="timer",
        choices=["manual", "timer"],
        help="How this scheduler run was triggered.",
    )
    args = parser.parse_args()

    init_db()
    result = CampaignRepository().run_scheduler(limit=max(args.limit, 1), trigger_type=args.trigger)
    print(
        json.dumps(
            {
                "run": result["run"],
                "status": result["status"],
                "item_count": len(result["items"]),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
