# tmail-api

Python API for the live TMail operator surface.

## Current Responsibilities

- operator auth and TOTP-backed admin access
- sender identity storage and Apple SMTP delivery
- message persistence, event timelines, tracked links, and soft-open tracking
- deliverability checks and preflight linting
- seed inbox run storage and scoring
- templates, contacts, analytics, and campaigns
- campaign scheduler status plus timed due-run execution

## Runtime

- app entrypoint: `track_server.py`
- scheduler entrypoint: `run_campaign_scheduler.py`

## Environment

- `HOST` defaults to `0.0.0.0`
- `PORT` defaults to `8010`
- `TMAIL_DB_PATH` overrides the SQLite database location
- `TMAIL_PUBLIC_BASE_URL` sets the tracking/click base URL
- `TMAIL_INTERNAL_API_TOKEN` protects internal admin API routes
- `TMAIL_CAMPAIGN_SCHEDULER_INTERVAL_MINUTES` describes the expected timer cadence for the scheduler status surface

## Current Production Shape

- Flask API behind Nginx
- systemd-managed services on the VPS
- Apple SMTP as the send rail
- public-safe health at `/healthz`
