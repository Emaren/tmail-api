# tmail-api

Python API for TMail tracking and analytics.

## Current Responsibilities

- serve tracking pixel requests at `/track`
- append open events to `open_log.txt`
- expose normalized analytics at `/api/stats`
- expose basic health at `/api/health`

## Environment

- `HOST` defaults to `0.0.0.0`
- `PORT` defaults to `8010`
- `OPEN_LOG_PATH` can override the log file location

## Next Direction

- sender identity persistence
- Apple SMTP provider abstraction
- message/event storage beyond raw log parsing
- click tracking and message-scoped instrumentation
- deliverability and seed-test endpoints
