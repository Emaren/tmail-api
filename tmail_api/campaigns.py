from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from tmail_api.db import get_connection, make_id, utc_now
from tmail_api.repositories import TemplateRepository
from tmail_api.services import MailWorkflowService


def parse_audience_emails(raw: str) -> list[str]:
    normalized: list[str] = []
    for part in raw.replace("\r", "\n").replace(",", "\n").split("\n"):
        email = part.strip().lower()
        if email and email not in normalized:
            normalized.append(email)
    return normalized


def parse_scheduled_at(raw: str | None) -> datetime | None:
    if not raw:
        return None
    value = raw.strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class CampaignRepository:
    VALID_STATUSES = {"draft", "ready", "scheduled", "live", "paused", "completed"}

    def __init__(self) -> None:
        self.templates = TemplateRepository()
        self.mail = MailWorkflowService()

    def list(self) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT campaigns.*, identities.email_address AS identity_email,
                       identities.label AS identity_label,
                       templates.name AS template_name
                FROM campaigns
                JOIN identities ON identities.id = campaigns.identity_id
                LEFT JOIN templates ON templates.id = campaigns.template_id
                ORDER BY campaigns.updated_at DESC, campaigns.created_at DESC
                """
            ).fetchall()
            return [self._row_to_dict(row, conn=conn) for row in rows]

    def get(self, campaign_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT campaigns.*, identities.email_address AS identity_email,
                       identities.label AS identity_label,
                       templates.name AS template_name
                FROM campaigns
                JOIN identities ON identities.id = campaigns.identity_id
                LEFT JOIN templates ON templates.id = campaigns.template_id
                WHERE campaigns.id = ?
                """,
                (campaign_id,),
            ).fetchone()
            return self._row_to_dict(row, conn=conn) if row else None

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name") or "").strip()
        objective = str(payload.get("objective") or "").strip()
        identity_id = str(payload.get("identity_id") or "").strip()
        audience_label = str(payload.get("audience_label") or "").strip()
        status = str(payload.get("status") or "draft").strip().lower()
        if not name or not objective or not identity_id or not audience_label:
            raise ValueError("Name, objective, identity, and audience are required.")
        if status not in self.VALID_STATUSES:
            raise ValueError("Invalid campaign status.")

        campaign_id = str(payload.get("id") or make_id("campaign"))
        now = utc_now()
        existing = self.get(campaign_id) if payload.get("id") else None
        audience_emails = "\n".join(parse_audience_emails(str(payload.get("audience_emails") or "")))
        scheduled_for = parse_scheduled_at(str(payload.get("scheduled_for") or "").strip() or None)
        row = {
            "id": campaign_id,
            "name": name,
            "objective": objective,
            "status": status,
            "identity_id": identity_id,
            "template_id": str(payload.get("template_id") or "").strip() or None,
            "audience_label": audience_label,
            "audience_emails": audience_emails,
            "send_window": str(payload.get("send_window") or "").strip(),
            "notes": str(payload.get("notes") or "").strip(),
            "scheduled_for": scheduled_for.isoformat() if scheduled_for else None,
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
        }
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO campaigns (
                    id, name, objective, status, identity_id, template_id,
                    audience_label, audience_emails, send_window, notes, scheduled_for,
                    created_at, updated_at
                ) VALUES (
                    :id, :name, :objective, :status, :identity_id, :template_id,
                    :audience_label, :audience_emails, :send_window, :notes, :scheduled_for,
                    :created_at, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    objective = excluded.objective,
                    status = excluded.status,
                    identity_id = excluded.identity_id,
                    template_id = excluded.template_id,
                    audience_label = excluded.audience_label,
                    audience_emails = excluded.audience_emails,
                    send_window = excluded.send_window,
                    notes = excluded.notes,
                    scheduled_for = excluded.scheduled_for,
                    updated_at = excluded.updated_at
                """,
                row,
            )
        return self.get(campaign_id)  # type: ignore[return-value]

    def launch(self, campaign_id: str, *, trigger_type: str = "manual") -> dict[str, Any]:
        campaign = self.get(campaign_id)
        if not campaign:
            raise ValueError("Campaign not found.")
        if not campaign.get("template_id"):
            raise ValueError("Link a template before launching the campaign.")

        recipients = parse_audience_emails(str(campaign.get("audience_emails") or ""))
        if not recipients:
            raise ValueError("Add at least one audience email before launching the campaign.")

        template = self.templates.get(str(campaign["template_id"]))
        if not template:
            raise ValueError("Linked template not found.")

        started_at = utc_now()
        run_id = make_id("camprun")
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO campaign_runs (
                    id, campaign_id, message_id, mode, trigger_type, status,
                    recipient_count, sent_count, summary, started_at, completed_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    campaign_id,
                    None,
                    "live",
                    trigger_type,
                    "running",
                    len(recipients),
                    0,
                    "Campaign execution started.",
                    started_at,
                    None,
                    started_at,
                    started_at,
                ),
            )

        sent_count = 0
        first_message_id: str | None = None
        failures: list[str] = []

        for recipient in recipients:
            result = self.mail.save_or_send(
                {
                    "identity_id": campaign["identity_id"],
                    "template_id": campaign.get("template_id"),
                    "campaign_id": campaign_id,
                    "recipients": [recipient],
                    "subject": template["subject"],
                    "preheader": template.get("preheader", ""),
                    "html_body": template["html_body"],
                    "text_body": template["text_body"],
                    "tracking_enabled": True,
                    "pixel_enabled": True,
                    "action": "send_live",
                }
            )
            if not first_message_id:
                first_message_id = result.get("id")
            if result.get("status") == "Sent":
                sent_count += 1
            else:
                failures.append(f"{recipient}: {result.get('error_message') or result.get('status') or 'Needs review'}")

        completed_at = utc_now()
        run_status = "sent" if sent_count == len(recipients) else "partial" if sent_count else "needs_review"
        summary = (
            f"Delivered {sent_count} of {len(recipients)} audience sends."
            if not failures
            else f"Delivered {sent_count} of {len(recipients)} audience sends. Review failures."
        )
        next_campaign_status = "completed" if run_status == "sent" else "ready"

        with get_connection() as conn:
            conn.execute(
                """
                UPDATE campaign_runs
                SET message_id = ?,
                    status = ?,
                    sent_count = ?,
                    summary = ?,
                    completed_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    first_message_id,
                    run_status,
                    sent_count,
                    summary if not failures else f"{summary} {' | '.join(failures[:3])}",
                    completed_at,
                    completed_at,
                    run_id,
                ),
            )
            conn.execute(
                """
                UPDATE campaigns
                SET status = ?,
                    scheduled_for = CASE WHEN status = 'scheduled' THEN NULL ELSE scheduled_for END,
                    updated_at = ?
                WHERE id = ?
                """,
                (next_campaign_status, completed_at, campaign_id),
            )

        updated_campaign = self.get(campaign_id)
        run = self.get_run(run_id)
        return {
            "campaign": updated_campaign,
            "run": run,
        }

    def run_due(self, limit: int = 5) -> list[dict[str, Any]]:
        return self.run_scheduler(limit=limit, trigger_type="manual")["items"]

    def run_scheduler(self, limit: int = 5, *, trigger_type: str = "manual") -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        started_at = utc_now()
        scheduler_run_id = make_id("scheduler")
        items: list[dict[str, Any]] = []
        failures: list[str] = []

        with get_connection() as conn:
            due_rows = self._due_rows(conn, now=now, limit=limit)
            conn.execute(
                """
                INSERT INTO scheduler_runs (
                    id, scope, trigger_type, status, due_count, launched_count,
                    failed_count, summary, campaign_ids_json, run_ids_json,
                    started_at, completed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scheduler_run_id,
                    "campaigns",
                    trigger_type,
                    "running",
                    len(due_rows),
                    0,
                    0,
                    "Scheduler run started.",
                    "[]",
                    "[]",
                    started_at,
                    None,
                    started_at,
                    started_at,
                ),
            )

        for row in due_rows:
            try:
                items.append(self.launch(row["id"], trigger_type="scheduled"))
            except ValueError as exc:
                failures.append(f"{row['id']}: {exc}")
                self._release_scheduled_campaign(row["id"])
                items.append(
                    {
                        "campaign": self.get(row["id"]),
                        "run": None,
                        "error": str(exc),
                    }
                )

        launched_count = sum(1 for item in items if item.get("run"))
        failed_count = len(items) - launched_count
        review_count = sum(
            1
            for item in items
            if item.get("run") and item["run"].get("status") in {"partial", "needs_review"}
        )
        if not items:
            status = "idle"
            summary = "No scheduled campaigns were due."
        elif failed_count or review_count:
            status = "needs_review"
            summary = f"Scheduler touched {len(items)} campaign(s). Review {failed_count + review_count} campaign outcome(s)."
        else:
            status = "completed"
            summary = f"Scheduler executed {len(items)} scheduled campaign(s)."

        completed_at = utc_now()
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE scheduler_runs
                SET status = ?,
                    launched_count = ?,
                    failed_count = ?,
                    summary = ?,
                    campaign_ids_json = ?,
                    run_ids_json = ?,
                    completed_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    launched_count,
                    failed_count,
                    summary if not failures else f"{summary} {' | '.join(failures[:3])}",
                    json.dumps([item["campaign"]["id"] for item in items if item.get("campaign")], separators=(",", ":")),
                    json.dumps([item["run"]["id"] for item in items if item.get("run")], separators=(",", ":")),
                    completed_at,
                    completed_at,
                    scheduler_run_id,
                ),
            )

        return {
            "run": self.get_scheduler_run(scheduler_run_id),
            "items": items,
            "status": self.get_scheduler_status(),
        }

    def get_scheduler_status(self, recent_limit: int = 4) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        with get_connection() as conn:
            due_rows = self._due_rows(conn, now=now, limit=None)
            next_scheduled = None
            future_rows = self._scheduled_rows(conn)
            for row in future_rows:
                scheduled = parse_scheduled_at(row["scheduled_for"])
                if scheduled and scheduled > now:
                    next_scheduled = scheduled.isoformat()
                    break

            recent_rows = conn.execute(
                """
                SELECT * FROM scheduler_runs
                WHERE scope = 'campaigns'
                ORDER BY started_at DESC, created_at DESC
                LIMIT ?
                """,
                (recent_limit,),
            ).fetchall()
            total_scheduled = conn.execute(
                "SELECT COUNT(*) AS total FROM campaigns WHERE status = 'scheduled'"
            ).fetchone()["total"] or 0

        recent_runs = [self._scheduler_run_to_dict(row) for row in recent_rows]
        return {
            "interval_minutes": max(int(os.getenv("TMAIL_CAMPAIGN_SCHEDULER_INTERVAL_MINUTES", "5") or "5"), 1),
            "scheduled_count": total_scheduled,
            "due_count": len(due_rows),
            "next_scheduled_for": next_scheduled,
            "last_run": recent_runs[0] if recent_runs else None,
            "recent_runs": recent_runs,
        }

    def get_scheduler_run(self, scheduler_run_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM scheduler_runs
                WHERE id = ?
                """,
                (scheduler_run_id,),
            ).fetchone()
            return self._scheduler_run_to_dict(row) if row else None

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM campaign_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            return self._run_to_dict(row) if row else None

    def _recent_runs(self, conn: Any, campaign_id: str, limit: int = 4) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT * FROM campaign_runs
            WHERE campaign_id = ?
            ORDER BY started_at DESC, created_at DESC
            LIMIT ?
            """,
            (campaign_id, limit),
        ).fetchall()
        return [self._run_to_dict(row) for row in rows]

    def _scheduled_rows(self, conn: Any) -> list[Any]:
        return conn.execute(
            """
            SELECT id, scheduled_for
            FROM campaigns
            WHERE status = 'scheduled' AND scheduled_for IS NOT NULL AND scheduled_for != ''
            ORDER BY scheduled_for ASC, updated_at ASC
            """
        ).fetchall()

    def _due_rows(self, conn: Any, *, now: datetime, limit: int | None) -> list[Any]:
        due_rows: list[Any] = []
        for row in self._scheduled_rows(conn):
            scheduled = parse_scheduled_at(row["scheduled_for"])
            if scheduled and scheduled <= now:
                due_rows.append(row)
                if limit and len(due_rows) >= limit:
                    break
        return due_rows

    def _release_scheduled_campaign(self, campaign_id: str) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE campaigns
                SET status = 'ready',
                    updated_at = ?
                WHERE id = ?
                """,
                (utc_now(), campaign_id),
            )

    def _run_to_dict(self, row: Any) -> dict[str, Any]:
        return {
            "id": row["id"],
            "campaign_id": row["campaign_id"],
            "message_id": row["message_id"],
            "mode": row["mode"],
            "trigger_type": row["trigger_type"],
            "status": row["status"],
            "recipient_count": row["recipient_count"],
            "sent_count": row["sent_count"],
            "summary": row["summary"] or "",
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _scheduler_run_to_dict(self, row: Any) -> dict[str, Any]:
        return {
            "id": row["id"],
            "scope": row["scope"],
            "trigger_type": row["trigger_type"],
            "status": row["status"],
            "due_count": row["due_count"],
            "launched_count": row["launched_count"],
            "failed_count": row["failed_count"],
            "summary": row["summary"] or "",
            "campaign_ids": json.loads(row["campaign_ids_json"] or "[]"),
            "run_ids": json.loads(row["run_ids_json"] or "[]"),
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _row_to_dict(self, row: Any, *, conn: Any) -> dict[str, Any]:
        counts = conn.execute(
            """
            SELECT
                COUNT(*) AS total_messages,
                SUM(CASE WHEN status = 'Sent' THEN 1 ELSE 0 END) AS sent_messages
            FROM messages
            WHERE campaign_id = ?
            """,
            (row["id"],),
        ).fetchone()
        event_counts = conn.execute(
            """
            SELECT events.event_type, COUNT(*) AS total
            FROM events
            JOIN messages ON messages.id = events.message_id
            WHERE messages.campaign_id = ?
            GROUP BY events.event_type
            """,
            (row["id"],),
        ).fetchall()
        counts_by_type = {item["event_type"]: item["total"] for item in event_counts}
        recent_runs = self._recent_runs(conn, row["id"])
        audience_emails = row["audience_emails"] or ""
        return {
            "id": row["id"],
            "name": row["name"],
            "objective": row["objective"],
            "status": row["status"],
            "identity_id": row["identity_id"],
            "identity": row["identity_email"],
            "identity_label": row["identity_label"],
            "template_id": row["template_id"],
            "template_name": row["template_name"],
            "audience_label": row["audience_label"],
            "audience_emails": audience_emails,
            "audience_count": len(parse_audience_emails(audience_emails)),
            "send_window": row["send_window"] or "",
            "notes": row["notes"] or "",
            "scheduled_for": row["scheduled_for"],
            "message_count": counts["total_messages"] or 0,
            "sent_count": counts["sent_messages"] or 0,
            "open_events": counts_by_type.get("opened", 0),
            "click_events": counts_by_type.get("clicked", 0),
            "reply_events": counts_by_type.get("replied", 0) + counts_by_type.get("replied_manual", 0),
            "conversion_events": counts_by_type.get("meeting_booked", 0) + counts_by_type.get("converted", 0),
            "last_run": recent_runs[0] if recent_runs else None,
            "recent_runs": recent_runs,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
