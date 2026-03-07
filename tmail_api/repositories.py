from __future__ import annotations

import json
import os
import re
import sqlite3
from typing import Any

from tmail_api.db import get_connection, make_id, utc_now


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or make_id("tpl")


class IdentityRepository:
    def list(self) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute("SELECT * FROM identities ORDER BY created_at ASC").fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get(self, identity_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute("SELECT * FROM identities WHERE id = ?", (identity_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        missing = [field for field in ("label", "display_name", "email_address") if not payload.get(field)]
        if missing:
            raise ValueError(f"Missing required identity fields: {', '.join(missing)}")

        existing = self.get(payload.get("id", "")) if payload.get("id") else None
        identity_id = payload.get("id") or make_id("identity")
        now = utc_now()
        row = {
            "id": identity_id,
            "label": payload["label"],
            "display_name": payload["display_name"],
            "email_address": payload["email_address"],
            "provider_type": payload.get("provider_type", "apple_smtp"),
            "smtp_host": payload.get("smtp_host", "smtp.mail.me.com"),
            "smtp_port": int(payload.get("smtp_port", 587)),
            "smtp_username": payload.get("smtp_username", payload["email_address"]),
            "smtp_secret_env": payload.get("smtp_secret_env", ""),
            "use_tls": 1 if payload.get("use_tls", True) else 0,
            "reply_to": payload.get("reply_to") or payload["email_address"],
            "tracking_enabled": 1 if payload.get("tracking_enabled", True) else 0,
            "pixel_enabled": 1 if payload.get("pixel_enabled", True) else 0,
            "status": payload.get("status", "attention"),
            "notes": payload.get("notes", ""),
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
        }

        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO identities (
                    id, label, display_name, email_address, provider_type,
                    smtp_host, smtp_port, smtp_username, smtp_secret_env,
                    use_tls, reply_to, tracking_enabled, pixel_enabled,
                    status, notes, created_at, updated_at
                ) VALUES (
                    :id, :label, :display_name, :email_address, :provider_type,
                    :smtp_host, :smtp_port, :smtp_username, :smtp_secret_env,
                    :use_tls, :reply_to, :tracking_enabled, :pixel_enabled,
                    :status, :notes, :created_at, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    label = excluded.label,
                    display_name = excluded.display_name,
                    email_address = excluded.email_address,
                    provider_type = excluded.provider_type,
                    smtp_host = excluded.smtp_host,
                    smtp_port = excluded.smtp_port,
                    smtp_username = excluded.smtp_username,
                    smtp_secret_env = excluded.smtp_secret_env,
                    use_tls = excluded.use_tls,
                    reply_to = excluded.reply_to,
                    tracking_enabled = excluded.tracking_enabled,
                    pixel_enabled = excluded.pixel_enabled,
                    status = excluded.status,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                row,
            )
        return self.get(identity_id)  # type: ignore[return-value]

    def connection_health(self, identity: dict[str, Any]) -> dict[str, Any]:
        secret_present = bool(os.getenv(identity["smtp_secret_env"]))
        return {
            "secretConfigured": secret_present,
            "status": "healthy" if secret_present else "attention",
        }

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        base = dict(row)
        health = self.connection_health(base)
        return {
            **base,
            "use_tls": bool(base["use_tls"]),
            "tracking_enabled": bool(base["tracking_enabled"]),
            "pixel_enabled": bool(base["pixel_enabled"]),
            "health": health,
        }


class MessageRepository:
    def list(self, limit: int = 20) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT messages.*, identities.email_address AS identity_email, identities.label AS identity_label
                FROM messages
                JOIN identities ON identities.id = messages.identity_id
                ORDER BY COALESCE(messages.sent_at, messages.created_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [self._row_to_summary(row, conn=conn) for row in rows]

    def get(self, message_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT messages.*, identities.email_address AS identity_email, identities.label AS identity_label
                FROM messages
                JOIN identities ON identities.id = messages.identity_id
                WHERE messages.id = ?
                """,
                (message_id,),
            ).fetchone()
            if not row:
                return None
            events = conn.execute(
                "SELECT * FROM events WHERE message_id = ? ORDER BY occurred_at ASC",
                (message_id,),
            ).fetchall()
            links = conn.execute(
                "SELECT token, url, label, created_at FROM tracked_links WHERE message_id = ? ORDER BY created_at ASC",
                (message_id,),
            ).fetchall()
            detail = self._row_to_summary(row, conn=conn)
        detail["html_body"] = row["html_body"]
        detail["text_body"] = row["text_body"]
        detail["preheader"] = row["preheader"]
        detail["events"] = [
            {
                "id": event["id"],
                "type": event["event_type"],
                "occurred_at": event["occurred_at"],
                "payload": json.loads(event["payload_json"]),
            }
            for event in events
        ]
        detail["tracked_links"] = [dict(link) for link in links]
        return detail

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        message_id = make_id("msg")
        now = utc_now()
        row = {
            "id": message_id,
            "identity_id": payload["identity_id"],
            "subject": payload["subject"],
            "preheader": payload.get("preheader", ""),
            "html_body": payload["html_body"],
            "text_body": payload["text_body"],
            "recipients_json": json.dumps(payload.get("recipients", [])),
            "status": payload["status"],
            "send_mode": payload.get("send_mode", "draft"),
            "tracking_enabled": 1 if payload.get("tracking_enabled", True) else 0,
            "pixel_enabled": 1 if payload.get("pixel_enabled", True) else 0,
            "preview": payload.get("preview", ""),
            "provider_message_id": payload.get("provider_message_id"),
            "error_message": payload.get("error_message"),
            "created_at": now,
            "updated_at": now,
            "sent_at": payload.get("sent_at"),
        }
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO messages (
                    id, identity_id, subject, preheader, html_body, text_body,
                    recipients_json, status, send_mode, tracking_enabled, pixel_enabled,
                    preview, provider_message_id, error_message, created_at, updated_at, sent_at
                ) VALUES (
                    :id, :identity_id, :subject, :preheader, :html_body, :text_body,
                    :recipients_json, :status, :send_mode, :tracking_enabled, :pixel_enabled,
                    :preview, :provider_message_id, :error_message, :created_at, :updated_at, :sent_at
                )
                """,
                row,
            )
        return self.get(message_id)  # type: ignore[return-value]

    def update_status(
        self,
        message_id: str,
        *,
        status: str,
        sent_at: str | None = None,
        error_message: str | None = None,
        provider_message_id: str | None = None,
    ) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE messages
                SET status = ?, sent_at = COALESCE(?, sent_at), error_message = ?, provider_message_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, sent_at, error_message, provider_message_id, utc_now(), message_id),
            )

    def update_content(self, message_id: str, *, html_body: str, text_body: str, preview: str) -> None:
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE messages
                SET html_body = ?, text_body = ?, preview = ?, updated_at = ?
                WHERE id = ?
                """,
                (html_body, text_body, preview, utc_now(), message_id),
            )

    def add_event(self, message_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        event_id = make_id("evt")
        occurred_at = utc_now()
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO events (id, message_id, event_type, occurred_at, payload_json) VALUES (?, ?, ?, ?, ?)",
                (event_id, message_id, event_type, occurred_at, json.dumps(payload)),
            )
        return {
            "id": event_id,
            "message_id": message_id,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "payload": payload,
        }

    def create_tracked_link(self, message_id: str, url: str, label: str | None = None) -> str:
        token = make_id("lnk")
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO tracked_links (token, message_id, url, label, created_at) VALUES (?, ?, ?, ?, ?)",
                (token, message_id, url, label, utc_now()),
            )
        return token

    def get_tracked_link(self, token: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute("SELECT * FROM tracked_links WHERE token = ?", (token,)).fetchone()
        return dict(row) if row else None

    def _event_counts(self, conn: sqlite3.Connection, message_id: str) -> dict[str, int]:
        rows = conn.execute(
            """
            SELECT event_type, COUNT(*) AS total
            FROM events
            WHERE message_id = ?
            GROUP BY event_type
            """,
            (message_id,),
        ).fetchall()
        counts = {row["event_type"]: row["total"] for row in rows}
        return {
            "opens": counts.get("opened", 0),
            "clicks": counts.get("clicked", 0),
            "replies": counts.get("replied", 0) + counts.get("replied_manual", 0),
        }

    def _row_to_summary(self, row: sqlite3.Row, *, conn: sqlite3.Connection) -> dict[str, Any]:
        recipients = json.loads(row["recipients_json"])
        counts = self._event_counts(conn, row["id"])
        return {
            "id": row["id"],
            "identity_id": row["identity_id"],
            "identity": row["identity_email"],
            "identity_label": row["identity_label"],
            "subject": row["subject"],
            "preview": row["preview"],
            "status": row["status"],
            "send_mode": row["send_mode"],
            "recipients": recipients,
            "recipient_count": len(recipients),
            "opens": counts["opens"],
            "clicks": counts["clicks"],
            "replies": counts["replies"],
            "tracking_enabled": bool(row["tracking_enabled"]),
            "pixel_enabled": bool(row["pixel_enabled"]),
            "provider_message_id": row["provider_message_id"],
            "error_message": row["error_message"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "sent_at": row["sent_at"],
        }


class TemplateRepository:
    def list(self) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute("SELECT * FROM templates ORDER BY updated_at DESC, name ASC").fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get(self, template_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute("SELECT * FROM templates WHERE id = ?", (template_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        missing = [field for field in ("name", "subject", "html_body", "text_body") if not str(payload.get(field, "")).strip()]
        if missing:
            raise ValueError(f"Missing required template fields: {', '.join(missing)}")

        existing = self.get(str(payload.get("id", ""))) if payload.get("id") else None
        template_id = str(payload.get("id") or make_id("tpl"))
        now = utc_now()
        name = str(payload["name"]).strip()

        with get_connection() as conn:
            slug = self._unique_slug(conn, str(payload.get("slug") or slugify(name)), template_id)
            row = {
                "id": template_id,
                "name": name,
                "slug": slug,
                "category": str(payload.get("category") or "General").strip(),
                "description": str(payload.get("description") or "").strip(),
                "subject": str(payload["subject"]).strip(),
                "preheader": str(payload.get("preheader") or "").strip(),
                "html_body": str(payload["html_body"]),
                "text_body": str(payload["text_body"]),
                "is_active": 1 if payload.get("is_active", True) else 0,
                "created_at": existing["created_at"] if existing else now,
                "updated_at": now,
            }
            conn.execute(
                """
                INSERT INTO templates (
                    id, name, slug, category, description, subject,
                    preheader, html_body, text_body, is_active,
                    created_at, updated_at
                ) VALUES (
                    :id, :name, :slug, :category, :description, :subject,
                    :preheader, :html_body, :text_body, :is_active,
                    :created_at, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    slug = excluded.slug,
                    category = excluded.category,
                    description = excluded.description,
                    subject = excluded.subject,
                    preheader = excluded.preheader,
                    html_body = excluded.html_body,
                    text_body = excluded.text_body,
                    is_active = excluded.is_active,
                    updated_at = excluded.updated_at
                """,
                row,
            )
        return self.get(template_id)  # type: ignore[return-value]

    def _unique_slug(self, conn: sqlite3.Connection, desired_slug: str, template_id: str) -> str:
        base_slug = desired_slug.strip() or make_id("tpl")
        candidate = base_slug
        suffix = 2

        while True:
            row = conn.execute("SELECT id FROM templates WHERE slug = ?", (candidate,)).fetchone()
            if not row or row["id"] == template_id:
                return candidate
            candidate = f"{base_slug}-{suffix}"
            suffix += 1

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            **dict(row),
            "is_active": bool(row["is_active"]),
        }


class SeedInboxRepository:
    def list(self) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute("SELECT * FROM seed_inboxes ORDER BY provider ASC, label ASC").fetchall()
        return [self._row_to_dict(row) for row in rows]

    def enabled(self) -> list[dict[str, Any]]:
        return [item for item in self.list() if item["enabled"] and item["email_address"]]

    def get(self, seed_inbox_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute("SELECT * FROM seed_inboxes WHERE id = ?", (seed_inbox_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        missing = [field for field in ("provider", "label") if not str(payload.get(field, "")).strip()]
        if missing:
            raise ValueError(f"Missing required seed inbox fields: {', '.join(missing)}")

        existing = self.get(str(payload.get("id", ""))) if payload.get("id") else None
        seed_inbox_id = str(payload.get("id") or make_id("seed"))
        now = utc_now()
        row = {
            "id": seed_inbox_id,
            "provider": str(payload["provider"]).strip(),
            "label": str(payload["label"]).strip(),
            "email_address": str(payload.get("email_address") or "").strip(),
            "notes": str(payload.get("notes") or "").strip(),
            "enabled": 1 if payload.get("enabled", False) else 0,
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
        }

        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO seed_inboxes (
                    id, provider, label, email_address, notes, enabled, created_at, updated_at
                ) VALUES (
                    :id, :provider, :label, :email_address, :notes, :enabled, :created_at, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    provider = excluded.provider,
                    label = excluded.label,
                    email_address = excluded.email_address,
                    notes = excluded.notes,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                row,
            )
        return self.get(seed_inbox_id)  # type: ignore[return-value]

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            **dict(row),
            "enabled": bool(row["enabled"]),
        }


class SeedTestRepository:
    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT seed_test_runs.*, identities.email_address AS identity_email, identities.label AS identity_label
                FROM seed_test_runs
                JOIN identities ON identities.id = seed_test_runs.identity_id
                ORDER BY COALESCE(seed_test_runs.sent_at, seed_test_runs.created_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [self._run_summary(row, conn=conn) for row in rows]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT seed_test_runs.*, identities.email_address AS identity_email, identities.label AS identity_label
                FROM seed_test_runs
                JOIN identities ON identities.id = seed_test_runs.identity_id
                WHERE seed_test_runs.id = ?
                """,
                (run_id,),
            ).fetchone()
            if not row:
                return None
            detail = self._run_summary(row, conn=conn)
            results = conn.execute(
                "SELECT * FROM seed_test_results WHERE run_id = ? ORDER BY provider ASC, label ASC",
                (run_id,),
            ).fetchall()
        detail["results"] = [self._result_to_dict(result) for result in results]
        return detail

    def create_run(
        self,
        *,
        identity_id: str,
        message_id: str | None,
        template_id: str | None,
        subject: str,
        status: str,
        summary: str,
        sent_at: str | None,
        seed_inboxes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        run_id = make_id("seedrun")
        now = utc_now()
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO seed_test_runs (
                    id, identity_id, message_id, template_id, subject, status,
                    summary, created_at, updated_at, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, identity_id, message_id, template_id, subject, status, summary, now, now, sent_at),
            )
            for seed in seed_inboxes:
                result_id = make_id("seedresult")
                conn.execute(
                    """
                    INSERT INTO seed_test_results (
                        id, run_id, seed_inbox_id, provider, label, email_address,
                        accepted, placement, render_status, notes, checked_at,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        result_id,
                        run_id,
                        seed["id"],
                        seed["provider"],
                        seed["label"],
                        seed.get("email_address"),
                        None,
                        "pending",
                        "pending",
                        "",
                        None,
                        now,
                        now,
                    ),
                )
        return self.get_run(run_id)  # type: ignore[return-value]

    def update_results(self, run_id: str, updates: list[dict[str, Any]]) -> dict[str, Any] | None:
        now = utc_now()
        with get_connection() as conn:
            exists = conn.execute("SELECT 1 FROM seed_test_runs WHERE id = ?", (run_id,)).fetchone()
            if not exists:
                return None
            for item in updates:
                accepted = item.get("accepted")
                accepted_value = None if accepted is None else 1 if bool(accepted) else 0
                seed_inbox_id = str(item.get("seed_inbox_id") or "")
                conn.execute(
                    """
                    UPDATE seed_test_results
                    SET accepted = ?, placement = ?, render_status = ?, notes = ?, checked_at = ?, updated_at = ?
                    WHERE run_id = ? AND seed_inbox_id = ?
                    """,
                    (
                        accepted_value,
                        str(item.get("placement") or "pending"),
                        str(item.get("render_status") or "pending"),
                        str(item.get("notes") or ""),
                        now,
                        now,
                        run_id,
                        seed_inbox_id,
                    ),
                )

            summary = self._build_summary(conn, run_id)
            status = "completed" if summary["completed_count"] == summary["result_count"] and summary["result_count"] > 0 else "in_review"
            conn.execute(
                "UPDATE seed_test_runs SET status = ?, summary = ?, updated_at = ? WHERE id = ?",
                (status, summary["summary_text"], now, run_id),
            )
        return self.get_run(run_id)

    def latest_preview(self) -> list[dict[str, str]]:
        latest = self.list_runs(limit=1)
        if not latest:
            return []
        run = self.get_run(latest[0]["id"])
        if not run:
            return []
        preview: list[dict[str, str]] = []
        for result in run.get("results", []):
            preview.append(
                {
                    "provider": result["provider"],
                    "accepted": self._accepted_label(result["accepted"]),
                    "placement": result["placement"].title() if result["placement"] != "pending" else "Pending",
                    "render": result["render_status"].title() if result["render_status"] != "pending" else "Pending",
                }
            )
        return preview

    def _run_summary(self, row: sqlite3.Row, *, conn: sqlite3.Connection) -> dict[str, Any]:
        summary = self._build_summary(conn, row["id"])
        return {
            "id": row["id"],
            "identity_id": row["identity_id"],
            "identity": row["identity_email"],
            "identity_label": row["identity_label"],
            "message_id": row["message_id"],
            "template_id": row["template_id"],
            "subject": row["subject"],
            "status": row["status"],
            "summary": row["summary"] or summary["summary_text"],
            "result_count": summary["result_count"],
            "completed_count": summary["completed_count"],
            "inbox_count": summary["inbox_count"],
            "spam_count": summary["spam_count"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "sent_at": row["sent_at"],
        }

    def _build_summary(self, conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
        rows = conn.execute(
            "SELECT accepted, placement, render_status FROM seed_test_results WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        result_count = len(rows)
        completed_count = len([row for row in rows if row["placement"] != "pending" or row["render_status"] != "pending" or row["accepted"] is not None])
        inbox_count = len([row for row in rows if row["placement"] == "inbox"])
        spam_count = len([row for row in rows if row["placement"] == "spam"])
        summary_text = f"{completed_count}/{result_count} seed results recorded. {inbox_count} inbox, {spam_count} spam."
        return {
            "result_count": result_count,
            "completed_count": completed_count,
            "inbox_count": inbox_count,
            "spam_count": spam_count,
            "summary_text": summary_text,
        }

    def _accepted_label(self, value: int | None) -> str:
        if value is None:
            return "Pending"
        return "Yes" if value else "No"

    def _result_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            **dict(row),
            "accepted": None if row["accepted"] is None else bool(row["accepted"]),
        }
