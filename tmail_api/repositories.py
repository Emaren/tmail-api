from __future__ import annotations

import json
import os
import re
import sqlite3
from typing import Any

from tmail_api.db import ensure_contact_row, get_connection, make_id, utc_now


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or make_id("tpl")


def normalize_tags(values: list[Any] | None) -> list[str]:
    normalized: list[str] = []
    for value in values or []:
        tag = str(value or "").strip().lower()
        if tag and tag not in normalized:
            normalized.append(tag)
    return normalized


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
            contacts = conn.execute(
                """
                SELECT message_contacts.*, contacts.display_name, contacts.company, contacts.tags_json, contacts.notes AS contact_notes
                FROM message_contacts
                JOIN contacts ON contacts.id = message_contacts.contact_id
                WHERE message_contacts.message_id = ?
                ORDER BY message_contacts.email_address ASC
                """,
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
        detail["contacts"] = [self._message_contact_to_dict(contact) for contact in contacts]
        return detail

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        message_id = make_id("msg")
        now = utc_now()
        row = {
            "id": message_id,
            "identity_id": payload["identity_id"],
            "template_id": payload.get("template_id"),
            "campaign_id": payload.get("campaign_id"),
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
                    id, identity_id, template_id, campaign_id, subject, preheader, html_body, text_body,
                    recipients_json, status, send_mode, tracking_enabled, pixel_enabled,
                    preview, provider_message_id, error_message, created_at, updated_at, sent_at
                ) VALUES (
                    :id, :identity_id, :template_id, :campaign_id, :subject, :preheader, :html_body, :text_body,
                    :recipients_json, :status, :send_mode, :tracking_enabled, :pixel_enabled,
                    :preview, :provider_message_id, :error_message, :created_at, :updated_at, :sent_at
                )
                """,
                row,
            )
            self._link_contacts(
                conn,
                message_id=message_id,
                recipients=payload.get("recipients", []),
                status=row["status"],
                sent_at=row["sent_at"],
                created_at=now,
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
            self._apply_event_to_message_contacts(conn, message_id, event_type, payload, occurred_at)
        return {
            "id": event_id,
            "message_id": message_id,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "payload": payload,
        }

    def record_outcome(self, message_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        outcome = str(payload.get("outcome") or "").strip()
        note = str(payload.get("note") or "").strip()
        actor = str(payload.get("actor") or "operator").strip() or "operator"
        if not outcome:
            raise ValueError("Outcome is required.")
        if not self.get(message_id):
            raise ValueError("Message not found")

        event_map = {
            "reply_positive": ("replied_manual", {"reply_state": "positive"}),
            "reply_neutral": ("replied_manual", {"reply_state": "neutral"}),
            "reply_objection": ("replied_manual", {"reply_state": "objection"}),
            "meeting_booked": ("meeting_booked", {"conversion_state": "meeting_booked"}),
            "converted": ("converted", {"conversion_state": "converted"}),
            "dead_thread": ("dead_thread", {"engagement_status": "dead_thread"}),
        }
        if outcome not in event_map:
            raise ValueError("Invalid outcome.")

        with get_connection() as conn:
            target = self._resolve_target_contact(conn, message_id, payload)

        event_type, extra = event_map[outcome]
        event_payload = {
            **extra,
            "actor": actor,
            "note": note or None,
            "contact_id": target["contact_id"],
            "contact_email": target["email_address"],
        }
        self.add_event(message_id, event_type, event_payload)
        return self.get(message_id)  # type: ignore[return-value]

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
            "conversions": counts.get("meeting_booked", 0) + counts.get("converted", 0),
        }

    def _row_to_summary(self, row: sqlite3.Row, *, conn: sqlite3.Connection) -> dict[str, Any]:
        recipients = json.loads(row["recipients_json"])
        counts = self._event_counts(conn, row["id"])
        return {
            "id": row["id"],
            "identity_id": row["identity_id"],
            "template_id": row["template_id"],
            "campaign_id": row["campaign_id"],
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
            "conversions": counts["conversions"],
            "tracking_enabled": bool(row["tracking_enabled"]),
            "pixel_enabled": bool(row["pixel_enabled"]),
            "provider_message_id": row["provider_message_id"],
            "error_message": row["error_message"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "sent_at": row["sent_at"],
        }

    def _link_contacts(
        self,
        conn: sqlite3.Connection,
        *,
        message_id: str,
        recipients: list[Any],
        status: str,
        sent_at: str | None,
        created_at: str,
    ) -> None:
        normalized_recipients: list[str] = []
        for recipient in recipients:
            normalized = str(recipient).strip().lower()
            if normalized and normalized not in normalized_recipients:
                normalized_recipients.append(normalized)

        if not normalized_recipients:
            return

        delivery_status = "sent" if status == "Sent" or sent_at else "draft"
        engagement_status = "sent" if delivery_status == "sent" else "draft"

        for email_address in normalized_recipients:
            contact_id, stored_email = ensure_contact_row(conn, email_address)
            conn.execute(
                """
                INSERT OR IGNORE INTO message_contacts (
                    id, message_id, contact_id, email_address, delivery_status,
                    inferred_open_count, inferred_click_count, reply_state,
                    conversion_state, engagement_status, notes, sent_at,
                    last_opened_at, last_clicked_at, last_replied_at,
                    last_converted_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    make_id("msgcontact"),
                    message_id,
                    contact_id,
                    stored_email,
                    delivery_status,
                    0,
                    0,
                    "",
                    "",
                    engagement_status,
                    "",
                    sent_at,
                    None,
                    None,
                    None,
                    None,
                    created_at,
                    created_at,
                ),
            )

    def _list_message_contacts(self, conn: sqlite3.Connection, message_id: str) -> list[sqlite3.Row]:
        return conn.execute(
            """
            SELECT * FROM message_contacts
            WHERE message_id = ?
            ORDER BY email_address ASC
            """,
            (message_id,),
        ).fetchall()

    def _resolve_target_contact(self, conn: sqlite3.Connection, message_id: str, payload: dict[str, Any]) -> sqlite3.Row:
        contacts = self._list_message_contacts(conn, message_id)
        if not contacts:
            raise ValueError("No contact record is linked to this message.")

        contact_id = str(payload.get("contact_id") or "").strip()
        contact_email = str(payload.get("contact_email") or "").strip().lower()
        if contact_id:
            for contact in contacts:
                if contact["contact_id"] == contact_id:
                    return contact
            raise ValueError("Selected contact is not linked to this message.")
        if contact_email:
            for contact in contacts:
                if contact["email_address"] == contact_email:
                    return contact
            raise ValueError("Selected contact email is not linked to this message.")
        if len(contacts) == 1:
            return contacts[0]
        raise ValueError("Select a recipient contact before marking manual engagement on a multi-recipient message.")

    def _apply_event_to_message_contacts(
        self,
        conn: sqlite3.Connection,
        message_id: str,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: str,
    ) -> None:
        contacts = self._list_message_contacts(conn, message_id)
        if not contacts:
            return

        if event_type == "sent":
            conn.execute(
                """
                UPDATE message_contacts
                SET delivery_status = 'sent',
                    sent_at = COALESCE(sent_at, ?),
                    engagement_status = CASE
                        WHEN COALESCE(engagement_status, '') IN ('', 'draft') THEN 'sent'
                        ELSE engagement_status
                    END,
                    updated_at = ?
                WHERE message_id = ?
                """,
                (occurred_at, occurred_at, message_id),
            )
            return

        if event_type in {"opened", "clicked"} and len(contacts) != 1:
            return

        if event_type == "opened":
            target = contacts[0]
            conn.execute(
                """
                UPDATE message_contacts
                SET inferred_open_count = inferred_open_count + 1,
                    last_opened_at = ?,
                    engagement_status = CASE
                        WHEN COALESCE(engagement_status, '') IN ('draft', 'sent', 'opened', '') THEN 'opened'
                        ELSE engagement_status
                    END,
                    updated_at = ?
                WHERE id = ?
                """,
                (occurred_at, occurred_at, target["id"]),
            )
            return

        if event_type == "clicked":
            target = contacts[0]
            conn.execute(
                """
                UPDATE message_contacts
                SET inferred_click_count = inferred_click_count + 1,
                    last_clicked_at = ?,
                    engagement_status = CASE
                        WHEN COALESCE(engagement_status, '') IN ('draft', 'sent', 'opened', 'clicked', '') THEN 'clicked'
                        ELSE engagement_status
                    END,
                    updated_at = ?
                WHERE id = ?
                """,
                (occurred_at, occurred_at, target["id"]),
            )
            return

        if event_type in {"replied_manual", "meeting_booked", "converted", "dead_thread"}:
            target = self._resolve_target_contact(conn, message_id, payload)
            note = str(payload.get("note") or "").strip()

            if event_type == "replied_manual":
                reply_state = str(payload.get("reply_state") or "manual").strip() or "manual"
                conn.execute(
                    """
                    UPDATE message_contacts
                    SET reply_state = ?,
                        last_replied_at = ?,
                        engagement_status = CASE
                            WHEN COALESCE(conversion_state, '') != '' THEN engagement_status
                            ELSE ?
                        END,
                        notes = CASE WHEN ? != '' THEN ? ELSE notes END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (reply_state, occurred_at, f"reply_{reply_state}", note, note, occurred_at, target["id"]),
                )
                return

            if event_type == "meeting_booked":
                conn.execute(
                    """
                    UPDATE message_contacts
                    SET conversion_state = 'meeting_booked',
                        last_converted_at = ?,
                        engagement_status = 'meeting_booked',
                        notes = CASE WHEN ? != '' THEN ? ELSE notes END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (occurred_at, note, note, occurred_at, target["id"]),
                )
                return

            if event_type == "converted":
                conn.execute(
                    """
                    UPDATE message_contacts
                    SET conversion_state = 'converted',
                        last_converted_at = ?,
                        engagement_status = 'converted',
                        notes = CASE WHEN ? != '' THEN ? ELSE notes END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (occurred_at, note, note, occurred_at, target["id"]),
                )
                return

            if event_type == "dead_thread":
                conn.execute(
                    """
                    UPDATE message_contacts
                    SET engagement_status = 'dead_thread',
                        notes = CASE WHEN ? != '' THEN ? ELSE notes END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (note, note, occurred_at, target["id"]),
                )

    def _message_contact_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        tags = json.loads(row["tags_json"] or "[]") if row["tags_json"] else []
        return {
            "id": row["id"],
            "contact_id": row["contact_id"],
            "email_address": row["email_address"],
            "display_name": row["display_name"] or "",
            "company": row["company"] or "",
            "tags": tags if isinstance(tags, list) else [],
            "contact_notes": row["contact_notes"] or "",
            "delivery_status": row["delivery_status"],
            "inferred_open_count": row["inferred_open_count"],
            "inferred_click_count": row["inferred_click_count"],
            "reply_state": row["reply_state"] or "",
            "conversion_state": row["conversion_state"] or "",
            "engagement_status": row["engagement_status"] or "",
            "notes": row["notes"] or "",
            "sent_at": row["sent_at"],
            "last_opened_at": row["last_opened_at"],
            "last_clicked_at": row["last_clicked_at"],
            "last_replied_at": row["last_replied_at"],
            "last_converted_at": row["last_converted_at"],
            "updated_at": row["updated_at"],
        }


class ContactRepository:
    def list(self, limit: int = 120) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    contacts.*,
                    COUNT(message_contacts.id) AS message_count,
                    SUM(CASE WHEN message_contacts.delivery_status = 'sent' THEN 1 ELSE 0 END) AS sent_count,
                    SUM(COALESCE(message_contacts.inferred_open_count, 0)) AS open_count,
                    SUM(COALESCE(message_contacts.inferred_click_count, 0)) AS click_count,
                    SUM(CASE WHEN COALESCE(message_contacts.reply_state, '') != '' THEN 1 ELSE 0 END) AS reply_count,
                    SUM(CASE WHEN COALESCE(message_contacts.conversion_state, '') != '' THEN 1 ELSE 0 END) AS conversion_count,
                    MAX(COALESCE(
                        message_contacts.last_converted_at,
                        message_contacts.last_replied_at,
                        message_contacts.last_clicked_at,
                        message_contacts.last_opened_at,
                        message_contacts.sent_at,
                        message_contacts.updated_at,
                        contacts.updated_at
                    )) AS last_activity_at
                FROM contacts
                LEFT JOIN message_contacts ON message_contacts.contact_id = contacts.id
                GROUP BY contacts.id
                ORDER BY COALESCE(last_activity_at, contacts.updated_at) DESC, contacts.email_address ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_summary(row) for row in rows]

    def get(self, contact_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT
                    contacts.*,
                    COUNT(message_contacts.id) AS message_count,
                    SUM(CASE WHEN message_contacts.delivery_status = 'sent' THEN 1 ELSE 0 END) AS sent_count,
                    SUM(COALESCE(message_contacts.inferred_open_count, 0)) AS open_count,
                    SUM(COALESCE(message_contacts.inferred_click_count, 0)) AS click_count,
                    SUM(CASE WHEN COALESCE(message_contacts.reply_state, '') != '' THEN 1 ELSE 0 END) AS reply_count,
                    SUM(CASE WHEN COALESCE(message_contacts.conversion_state, '') != '' THEN 1 ELSE 0 END) AS conversion_count,
                    MAX(COALESCE(
                        message_contacts.last_converted_at,
                        message_contacts.last_replied_at,
                        message_contacts.last_clicked_at,
                        message_contacts.last_opened_at,
                        message_contacts.sent_at,
                        message_contacts.updated_at,
                        contacts.updated_at
                    )) AS last_activity_at
                FROM contacts
                LEFT JOIN message_contacts ON message_contacts.contact_id = contacts.id
                WHERE contacts.id = ?
                GROUP BY contacts.id
                """,
                (contact_id,),
            ).fetchone()
            if not row:
                return None
            detail = self._row_to_summary(row)
            history_rows = conn.execute(
                """
                SELECT
                    message_contacts.*,
                    messages.subject,
                    messages.status,
                    messages.send_mode,
                    messages.sent_at AS message_sent_at
                FROM message_contacts
                JOIN messages ON messages.id = message_contacts.message_id
                WHERE message_contacts.contact_id = ?
                ORDER BY COALESCE(message_contacts.last_converted_at, message_contacts.last_replied_at,
                                  message_contacts.last_clicked_at, message_contacts.last_opened_at,
                                  message_contacts.sent_at, message_contacts.updated_at) DESC
                LIMIT 12
                """,
                (contact_id,),
            ).fetchall()
        detail["history"] = [
            {
                "message_id": history["message_id"],
                "subject": history["subject"],
                "status": history["status"],
                "send_mode": history["send_mode"],
                "delivery_status": history["delivery_status"],
                "inferred_open_count": history["inferred_open_count"],
                "inferred_click_count": history["inferred_click_count"],
                "reply_state": history["reply_state"] or "",
                "conversion_state": history["conversion_state"] or "",
                "engagement_status": history["engagement_status"] or "",
                "sent_at": history["message_sent_at"] or history["sent_at"],
                "updated_at": history["updated_at"],
            }
            for history in history_rows
        ]
        return detail

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        email_address = str(payload.get("email_address") or "").strip().lower()
        if not email_address:
            raise ValueError("Email address is required.")

        contact_id = str(payload.get("id") or "")
        existing = self.get(contact_id) if contact_id else None
        now = utc_now()
        existing_display_name = existing.get("display_name") if existing else ""
        existing_company = existing.get("company") if existing else ""
        existing_source = existing.get("source") if existing else "manual"
        existing_notes = existing.get("notes") if existing else ""
        tags = payload.get("tags") if isinstance(payload.get("tags"), list) else (existing.get("tags") if existing else [])
        row = {
            "id": contact_id or make_id("contact"),
            "email_address": email_address,
            "display_name": str(payload.get("display_name") or existing_display_name).strip(),
            "company": str(payload.get("company") or existing_company).strip(),
            "tags_json": json.dumps(normalize_tags(tags if isinstance(tags, list) else []), separators=(",", ":")),
            "source": str(payload.get("source") or existing_source).strip(),
            "notes": str(payload.get("notes") or existing_notes).strip(),
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
        }
        with get_connection() as conn:
            row_with_default = conn.execute(
                "SELECT id, created_at FROM contacts WHERE email_address = ?",
                (email_address,),
            ).fetchone()
            if row_with_default and not contact_id:
                row["id"] = row_with_default["id"]
                row["created_at"] = row_with_default["created_at"]
            conn.execute(
                """
                INSERT INTO contacts (
                    id, email_address, display_name, company, tags_json,
                    source, notes, created_at, updated_at
                ) VALUES (
                    :id, :email_address, :display_name, :company, :tags_json,
                    :source, :notes, :created_at, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    email_address = excluded.email_address,
                    display_name = excluded.display_name,
                    company = excluded.company,
                    tags_json = excluded.tags_json,
                    source = excluded.source,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                row,
            )
        return self.get(row["id"])  # type: ignore[return-value]

    def _row_to_summary(self, row: sqlite3.Row) -> dict[str, Any]:
        tags = normalize_tags(json.loads(row["tags_json"] or "[]") if row["tags_json"] else [])
        click_count = int(row["click_count"] or 0)
        reply_count = int(row["reply_count"] or 0)
        conversion_count = int(row["conversion_count"] or 0)
        open_count = int(row["open_count"] or 0)
        engagement_score = (conversion_count * 12) + (reply_count * 8) + (click_count * 4) + open_count
        return {
            "id": row["id"],
            "email_address": row["email_address"],
            "display_name": row["display_name"] or "",
            "company": row["company"] or "",
            "tags": tags if isinstance(tags, list) else [],
            "source": row["source"] or "",
            "notes": row["notes"] or "",
            "message_count": int(row["message_count"] or 0),
            "sent_count": int(row["sent_count"] or 0),
            "open_count": open_count,
            "click_count": click_count,
            "reply_count": reply_count,
            "conversion_count": conversion_count,
            "engagement_score": engagement_score,
            "last_activity_at": row["last_activity_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


class SegmentRepository:
    VALID_MATCH_MODES = {"any", "all"}

    def list(self, limit: int = 80) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM segments
                ORDER BY updated_at DESC, name ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [self._row_to_dict(row, conn=conn) for row in rows]

    def get(self, segment_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute("SELECT * FROM segments WHERE id = ?", (segment_id,)).fetchone()
            return self._row_to_dict(row, conn=conn) if row else None

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("Segment name is required.")

        match_mode = str(payload.get("match_mode") or "any").strip().lower()
        if match_mode not in self.VALID_MATCH_MODES:
            raise ValueError("Match mode must be 'any' or 'all'.")

        tags = normalize_tags(payload.get("tags") if isinstance(payload.get("tags"), list) else [])
        if not tags:
            raise ValueError("Add at least one tag to define the segment.")

        segment_id = str(payload.get("id") or "")
        existing = self.get(segment_id) if segment_id else None
        now = utc_now()
        row = {
            "id": segment_id or make_id("segment"),
            "name": name,
            "description": str(payload.get("description") or (existing.get("description") if existing else "") or "").strip(),
            "match_mode": match_mode,
            "tags_json": json.dumps(tags, separators=(",", ":")),
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
        }

        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO segments (
                    id, name, description, match_mode, tags_json, created_at, updated_at
                ) VALUES (
                    :id, :name, :description, :match_mode, :tags_json, :created_at, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    description = excluded.description,
                    match_mode = excluded.match_mode,
                    tags_json = excluded.tags_json,
                    updated_at = excluded.updated_at
                """,
                row,
            )
        return self.get(row["id"])  # type: ignore[return-value]

    def resolve_contacts(self, segment_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        with get_connection() as conn:
            row = conn.execute("SELECT * FROM segments WHERE id = ?", (segment_id,)).fetchone()
            if not row:
                return []
            return self._resolve_contacts(conn, row, limit=limit)

    def resolve_email_addresses(self, segment_id: str) -> list[str]:
        contacts = self.resolve_contacts(segment_id)
        return [contact["email_address"] for contact in contacts if contact.get("email_address")]

    def _row_to_dict(self, row: sqlite3.Row, *, conn: sqlite3.Connection) -> dict[str, Any]:
        tags = normalize_tags(json.loads(row["tags_json"] or "[]") if row["tags_json"] else [])
        contacts = self._resolve_contacts(conn, row, limit=6)
        all_contacts = self._resolve_contacts(conn, row, limit=None)
        return {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"] or "",
            "match_mode": row["match_mode"] or "any",
            "tags": tags,
            "contact_count": len(all_contacts),
            "contact_emails": [contact["email_address"] for contact in contacts if contact.get("email_address")],
            "contacts_preview": [
                {
                    "id": contact["id"],
                    "email_address": contact["email_address"],
                    "display_name": contact["display_name"] or "",
                    "company": contact["company"] or "",
                }
                for contact in contacts
            ],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _resolve_contacts(self, conn: sqlite3.Connection, row: sqlite3.Row, *, limit: int | None) -> list[dict[str, Any]]:
        tags = normalize_tags(json.loads(row["tags_json"] or "[]") if row["tags_json"] else [])
        if not tags:
            return []

        contacts = conn.execute(
            """
            SELECT id, email_address, display_name, company, tags_json
            FROM contacts
            ORDER BY updated_at DESC, email_address ASC
            """
        ).fetchall()
        matched: list[dict[str, Any]] = []
        for contact in contacts:
            contact_tags = set(normalize_tags(json.loads(contact["tags_json"] or "[]") if contact["tags_json"] else []))
            if not contact_tags:
                continue
            if row["match_mode"] == "all":
                ok = all(tag in contact_tags for tag in tags)
            else:
                ok = any(tag in contact_tags for tag in tags)
            if not ok:
                continue
            matched.append(
                {
                    "id": contact["id"],
                    "email_address": contact["email_address"],
                    "display_name": contact["display_name"] or "",
                    "company": contact["company"] or "",
                }
            )
            if limit and len(matched) >= limit:
                break
        return matched


class TemplateRepository:
    def list(self) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute("SELECT * FROM templates ORDER BY updated_at DESC, name ASC").fetchall()
            return [self._row_to_dict(row, conn=conn) for row in rows]

    def get(self, template_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute("SELECT * FROM templates WHERE id = ?", (template_id,)).fetchone()
            return self._row_to_dict(row, conn=conn) if row else None

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
            self._snapshot_version_if_changed(conn, row)
        return self.get(template_id)  # type: ignore[return-value]

    def list_versions(self, template_id: str, limit: int = 12) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM template_versions
                WHERE template_id = ?
                ORDER BY version_number DESC, created_at DESC
                LIMIT ?
                """,
                (template_id, limit),
            ).fetchall()
        return [self._version_to_dict(row) for row in rows]

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

    def _snapshot_version_if_changed(self, conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        latest = conn.execute(
            """
            SELECT * FROM template_versions
            WHERE template_id = ?
            ORDER BY version_number DESC
            LIMIT 1
            """,
            (row["id"],),
        ).fetchone()

        if latest and self._version_matches_row(latest, row):
            return

        next_version = (latest["version_number"] if latest else 0) + 1
        conn.execute(
            """
            INSERT INTO template_versions (
                id, template_id, version_number, name, category, description,
                subject, preheader, html_body, text_body, is_active, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                make_id("tplver"),
                row["id"],
                next_version,
                row["name"],
                row["category"],
                row["description"],
                row["subject"],
                row["preheader"],
                row["html_body"],
                row["text_body"],
                row["is_active"],
                row["updated_at"],
            ),
        )

    def _version_matches_row(self, version_row: sqlite3.Row, row: dict[str, Any]) -> bool:
        return all(
            [
                version_row["name"] == row["name"],
                version_row["category"] == row["category"],
                (version_row["description"] or "") == row["description"],
                version_row["subject"] == row["subject"],
                (version_row["preheader"] or "") == row["preheader"],
                version_row["html_body"] == row["html_body"],
                version_row["text_body"] == row["text_body"],
                bool(version_row["is_active"]) == bool(row["is_active"]),
            ]
        )

    def _version_meta(self, conn: sqlite3.Connection, template_id: str) -> dict[str, Any]:
        row = conn.execute(
            """
            SELECT COUNT(*) AS version_count, MAX(version_number) AS current_version
            FROM template_versions
            WHERE template_id = ?
            """,
            (template_id,),
        ).fetchone()
        return {
            "version_count": row["version_count"] if row else 0,
            "current_version": row["current_version"] if row else 0,
        }

    def _row_to_dict(self, row: sqlite3.Row, *, conn: sqlite3.Connection) -> dict[str, Any]:
        meta = self._version_meta(conn, row["id"])
        return {
            **dict(row),
            "is_active": bool(row["is_active"]),
            **meta,
        }

    def _version_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
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
            "accepted_count": summary["accepted_count"],
            "rejected_count": summary["rejected_count"],
            "inbox_count": summary["inbox_count"],
            "promotions_count": summary["promotions_count"],
            "spam_count": summary["spam_count"],
            "missing_count": summary["missing_count"],
            "clean_count": summary["clean_count"],
            "issues_count": summary["issues_count"],
            "acceptance_score": summary["acceptance_score"],
            "placement_score": summary["placement_score"],
            "render_score": summary["render_score"],
            "overall_score": summary["overall_score"],
            "score_state": summary["score_state"],
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
        accepted_count = len([row for row in rows if row["accepted"] == 1])
        rejected_count = len([row for row in rows if row["accepted"] == 0])
        inbox_count = len([row for row in rows if row["placement"] == "inbox"])
        promotions_count = len([row for row in rows if row["placement"] == "promotions"])
        spam_count = len([row for row in rows if row["placement"] == "spam"])
        missing_count = len([row for row in rows if row["placement"] == "missing"])
        clean_count = len([row for row in rows if row["render_status"] == "clean"])
        issues_count = len([row for row in rows if row["render_status"] == "issues"])

        accepted_evaluated = accepted_count + rejected_count
        acceptance_score = round((accepted_count / accepted_evaluated) * 100) if accepted_evaluated else 0

        placement_values = {"inbox": 100, "promotions": 72, "spam": 12, "missing": 0}
        placement_rows = [placement_values[row["placement"]] for row in rows if row["placement"] in placement_values]
        placement_score = round(sum(placement_rows) / len(placement_rows)) if placement_rows else 0

        render_values = {"clean": 100, "issues": 45}
        render_rows = [render_values[row["render_status"]] for row in rows if row["render_status"] in render_values]
        render_score = round(sum(render_rows) / len(render_rows)) if render_rows else 0

        completion_ratio = (completed_count / result_count) if result_count else 0
        weighted_score = (acceptance_score * 0.35) + (placement_score * 0.45) + (render_score * 0.20)
        overall_score = round(weighted_score * completion_ratio)
        score_state = self._score_state(overall_score, spam_count=spam_count, missing_count=missing_count, result_count=result_count)
        summary_text = (
            f"{completed_count}/{result_count} seed results recorded. "
            f"Score {overall_score}. {inbox_count} inbox, {promotions_count} promotions, {spam_count} spam, {missing_count} missing."
        )
        return {
            "result_count": result_count,
            "completed_count": completed_count,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "inbox_count": inbox_count,
            "promotions_count": promotions_count,
            "spam_count": spam_count,
            "missing_count": missing_count,
            "clean_count": clean_count,
            "issues_count": issues_count,
            "acceptance_score": acceptance_score,
            "placement_score": placement_score,
            "render_score": render_score,
            "overall_score": overall_score,
            "score_state": score_state,
            "summary_text": summary_text,
        }

    def _score_state(self, overall_score: int, *, spam_count: int, missing_count: int, result_count: int) -> str:
        if result_count == 0:
            return "attention"
        if spam_count > 0 or overall_score < 45:
            return "critical"
        if missing_count > 0 or overall_score < 75:
            return "attention"
        return "healthy"

    def _accepted_label(self, value: int | None) -> str:
        if value is None:
            return "Pending"
        return "Yes" if value else "No"

    def _result_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            **dict(row),
            "accepted": None if row["accepted"] is None else bool(row["accepted"]),
        }
