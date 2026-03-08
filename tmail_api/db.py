from __future__ import annotations

import os
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from uuid import uuid4

from tmail_api.config import get_settings
from tmail_api.security import hash_password


SCHEMA = """
CREATE TABLE IF NOT EXISTS identities (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    display_name TEXT NOT NULL,
    email_address TEXT NOT NULL UNIQUE,
    provider_type TEXT NOT NULL,
    smtp_host TEXT NOT NULL,
    smtp_port INTEGER NOT NULL,
    smtp_username TEXT NOT NULL,
    smtp_secret_env TEXT NOT NULL,
    use_tls INTEGER NOT NULL DEFAULT 1,
    reply_to TEXT,
    tracking_enabled INTEGER NOT NULL DEFAULT 1,
    pixel_enabled INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'attention',
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    identity_id TEXT NOT NULL,
    template_id TEXT,
    campaign_id TEXT,
    subject TEXT NOT NULL,
    preheader TEXT,
    html_body TEXT NOT NULL,
    text_body TEXT NOT NULL,
    recipients_json TEXT NOT NULL,
    status TEXT NOT NULL,
    send_mode TEXT NOT NULL,
    tracking_enabled INTEGER NOT NULL DEFAULT 1,
    pixel_enabled INTEGER NOT NULL DEFAULT 1,
    preview TEXT,
    provider_message_id TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    sent_at TEXT,
    FOREIGN KEY(identity_id) REFERENCES identities(id)
);

CREATE TABLE IF NOT EXISTS operators (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'owner',
    password_hash TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    totp_secret TEXT,
    pending_totp_secret TEXT,
    totp_enabled INTEGER NOT NULL DEFAULT 0,
    last_login_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contacts (
    id TEXT PRIMARY KEY,
    email_address TEXT NOT NULL UNIQUE,
    display_name TEXT,
    company TEXT,
    tags_json TEXT NOT NULL DEFAULT '[]',
    source TEXT,
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    match_mode TEXT NOT NULL DEFAULT 'any',
    tags_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    FOREIGN KEY(message_id) REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS tracked_links (
    token TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    url TEXT NOT NULL,
    label TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(message_id) REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS message_contacts (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    email_address TEXT NOT NULL,
    delivery_status TEXT NOT NULL DEFAULT 'draft',
    inferred_open_count INTEGER NOT NULL DEFAULT 0,
    inferred_click_count INTEGER NOT NULL DEFAULT 0,
    reply_state TEXT,
    conversion_state TEXT,
    engagement_status TEXT,
    notes TEXT,
    sent_at TEXT,
    last_opened_at TEXT,
    last_clicked_at TEXT,
    last_replied_at TEXT,
    last_converted_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(message_id, contact_id),
    FOREIGN KEY(message_id) REFERENCES messages(id),
    FOREIGN KEY(contact_id) REFERENCES contacts(id)
);

CREATE TABLE IF NOT EXISTS templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,
    description TEXT,
    subject TEXT NOT NULL,
    preheader TEXT,
    html_body TEXT NOT NULL,
    text_body TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS template_versions (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    description TEXT,
    subject TEXT NOT NULL,
    preheader TEXT,
    html_body TEXT NOT NULL,
    text_body TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    FOREIGN KEY(template_id) REFERENCES templates(id)
);

CREATE TABLE IF NOT EXISTS seed_inboxes (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    label TEXT NOT NULL,
    email_address TEXT,
    notes TEXT,
    enabled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS seed_test_runs (
    id TEXT PRIMARY KEY,
    identity_id TEXT NOT NULL,
    message_id TEXT,
    template_id TEXT,
    subject TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    sent_at TEXT,
    FOREIGN KEY(identity_id) REFERENCES identities(id),
    FOREIGN KEY(message_id) REFERENCES messages(id),
    FOREIGN KEY(template_id) REFERENCES templates(id)
);

CREATE TABLE IF NOT EXISTS seed_test_results (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    seed_inbox_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    label TEXT NOT NULL,
    email_address TEXT,
    accepted INTEGER,
    placement TEXT,
    render_status TEXT,
    notes TEXT,
    checked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES seed_test_runs(id),
    FOREIGN KEY(seed_inbox_id) REFERENCES seed_inboxes(id)
);

CREATE TABLE IF NOT EXISTS campaigns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    objective TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    identity_id TEXT NOT NULL,
    template_id TEXT,
    audience_source TEXT NOT NULL DEFAULT 'manual',
    segment_id TEXT,
    audience_label TEXT NOT NULL,
    audience_emails TEXT,
    send_window TEXT,
    notes TEXT,
    scheduled_for TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(identity_id) REFERENCES identities(id),
    FOREIGN KEY(template_id) REFERENCES templates(id),
    FOREIGN KEY(segment_id) REFERENCES segments(id)
);

CREATE TABLE IF NOT EXISTS campaign_runs (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    message_id TEXT,
    mode TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    status TEXT NOT NULL,
    recipient_count INTEGER NOT NULL DEFAULT 0,
    sent_count INTEGER NOT NULL DEFAULT 0,
    summary TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(campaign_id) REFERENCES campaigns(id),
    FOREIGN KEY(message_id) REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS scheduler_runs (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL DEFAULT 'campaigns',
    trigger_type TEXT NOT NULL,
    status TEXT NOT NULL,
    due_count INTEGER NOT NULL DEFAULT 0,
    launched_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    summary TEXT,
    campaign_ids_json TEXT NOT NULL DEFAULT '[]',
    run_ids_json TEXT NOT NULL DEFAULT '[]',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


DEFAULT_IDENTITIES = [
    {
        "id": "tony-me",
        "label": "Founder Rail",
        "display_name": "Tony Blum",
        "email_address": "tonyblum@me.com",
        "provider_type": "apple_smtp",
        "smtp_username": "tonyblum@me.com",
        "smtp_secret_env": "TMAIL_APPLE_TONYBLUM_PASSWORD",
        "notes": "Primary founder identity routed through Apple SMTP.",
    },
    {
        "id": "ws-info",
        "label": "Brand Rail",
        "display_name": "Wheat & Stone",
        "email_address": "info@wheatandstone.ca",
        "provider_type": "apple_smtp",
        "smtp_username": "tonyblum@me.com",
        "smtp_secret_env": "TMAIL_APPLE_WHEATANDSTONE_PASSWORD",
        "notes": "Apple custom-domain identity for branded sends. Auth rides through the owning Apple account username.",
    },
]

DEFAULT_TEMPLATES = [
    {
        "id": "tpl-founder-note",
        "name": "Founder Note",
        "slug": "founder-note",
        "category": "Founder",
        "description": "A plainspoken founder outreach template with a soft CTA and clean fallback text.",
        "subject": "Quick founder note from Tony",
        "preheader": "A short, direct note sent through TMail.",
        "html_body": "<html><body><p>Hey there,</p><p>I wanted to send a quick note directly and keep it simple.</p><p><a href=\"https://wheatandstone.ca\">Open the destination page</a></p><p>Tony</p></body></html>",
        "text_body": "Hey there,\n\nI wanted to send a quick note directly and keep it simple.\n\nOpen the destination page: https://wheatandstone.ca\n\nTony",
        "is_active": 1,
    },
    {
        "id": "tpl-brand-checkpoint",
        "name": "Brand Checkpoint",
        "slug": "brand-checkpoint",
        "category": "Operations",
        "description": "A branded update template for product or campaign checkpoints.",
        "subject": "TMail checkpoint update",
        "preheader": "Progress update from the TMail operator desk.",
        "html_body": "<html><body><p>Hello,</p><p>Here is the current TMail checkpoint and what changed in this pass.</p><ul><li>Sending rail verified</li><li>Tracking verified</li><li>Next build target defined</li></ul><p><a href=\"https://tmail.tokentap.ca/dashboard\">Open the dashboard</a></p></body></html>",
        "text_body": "Hello,\n\nHere is the current TMail checkpoint and what changed in this pass.\n- Sending rail verified\n- Tracking verified\n- Next build target defined\n\nOpen the dashboard: https://tmail.tokentap.ca/dashboard",
        "is_active": 1,
    },
    {
        "id": "tpl-seed-lab",
        "name": "Seed Lab Probe",
        "slug": "seed-lab-probe",
        "category": "Testing",
        "description": "A minimal test message for seed inbox placement and render capture.",
        "subject": "TMail seed test probe",
        "preheader": "Seed inbox validation run from TMail.",
        "html_body": "<html><body><p>This is a TMail seed test probe.</p><p><a href=\"https://api.tmail.tokentap.ca/healthz\">Check the public API health endpoint</a></p></body></html>",
        "text_body": "This is a TMail seed test probe.\n\nCheck the public API health endpoint: https://api.tmail.tokentap.ca/healthz",
        "is_active": 1,
    },
]

DEFAULT_SEED_INBOXES = [
    {
        "id": "seed-gmail",
        "provider": "Gmail",
        "label": "Gmail Primary",
        "email_address": "",
        "notes": "Configure a real Gmail seed inbox before enabling.",
        "enabled": 0,
    },
    {
        "id": "seed-outlook",
        "provider": "Outlook",
        "label": "Outlook Primary",
        "email_address": "",
        "notes": "Configure a real Outlook/Hotmail seed inbox before enabling.",
        "enabled": 0,
    },
    {
        "id": "seed-yahoo",
        "provider": "Yahoo",
        "label": "Yahoo Primary",
        "email_address": "",
        "notes": "Configure a real Yahoo seed inbox before enabling.",
        "enabled": 0,
    },
    {
        "id": "seed-icloud",
        "provider": "iCloud",
        "label": "iCloud Primary",
        "email_address": "",
        "notes": "Configure a real iCloud seed inbox before enabling.",
        "enabled": 0,
    },
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def connect() -> sqlite3.Connection:
    settings = get_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    settings = get_settings()
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        conn.executescript(SCHEMA)
        run_migrations(conn)
        seed_default_identities(conn)
        seed_default_templates(conn)
        repair_default_template_content(conn)
        seed_missing_template_versions(conn)
        seed_default_seed_inboxes(conn)
        seed_default_operator(conn)
        seed_missing_message_contacts(conn)


def run_migrations(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "messages", "template_id", "TEXT")
    ensure_column(conn, "messages", "campaign_id", "TEXT")
    ensure_column(conn, "campaigns", "audience_emails", "TEXT")
    ensure_column(conn, "campaigns", "audience_source", "TEXT DEFAULT 'manual'")
    ensure_column(conn, "campaigns", "segment_id", "TEXT")


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def seed_default_identities(conn: sqlite3.Connection) -> None:
    settings = get_settings()
    for identity in DEFAULT_IDENTITIES:
        exists = conn.execute(
            "SELECT 1 FROM identities WHERE id = ?",
            (identity["id"],),
        ).fetchone()
        if exists:
            continue

        now = utc_now()
        conn.execute(
            """
            INSERT INTO identities (
                id, label, display_name, email_address, provider_type,
                smtp_host, smtp_port, smtp_username, smtp_secret_env,
                use_tls, reply_to, tracking_enabled, pixel_enabled,
                status, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity["id"],
                identity["label"],
                identity["display_name"],
                identity["email_address"],
                identity["provider_type"],
                settings.default_smtp_host,
                settings.default_smtp_port,
                identity.get("smtp_username", identity["email_address"]),
                identity["smtp_secret_env"],
                1,
                identity["email_address"],
                1,
                1,
                "attention",
                identity["notes"],
                now,
                now,
            ),
        )


def seed_default_templates(conn: sqlite3.Connection) -> None:
    for template in DEFAULT_TEMPLATES:
        exists = conn.execute(
            "SELECT 1 FROM templates WHERE id = ?",
            (template["id"],),
        ).fetchone()
        if exists:
            continue

        now = utc_now()
        conn.execute(
            """
            INSERT INTO templates (
                id, name, slug, category, description, subject,
                preheader, html_body, text_body, is_active,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                template["id"],
                template["name"],
                template["slug"],
                template["category"],
                template["description"],
                template["subject"],
                template["preheader"],
                template["html_body"],
                template["text_body"],
                template["is_active"],
                now,
                now,
            ),
        )


def repair_default_template_content(conn: sqlite3.Connection) -> None:
    now = utc_now()
    conn.execute(
        """
        UPDATE templates
        SET html_body = ?,
            text_body = ?,
            updated_at = ?
        WHERE id = 'tpl-seed-lab'
          AND (html_body LIKE '%https://api.tmail.tokentap.ca/api/health%'
               OR text_body LIKE '%https://api.tmail.tokentap.ca/api/health%')
        """,
        (
            "<html><body><p>This is a TMail seed test probe.</p><p><a href=\"https://api.tmail.tokentap.ca/healthz\">Check the public API health endpoint</a></p></body></html>",
            "This is a TMail seed test probe.\n\nCheck the public API health endpoint: https://api.tmail.tokentap.ca/healthz",
            now,
        ),
    )


def seed_default_seed_inboxes(conn: sqlite3.Connection) -> None:
    for seed in DEFAULT_SEED_INBOXES:
        exists = conn.execute(
            "SELECT 1 FROM seed_inboxes WHERE id = ?",
            (seed["id"],),
        ).fetchone()
        if exists:
            continue

        now = utc_now()
        conn.execute(
            """
            INSERT INTO seed_inboxes (
                id, provider, label, email_address, notes, enabled,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                seed["id"],
                seed["provider"],
                seed["label"],
                seed["email_address"],
                seed["notes"],
                seed["enabled"],
                now,
                now,
            ),
        )


def seed_missing_template_versions(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT * FROM templates ORDER BY created_at ASC").fetchall()
    for row in rows:
        exists = conn.execute(
            "SELECT 1 FROM template_versions WHERE template_id = ? LIMIT 1",
            (row["id"],),
        ).fetchone()
        if exists:
            continue

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
                1,
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


def seed_default_operator(conn: sqlite3.Connection) -> None:
    existing = conn.execute("SELECT COUNT(*) AS total FROM operators").fetchone()
    if existing and existing["total"]:
        return

    admin_username = os.getenv("TMAIL_ADMIN_USERNAME", "tony").strip() or "tony"
    admin_password = os.getenv("TMAIL_ADMIN_PASSWORD", "").strip()
    if not admin_password:
        return

    now = utc_now()
    conn.execute(
        """
        INSERT INTO operators (
            id, username, display_name, role, password_hash, is_active,
            totp_secret, pending_totp_secret, totp_enabled, last_login_at,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "operator-tony",
            admin_username,
            "Tony Blum",
            "owner",
            hash_password(admin_password),
            1,
            None,
            None,
            0,
            None,
            now,
            now,
        ),
    )


def ensure_contact_row(conn: sqlite3.Connection, email_address: str, *, source: str = "message_recipient") -> tuple[str, str]:
    normalized = email_address.strip().lower()
    if not normalized:
        raise ValueError("Contact email is required")

    row = conn.execute(
        "SELECT id, email_address FROM contacts WHERE email_address = ?",
        (normalized,),
    ).fetchone()
    if row:
        return row["id"], row["email_address"]

    contact_id = make_id("contact")
    now = utc_now()
    conn.execute(
        """
        INSERT INTO contacts (
            id, email_address, display_name, company, tags_json,
            source, notes, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (contact_id, normalized, "", "", "[]", source, "", now, now),
    )
    return contact_id, normalized


def seed_missing_message_contacts(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, recipients_json, status, created_at, sent_at FROM messages ORDER BY created_at ASC"
    ).fetchall()

    for row in rows:
        exists = conn.execute(
            "SELECT 1 FROM message_contacts WHERE message_id = ? LIMIT 1",
            (row["id"],),
        ).fetchone()
        if exists:
            continue

        try:
            recipients = json.loads(row["recipients_json"] or "[]")
        except json.JSONDecodeError:
            recipients = []

        normalized_recipients: list[str] = []
        for recipient in recipients:
            normalized = str(recipient).strip().lower()
            if normalized and normalized not in normalized_recipients:
                normalized_recipients.append(normalized)

        if not normalized_recipients:
            continue

        inferred_open_count = 0
        inferred_click_count = 0
        last_opened_at = None
        last_clicked_at = None
        if len(normalized_recipients) == 1:
            open_row = conn.execute(
                """
                SELECT COUNT(*) AS total, MAX(occurred_at) AS last_at
                FROM events
                WHERE message_id = ? AND event_type = 'opened'
                """,
                (row["id"],),
            ).fetchone()
            click_row = conn.execute(
                """
                SELECT COUNT(*) AS total, MAX(occurred_at) AS last_at
                FROM events
                WHERE message_id = ? AND event_type = 'clicked'
                """,
                (row["id"],),
            ).fetchone()
            inferred_open_count = int(open_row["total"] or 0)
            inferred_click_count = int(click_row["total"] or 0)
            last_opened_at = open_row["last_at"]
            last_clicked_at = click_row["last_at"]

        delivery_status = "sent" if row["status"] == "Sent" or row["sent_at"] else "draft"
        engagement_status = (
            "clicked"
            if inferred_click_count
            else "opened"
            if inferred_open_count
            else "sent"
            if delivery_status == "sent"
            else "draft"
        )

        for email_address in normalized_recipients:
            contact_id, stored_email = ensure_contact_row(conn, email_address)
            now = utc_now()
            conn.execute(
                """
                INSERT INTO message_contacts (
                    id, message_id, contact_id, email_address, delivery_status,
                    inferred_open_count, inferred_click_count, reply_state,
                    conversion_state, engagement_status, notes, sent_at,
                    last_opened_at, last_clicked_at, last_replied_at,
                    last_converted_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    make_id("msgcontact"),
                    row["id"],
                    contact_id,
                    stored_email,
                    delivery_status,
                    inferred_open_count if len(normalized_recipients) == 1 else 0,
                    inferred_click_count if len(normalized_recipients) == 1 else 0,
                    "",
                    "",
                    engagement_status,
                    "",
                    row["sent_at"],
                    last_opened_at if len(normalized_recipients) == 1 else None,
                    last_clicked_at if len(normalized_recipients) == 1 else None,
                    None,
                    None,
                    row["created_at"],
                    now,
                ),
            )
