from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator
from uuid import uuid4

from tmail_api.config import get_settings


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
        "html_body": "<html><body><p>This is a TMail seed test probe.</p><p><a href=\"https://api.tmail.tokentap.ca/api/health\">Check the API health endpoint</a></p></body></html>",
        "text_body": "This is a TMail seed test probe.\n\nCheck the API health endpoint: https://api.tmail.tokentap.ca/api/health",
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
        seed_default_identities(conn)
        seed_default_templates(conn)
        seed_default_seed_inboxes(conn)


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
