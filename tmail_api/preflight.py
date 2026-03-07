from __future__ import annotations

import re
from html import unescape
from typing import Any
from urllib.parse import urlparse

from tmail_api.config import get_settings


SHORTENER_DOMAINS = {
    "bit.ly",
    "tinyurl.com",
    "t.co",
    "ow.ly",
    "buff.ly",
    "rebrand.ly",
}


def extract_links(html_body: str) -> list[str]:
    href_links = re.findall(r"""href=["']([^"']+)["']""", html_body, flags=re.IGNORECASE)
    text_links = re.findall(r"""https?://[^\s<>"']+""", html_body, flags=re.IGNORECASE)
    return list(dict.fromkeys([*href_links, *text_links]))


def strip_html(html_body: str) -> str:
    text = re.sub(r"<(script|style)\b[^>]*>.*?</\1>", " ", html_body, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", unescape(text))
    return text.strip()


def analyze_preflight(payload: dict[str, Any], *, identity: dict[str, Any] | None = None) -> dict[str, Any]:
    subject = str(payload.get("subject", "")).strip()
    preheader = str(payload.get("preheader", "")).strip()
    html_body = str(payload.get("html_body", "")).strip()
    text_body = str(payload.get("text_body", "")).strip()
    recipients = payload.get("recipients", []) or []
    tracking_enabled = bool(payload.get("tracking_enabled", True))
    pixel_enabled = bool(payload.get("pixel_enabled", True))
    public_base_url = get_settings().public_base_url

    checks: list[dict[str, str]] = []
    errors = 0
    warnings = 0
    score = 100

    def add(level: str, title: str, detail: str) -> None:
        nonlocal score, errors, warnings
        checks.append({"level": level, "title": title, "detail": detail})
        if level == "error":
            errors += 1
            score -= 18
        elif level == "warning":
            warnings += 1
            score -= 8
        elif level == "info":
            score -= 2

    if identity is None:
        add("error", "Sender rail missing", "Select a valid sender identity before sending.")
    elif not identity["health"]["secretConfigured"]:
        add("error", "SMTP secret missing", "The selected sender identity is missing its env-backed Apple credential.")
    else:
        add("pass", "Sender rail healthy", f"{identity['email_address']} is ready for Apple SMTP.")

    if not subject:
        add("error", "Subject missing", "Every message needs a subject line.")
    elif len(subject) > 96:
        add("warning", "Subject is long", "Trim the subject line before sending so it renders cleanly in inbox lists.")
    else:
        add("pass", "Subject length looks good", "The subject line is within a sane range.")

    if not preheader:
        add("warning", "Preheader missing", "Add a preheader to control the preview text in modern inboxes.")
    else:
        add("pass", "Preheader set", "Preview text is present.")

    if not html_body:
        add("error", "HTML body missing", "The HTML payload is empty.")

    if not text_body:
        add("warning", "Plain-text part missing", "Add a text alternative so the message degrades gracefully.")
    elif len(text_body) < 40:
        add("warning", "Plain-text body is thin", "The text version is short enough that some recipients will get a poor fallback.")
    else:
        add("pass", "Plain-text part present", "The text fallback is populated.")

    links = extract_links(html_body)
    visible_text = strip_html(html_body)
    image_count = len(re.findall(r"<img\b", html_body, flags=re.IGNORECASE))
    shortener_hits = [link for link in links if urlparse(link).netloc.lower() in SHORTENER_DOMAINS]

    if len(links) > 6:
        add("warning", "Link count is high", "This draft has more than six links. That is heavy for a founder-style email.")
    elif len(links) == 0:
        add("info", "No links found", "No tracked destinations were detected in the HTML payload.")
    else:
        add("pass", "Link count is sane", f"{len(links)} link{'s' if len(links) != 1 else ''} detected.")

    if shortener_hits:
        add("warning", "Shortener detected", "Avoid link shorteners if you want cleaner trust signals.")

    if image_count and len(visible_text) < 90:
        add("warning", "Image-heavy draft", "The HTML looks visually heavy relative to the visible text.")
    elif image_count:
        add("info", "Images detected", "Rendered assets are present. Verify they are necessary and load correctly.")

    if len(recipients) == 0:
        add("warning", "Recipient list empty", "Draft mode is fine, but send actions need at least one recipient.")
    elif len(recipients) > 25:
        add("warning", "Recipient count is high", "Large recipient batches should move into the campaign engine rather than the founder composer.")
    else:
        add("pass", "Recipient count is sane", f"{len(recipients)} recipient{'s' if len(recipients) != 1 else ''} queued.")

    if tracking_enabled:
        add("pass", "Tracked links enabled", "Outbound links will be rewritten for click telemetry.")
    else:
        add("warning", "Tracked links disabled", "This draft will not record click engagement.")

    if pixel_enabled:
        add("info", "Soft-open pixel enabled", "Opens remain a weak signal. Treat clicks and replies as stronger truth.")

    if public_base_url.startswith("http://127.0.0.1") or "localhost" in public_base_url:
        add("warning", "Tracker base is local", "Open and click telemetry will only resolve while the local tracker is reachable.")
    else:
        add("pass", "Tracker base is public", f"Tracking resolves through {public_base_url}.")

    score = max(score, 0)
    status = "blocked" if errors else "warning" if warnings else "ready"
    summary = (
        "Fix blocking issues before sending."
        if status == "blocked"
        else "Ready with a few warnings."
        if status == "warning"
        else "Draft is ready for send."
    )

    return {
        "status": status,
        "score": score,
        "summary": summary,
        "checks": checks,
        "metrics": {
            "recipient_count": len(recipients),
            "link_count": len(links),
            "image_count": image_count,
            "subject_length": len(subject),
            "text_length": len(text_body),
        },
    }
