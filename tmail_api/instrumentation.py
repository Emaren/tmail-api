from __future__ import annotations

import html
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from tmail_api.config import get_settings
from tmail_api.repositories import MessageRepository

HREF_RE = re.compile(r'href=("|\')(?P<url>https?://[^"\']+)(\1)', re.IGNORECASE)
TEXT_URL_RE = re.compile(r'https?://[^\s<>"]+')
TAG_RE = re.compile(r'<[^>]+>')


@dataclass
class InstrumentedContent:
    html_body: str
    text_body: str
    tracked_links: list[dict[str, str | None]]
    pixel_url: str | None


class InstrumentationService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.messages = MessageRepository()

    def preview_text(self, text_body: str, html_body: str) -> str:
        base = text_body.strip() or TAG_RE.sub(' ', html_body)
        compact = ' '.join(base.split())
        sanitized = TEXT_URL_RE.sub(self._preview_url_replacement, compact)
        sanitized = ' '.join(sanitized.split())
        return sanitized[:140]

    def _preview_url_replacement(self, match: re.Match[str]) -> str:
        raw_url = match.group(0)
        parsed = urlparse(raw_url)
        host = (parsed.netloc or '').replace('www.', '')
        if not host:
            return 'link'
        if '/api/tracking/click/' in parsed.path:
            return f'link via {host}'
        short_path = parsed.path.rstrip('/')
        if not short_path or short_path == '/':
            return host
        if len(short_path) > 18:
            short_path = f"{short_path[:15]}..."
        return f"{host}{short_path}"

    def instrument(self, *, message_id: str, html_body: str, text_body: str, pixel_enabled: bool) -> InstrumentedContent:
        tracked_links: list[dict[str, str | None]] = []
        replacements: dict[str, str] = {}

        for match in HREF_RE.finditer(html_body):
            url = match.group('url')
            if url in replacements:
                continue
            token = self.messages.create_tracked_link(message_id, url)
            tracked_url = f"{self.settings.public_base_url}/api/tracking/click/{token}"
            replacements[url] = tracked_url
            tracked_links.append({"token": token, "url": url, "tracked_url": tracked_url})

        rendered_html = html_body
        for original, tracked in replacements.items():
            rendered_html = rendered_html.replace(original, tracked)

        rendered_text = text_body
        for original, tracked in replacements.items():
            rendered_text = rendered_text.replace(original, tracked)

        pixel_url = None
        if pixel_enabled:
            pixel_url = f"{self.settings.public_base_url}/track?message_id={message_id}"
            pixel_tag = (
                f'<img src="{html.escape(pixel_url)}" width="1" height="1" '
                'style="display:block;width:1px;height:1px" alt="">'
            )
            if '</body>' in rendered_html.lower():
                closing_index = rendered_html.lower().rfind('</body>')
                rendered_html = rendered_html[:closing_index] + pixel_tag + rendered_html[closing_index:]
            else:
                rendered_html = rendered_html + pixel_tag

        if not rendered_text.strip():
            rendered_text = TAG_RE.sub(' ', rendered_html)
            rendered_text = ' '.join(rendered_text.split())

        return InstrumentedContent(
            html_body=rendered_html,
            text_body=rendered_text,
            tracked_links=tracked_links,
            pixel_url=pixel_url,
        )
