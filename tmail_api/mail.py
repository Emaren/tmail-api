from __future__ import annotations

import os
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Any


class MailProviderError(RuntimeError):
    pass


@dataclass
class SendResult:
    status: str
    provider_message_id: str | None = None
    detail: str | None = None


class AppleSMTPProvider:
    def test_connection(self, identity: dict[str, Any]) -> SendResult:
        secret = os.getenv(identity["smtp_secret_env"])
        if not secret:
            raise MailProviderError(f"Missing secret env: {identity['smtp_secret_env']}")

        try:
            with smtplib.SMTP(identity["smtp_host"], int(identity["smtp_port"]), timeout=12) as server:
                if identity.get("use_tls", True):
                    server.starttls()
                server.login(identity["smtp_username"], secret)
        except Exception as exc:  # pragma: no cover - network dependent
            raise MailProviderError(str(exc)) from exc

        return SendResult(status="ok", detail="Connection verified")

    def send_message(
        self,
        *,
        identity: dict[str, Any],
        recipients: list[str],
        subject: str,
        preheader: str,
        html_body: str,
        text_body: str,
    ) -> SendResult:
        secret = os.getenv(identity["smtp_secret_env"])
        if not secret:
            raise MailProviderError(f"Missing secret env: {identity['smtp_secret_env']}")

        msg = MIMEMultipart("alternative")
        msg["From"] = formataddr((identity["display_name"], identity["email_address"]))
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        if identity.get("reply_to"):
            msg["Reply-To"] = identity["reply_to"]
        if preheader:
            msg["X-TMail-Preheader"] = preheader

        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        try:
            with smtplib.SMTP(identity["smtp_host"], int(identity["smtp_port"]), timeout=20) as server:
                if identity.get("use_tls", True):
                    server.starttls()
                server.login(identity["smtp_username"], secret)
                server.sendmail(identity["email_address"], recipients, msg.as_string())
        except Exception as exc:  # pragma: no cover - network dependent
            raise MailProviderError(str(exc)) from exc

        return SendResult(status="sent", detail="Message accepted by SMTP relay")
