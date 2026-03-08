from __future__ import annotations

from dataclasses import asdict
from typing import Any

from tmail_api.db import utc_now
from tmail_api.instrumentation import InstrumentationService
from tmail_api.mail import AppleSMTPProvider, MailProviderError
from tmail_api.repositories import IdentityRepository, MessageRepository, SeedInboxRepository, SeedTestRepository


class MailWorkflowService:
    def __init__(self) -> None:
        self.identities = IdentityRepository()
        self.messages = MessageRepository()
        self.instrumentation = InstrumentationService()
        self.provider = AppleSMTPProvider()

    def save_or_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        identity = self.identities.get(payload["identity_id"])
        if not identity:
            raise ValueError("Unknown identity")

        recipients = [recipient.strip() for recipient in payload.get("recipients", []) if recipient.strip()]
        if not payload.get("subject"):
            raise ValueError("Subject is required")
        if not payload.get("html_body"):
            raise ValueError("HTML body is required")
        if payload.get("action") in {"send_test", "send_live"} and not recipients:
            raise ValueError("At least one recipient is required for sending")

        preview = self.instrumentation.preview_text(payload.get("text_body", ""), payload["html_body"])
        initial_status = "Draft" if payload.get("action") == "draft" else "Queued"
        send_mode = "test" if payload.get("action") == "send_test" else "live" if payload.get("action") == "send_live" else "draft"

        message = self.messages.create(
            {
                "identity_id": identity["id"],
                "template_id": payload.get("template_id"),
                "campaign_id": payload.get("campaign_id"),
                "subject": payload["subject"],
                "preheader": payload.get("preheader", ""),
                "html_body": payload["html_body"],
                "text_body": payload.get("text_body", ""),
                "recipients": recipients,
                "status": initial_status,
                "send_mode": send_mode,
                "tracking_enabled": payload.get("tracking_enabled", True),
                "pixel_enabled": payload.get("pixel_enabled", True),
                "preview": preview,
            }
        )
        self.messages.add_event(message["id"], "draft_saved", {"mode": send_mode, "recipient_count": len(recipients)})

        if payload.get("action") == "draft":
            return self.messages.get(message["id"])  # type: ignore[return-value]

        instrumented = self.instrumentation.instrument(
            message_id=message["id"],
            html_body=payload["html_body"],
            text_body=payload.get("text_body", ""),
            pixel_enabled=bool(payload.get("pixel_enabled", True)),
        )
        self.messages.add_event(
            message["id"],
            "instrumented",
            {
                "tracked_link_count": len(instrumented.tracked_links),
                "pixel_url": instrumented.pixel_url,
            },
        )
        self.messages.update_content(
            message["id"],
            html_body=instrumented.html_body,
            text_body=instrumented.text_body,
            preview=self.instrumentation.preview_text(instrumented.text_body, instrumented.html_body),
        )

        try:
            result = self.provider.send_message(
                identity=identity,
                recipients=recipients,
                subject=payload["subject"],
                preheader=payload.get("preheader", ""),
                html_body=instrumented.html_body,
                text_body=instrumented.text_body,
            )
        except MailProviderError as exc:
            self.messages.update_status(message["id"], status="Needs Review", error_message=str(exc))
            self.messages.add_event(message["id"], "send_failed", {"error": str(exc), "mode": send_mode})
            return self.messages.get(message["id"])  # type: ignore[return-value]

        sent_at = utc_now()
        self.messages.update_status(
            message["id"],
            status="Sent",
            sent_at=sent_at,
            error_message=None,
            provider_message_id=result.provider_message_id,
        )
        self.messages.add_event(message["id"], "sent", {"mode": send_mode, **asdict(result)})
        return self.messages.get(message["id"])  # type: ignore[return-value]

    def test_identity_connection(self, identity_id: str) -> dict[str, Any]:
        identity = self.identities.get(identity_id)
        if not identity:
            raise ValueError("Unknown identity")
        try:
            result = self.provider.test_connection(identity)
            return {"identity_id": identity_id, "ok": True, **asdict(result)}
        except MailProviderError as exc:
            return {"identity_id": identity_id, "ok": False, "status": "error", "detail": str(exc)}


class SeedLabService:
    def __init__(self) -> None:
        self.identities = IdentityRepository()
        self.seed_inboxes = SeedInboxRepository()
        self.seed_runs = SeedTestRepository()
        self.mail = MailWorkflowService()

    def launch_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        identity_id = str(payload.get("identity_id") or "")
        if not identity_id:
            raise ValueError("Identity is required")
        if not payload.get("subject"):
            raise ValueError("Subject is required")
        if not payload.get("html_body"):
            raise ValueError("HTML body is required")

        enabled_seed_inboxes = self.seed_inboxes.enabled()
        if not enabled_seed_inboxes:
            raise ValueError("Configure and enable at least one seed inbox before launching a run.")

        recipients = [seed["email_address"] for seed in enabled_seed_inboxes if seed.get("email_address")]
        if not recipients:
            raise ValueError("Enabled seed inboxes are missing email addresses.")

        message = self.mail.save_or_send(
            {
                "identity_id": identity_id,
                "recipients": recipients,
                "subject": payload["subject"],
                "preheader": payload.get("preheader", ""),
                "html_body": payload["html_body"],
                "text_body": payload.get("text_body", ""),
                "tracking_enabled": payload.get("tracking_enabled", True),
                "pixel_enabled": payload.get("pixel_enabled", True),
                "action": "send_test",
            }
        )

        status = "sent" if message.get("status") == "Sent" else "needs_review"
        summary = (
            f"Seed run sent to {len(recipients)} inboxes. Record placement after checking the real inboxes."
            if status == "sent"
            else message.get("error_message") or "Seed run send needs review."
        )

        return self.seed_runs.create_run(
            identity_id=identity_id,
            message_id=message.get("id"),
            template_id=payload.get("template_id"),
            subject=str(payload["subject"]),
            status=status,
            summary=summary,
            sent_at=message.get("sent_at"),
            seed_inboxes=enabled_seed_inboxes,
        )

    def record_results(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        results = payload.get("results") or []
        if not isinstance(results, list):
            raise ValueError("Results payload must be a list.")
        updated = self.seed_runs.update_results(run_id, results)
        if not updated:
            raise ValueError("Seed run not found")
        return updated
