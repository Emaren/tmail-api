from __future__ import annotations

from typing import Any

from tmail_api.db import get_connection, make_id, utc_now


class CampaignRepository:
    VALID_STATUSES = {'draft', 'ready', 'scheduled', 'live', 'paused'}

    def list(self) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                '''
                SELECT campaigns.*, identities.email_address AS identity_email,
                       identities.label AS identity_label,
                       templates.name AS template_name
                FROM campaigns
                JOIN identities ON identities.id = campaigns.identity_id
                LEFT JOIN templates ON templates.id = campaigns.template_id
                ORDER BY campaigns.updated_at DESC, campaigns.created_at DESC
                '''
            ).fetchall()
            return [self._row_to_dict(row, conn=conn) for row in rows]

    def get(self, campaign_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute(
                '''
                SELECT campaigns.*, identities.email_address AS identity_email,
                       identities.label AS identity_label,
                       templates.name AS template_name
                FROM campaigns
                JOIN identities ON identities.id = campaigns.identity_id
                LEFT JOIN templates ON templates.id = campaigns.template_id
                WHERE campaigns.id = ?
                ''',
                (campaign_id,),
            ).fetchone()
            return self._row_to_dict(row, conn=conn) if row else None

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload.get('name') or '').strip()
        objective = str(payload.get('objective') or '').strip()
        identity_id = str(payload.get('identity_id') or '').strip()
        audience_label = str(payload.get('audience_label') or '').strip()
        status = str(payload.get('status') or 'draft').strip().lower()
        if not name or not objective or not identity_id or not audience_label:
            raise ValueError('Name, objective, identity, and audience are required.')
        if status not in self.VALID_STATUSES:
            raise ValueError('Invalid campaign status.')

        campaign_id = str(payload.get('id') or make_id('campaign'))
        now = utc_now()
        existing = self.get(campaign_id) if payload.get('id') else None
        row = {
            'id': campaign_id,
            'name': name,
            'objective': objective,
            'status': status,
            'identity_id': identity_id,
            'template_id': str(payload.get('template_id') or '').strip() or None,
            'audience_label': audience_label,
            'send_window': str(payload.get('send_window') or '').strip(),
            'notes': str(payload.get('notes') or '').strip(),
            'scheduled_for': str(payload.get('scheduled_for') or '').strip() or None,
            'created_at': existing['created_at'] if existing else now,
            'updated_at': now,
        }
        with get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO campaigns (
                    id, name, objective, status, identity_id, template_id,
                    audience_label, send_window, notes, scheduled_for,
                    created_at, updated_at
                ) VALUES (
                    :id, :name, :objective, :status, :identity_id, :template_id,
                    :audience_label, :send_window, :notes, :scheduled_for,
                    :created_at, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    objective = excluded.objective,
                    status = excluded.status,
                    identity_id = excluded.identity_id,
                    template_id = excluded.template_id,
                    audience_label = excluded.audience_label,
                    send_window = excluded.send_window,
                    notes = excluded.notes,
                    scheduled_for = excluded.scheduled_for,
                    updated_at = excluded.updated_at
                ''',
                row,
            )
        return self.get(campaign_id)  # type: ignore[return-value]

    def _row_to_dict(self, row: Any, *, conn: Any) -> dict[str, Any]:
        counts = conn.execute(
            '''
            SELECT
                COUNT(*) AS total_messages,
                SUM(CASE WHEN status = 'Sent' THEN 1 ELSE 0 END) AS sent_messages
            FROM messages
            WHERE campaign_id = ?
            ''',
            (row['id'],),
        ).fetchone()
        event_counts = conn.execute(
            '''
            SELECT events.event_type, COUNT(*) AS total
            FROM events
            JOIN messages ON messages.id = events.message_id
            WHERE messages.campaign_id = ?
            GROUP BY events.event_type
            ''',
            (row['id'],),
        ).fetchall()
        counts_by_type = {item['event_type']: item['total'] for item in event_counts}
        return {
            'id': row['id'],
            'name': row['name'],
            'objective': row['objective'],
            'status': row['status'],
            'identity_id': row['identity_id'],
            'identity': row['identity_email'],
            'identity_label': row['identity_label'],
            'template_id': row['template_id'],
            'template_name': row['template_name'],
            'audience_label': row['audience_label'],
            'send_window': row['send_window'] or '',
            'notes': row['notes'] or '',
            'scheduled_for': row['scheduled_for'],
            'message_count': counts['total_messages'] or 0,
            'sent_count': counts['sent_messages'] or 0,
            'open_events': counts_by_type.get('opened', 0),
            'click_events': counts_by_type.get('clicked', 0),
            'reply_events': counts_by_type.get('replied', 0) + counts_by_type.get('replied_manual', 0),
            'conversion_events': counts_by_type.get('meeting_booked', 0) + counts_by_type.get('converted', 0),
            'created_at': row['created_at'],
            'updated_at': row['updated_at'],
        }
