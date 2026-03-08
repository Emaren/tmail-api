from __future__ import annotations

from typing import Any

from tmail_api.db import get_connection


def build_analytics_summary() -> dict[str, Any]:
    with get_connection() as conn:
        overview_row = conn.execute(
            '''
            SELECT
                COUNT(*) AS total_messages,
                SUM(CASE WHEN status = 'Sent' THEN 1 ELSE 0 END) AS sent_messages,
                SUM(CASE WHEN status = 'Draft' THEN 1 ELSE 0 END) AS draft_messages,
                SUM(CASE WHEN status = 'Needs Review' THEN 1 ELSE 0 END) AS review_messages
            FROM messages
            '''
        ).fetchone()
        event_rows = conn.execute(
            '''
            SELECT event_type, COUNT(*) AS total
            FROM events
            GROUP BY event_type
            '''
        ).fetchall()
        event_counts = {row['event_type']: row['total'] for row in event_rows}

        identity_rows = conn.execute(
            '''
            SELECT id, label, email_address
            FROM identities
            ORDER BY label ASC
            '''
        ).fetchall()
        identity_message_rows = conn.execute(
            '''
            SELECT identity_id,
                   COUNT(*) AS total_messages,
                   SUM(CASE WHEN status = 'Sent' THEN 1 ELSE 0 END) AS sent_messages
            FROM messages
            GROUP BY identity_id
            '''
        ).fetchall()
        identity_event_rows = conn.execute(
            '''
            SELECT messages.identity_id AS identity_id, events.event_type, COUNT(*) AS total
            FROM events
            JOIN messages ON messages.id = events.message_id
            GROUP BY messages.identity_id, events.event_type
            '''
        ).fetchall()

        template_rows = conn.execute(
            '''
            SELECT id, name, category
            FROM templates
            ORDER BY updated_at DESC, name ASC
            '''
        ).fetchall()
        template_message_rows = conn.execute(
            '''
            SELECT template_id,
                   COUNT(*) AS total_messages,
                   SUM(CASE WHEN status = 'Sent' THEN 1 ELSE 0 END) AS sent_messages
            FROM messages
            WHERE template_id IS NOT NULL AND template_id != ''
            GROUP BY template_id
            '''
        ).fetchall()
        template_event_rows = conn.execute(
            '''
            SELECT messages.template_id AS template_id, events.event_type, COUNT(*) AS total
            FROM events
            JOIN messages ON messages.id = events.message_id
            WHERE messages.template_id IS NOT NULL AND messages.template_id != ''
            GROUP BY messages.template_id, events.event_type
            '''
        ).fetchall()

        seed_rows = conn.execute(
            '''
            SELECT id, subject, status, overall_score, acceptance_score, placement_score, render_score, sent_at, updated_at
            FROM (
                SELECT seed_test_runs.id,
                       seed_test_runs.subject,
                       seed_test_runs.status,
                       seed_test_runs.sent_at,
                       seed_test_runs.updated_at,
                       ROUND(
                           (
                               COALESCE((
                                   SELECT AVG(CASE seed_test_results.accepted WHEN 1 THEN 100 WHEN 0 THEN 0 END)
                                   FROM seed_test_results
                                   WHERE seed_test_results.run_id = seed_test_runs.id AND seed_test_results.accepted IS NOT NULL
                               ), 0) * 0.35
                               +
                               COALESCE((
                                   SELECT AVG(CASE seed_test_results.placement
                                       WHEN 'inbox' THEN 100
                                       WHEN 'promotions' THEN 72
                                       WHEN 'spam' THEN 12
                                       WHEN 'missing' THEN 0
                                   END)
                                   FROM seed_test_results
                                   WHERE seed_test_results.run_id = seed_test_runs.id AND seed_test_results.placement IN ('inbox','promotions','spam','missing')
                               ), 0) * 0.45
                               +
                               COALESCE((
                                   SELECT AVG(CASE seed_test_results.render_status
                                       WHEN 'clean' THEN 100
                                       WHEN 'issues' THEN 45
                                   END)
                                   FROM seed_test_results
                                   WHERE seed_test_results.run_id = seed_test_runs.id AND seed_test_results.render_status IN ('clean','issues')
                               ), 0) * 0.20
                           )
                       ) AS overall_score,
                       COALESCE((
                           SELECT ROUND(AVG(CASE seed_test_results.accepted WHEN 1 THEN 100 WHEN 0 THEN 0 END))
                           FROM seed_test_results
                           WHERE seed_test_results.run_id = seed_test_runs.id AND seed_test_results.accepted IS NOT NULL
                       ), 0) AS acceptance_score,
                       COALESCE((
                           SELECT ROUND(AVG(CASE seed_test_results.placement
                               WHEN 'inbox' THEN 100
                               WHEN 'promotions' THEN 72
                               WHEN 'spam' THEN 12
                               WHEN 'missing' THEN 0
                           END))
                           FROM seed_test_results
                           WHERE seed_test_results.run_id = seed_test_runs.id AND seed_test_results.placement IN ('inbox','promotions','spam','missing')
                       ), 0) AS placement_score,
                       COALESCE((
                           SELECT ROUND(AVG(CASE seed_test_results.render_status
                               WHEN 'clean' THEN 100
                               WHEN 'issues' THEN 45
                           END))
                           FROM seed_test_results
                           WHERE seed_test_results.run_id = seed_test_runs.id AND seed_test_results.render_status IN ('clean','issues')
                       ), 0) AS render_score
                FROM seed_test_runs
            )
            ORDER BY COALESCE(sent_at, updated_at) DESC
            LIMIT 6
            '''
        ).fetchall()

        campaign_rows = conn.execute(
            '''
            SELECT campaigns.id, campaigns.name, campaigns.status, campaigns.objective,
                   identities.email_address AS identity,
                   templates.name AS template_name,
                   campaigns.updated_at
            FROM campaigns
            JOIN identities ON identities.id = campaigns.identity_id
            LEFT JOIN templates ON templates.id = campaigns.template_id
            ORDER BY campaigns.updated_at DESC
            LIMIT 6
            '''
        ).fetchall()

        contact_count_row = conn.execute(
            "SELECT COUNT(*) AS total FROM contacts"
        ).fetchone()

        top_contact_rows = conn.execute(
            '''
            SELECT
                contacts.id,
                contacts.email_address,
                contacts.display_name,
                COUNT(message_contacts.id) AS message_count,
                SUM(CASE WHEN message_contacts.delivery_status = 'sent' THEN 1 ELSE 0 END) AS sent_count,
                SUM(COALESCE(message_contacts.inferred_open_count, 0)) AS open_count,
                SUM(COALESCE(message_contacts.inferred_click_count, 0)) AS click_count,
                SUM(CASE WHEN COALESCE(message_contacts.reply_state, '') != '' THEN 1 ELSE 0 END) AS reply_count,
                SUM(CASE WHEN COALESCE(message_contacts.conversion_state, '') != '' THEN 1 ELSE 0 END) AS conversion_count
            FROM contacts
            LEFT JOIN message_contacts ON message_contacts.contact_id = contacts.id
            GROUP BY contacts.id
            ORDER BY (
                SUM(CASE WHEN COALESCE(message_contacts.conversion_state, '') != '' THEN 1 ELSE 0 END) * 12 +
                SUM(CASE WHEN COALESCE(message_contacts.reply_state, '') != '' THEN 1 ELSE 0 END) * 8 +
                SUM(COALESCE(message_contacts.inferred_click_count, 0)) * 4 +
                SUM(COALESCE(message_contacts.inferred_open_count, 0))
            ) DESC, contacts.email_address ASC
            LIMIT 8
            '''
        ).fetchall()

    identity_message_map = {row['identity_id']: dict(row) for row in identity_message_rows}
    identity_event_map: dict[str, dict[str, int]] = {}
    for row in identity_event_rows:
        identity_event_map.setdefault(row['identity_id'], {})[row['event_type']] = row['total']

    template_message_map = {row['template_id']: dict(row) for row in template_message_rows}
    template_event_map: dict[str, dict[str, int]] = {}
    for row in template_event_rows:
        template_event_map.setdefault(row['template_id'], {})[row['event_type']] = row['total']

    identity_performance = []
    for row in identity_rows:
        message_meta = identity_message_map.get(row['id'], {})
        event_meta = identity_event_map.get(row['id'], {})
        sent_count = int(message_meta.get('sent_messages') or 0)
        click_events = int(event_meta.get('clicked') or 0)
        reply_events = int(event_meta.get('replied') or 0) + int(event_meta.get('replied_manual') or 0)
        engagement_score = (click_events * 4) + (reply_events * 9) + int(event_meta.get('opened') or 0)
        identity_performance.append(
            {
                'id': row['id'],
                'label': row['label'],
                'address': row['email_address'],
                'total_messages': int(message_meta.get('total_messages') or 0),
                'sent_count': sent_count,
                'open_events': int(event_meta.get('opened') or 0),
                'click_events': click_events,
                'reply_events': reply_events,
                'engagement_score': engagement_score,
            }
        )

    template_performance = []
    for row in template_rows:
        message_meta = template_message_map.get(row['id'], {})
        event_meta = template_event_map.get(row['id'], {})
        template_performance.append(
            {
                'id': row['id'],
                'name': row['name'],
                'category': row['category'],
                'total_messages': int(message_meta.get('total_messages') or 0),
                'sent_count': int(message_meta.get('sent_messages') or 0),
                'open_events': int(event_meta.get('opened') or 0),
                'click_events': int(event_meta.get('clicked') or 0),
                'reply_events': int(event_meta.get('replied') or 0) + int(event_meta.get('replied_manual') or 0),
            }
        )

    return {
        'overview': {
            'total_messages': int(overview_row['total_messages'] or 0),
            'sent_messages': int(overview_row['sent_messages'] or 0),
            'draft_messages': int(overview_row['draft_messages'] or 0),
            'review_messages': int(overview_row['review_messages'] or 0),
            'contacts_count': int(contact_count_row['total'] or 0),
            'open_events': int(event_counts.get('opened', 0)),
            'click_events': int(event_counts.get('clicked', 0)),
            'reply_events': int(event_counts.get('replied', 0)) + int(event_counts.get('replied_manual', 0)),
            'conversion_events': int(event_counts.get('meeting_booked', 0)) + int(event_counts.get('converted', 0)),
            'seed_average_score': round(sum(int(row['overall_score'] or 0) for row in seed_rows) / len(seed_rows)) if seed_rows else 0,
            'active_campaigns': len([row for row in campaign_rows if row['status'] in {'ready', 'scheduled', 'live'}]),
        },
        'identity_performance': identity_performance,
        'template_performance': template_performance,
        'seed_runs': [dict(row) for row in seed_rows],
        'campaigns': [dict(row) for row in campaign_rows],
        'top_contacts': [dict(row) for row in top_contact_rows],
    }
