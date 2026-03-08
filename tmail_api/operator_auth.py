from __future__ import annotations

from typing import Any

from tmail_api.db import get_connection, make_id, utc_now
from tmail_api.security import build_otpauth_uri, generate_totp_secret, hash_password, verify_password, verify_totp


class OperatorRepository:
    def list(self) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                'SELECT * FROM operators ORDER BY created_at ASC, username ASC'
            ).fetchall()
        return [self._row_to_public(row) for row in rows]

    def get(self, operator_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute('SELECT * FROM operators WHERE id = ?', (operator_id,)).fetchone()
        return self._row_to_public(row) if row else None

    def get_by_username(self, username: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            row = conn.execute('SELECT * FROM operators WHERE username = ?', (username,)).fetchone()
        return self._row_to_public(row) if row else None

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        username = str(payload.get('username') or '').strip().lower()
        display_name = str(payload.get('display_name') or '').strip()
        password = str(payload.get('password') or '')
        role = str(payload.get('role') or 'admin').strip().lower() or 'admin'
        if not username or not display_name or not password:
            raise ValueError('Username, display name, and password are required.')
        if role not in {'owner', 'admin'}:
            raise ValueError('Role must be owner or admin.')

        operator_id = str(payload.get('id') or make_id('operator'))
        now = utc_now()
        with get_connection() as conn:
            existing = conn.execute('SELECT 1 FROM operators WHERE username = ? AND id != ?', (username, operator_id)).fetchone()
            if existing:
                raise ValueError('Username is already in use.')
            conn.execute(
                '''
                INSERT INTO operators (
                    id, username, display_name, role, password_hash, is_active,
                    totp_secret, pending_totp_secret, totp_enabled, last_login_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    operator_id,
                    username,
                    display_name,
                    role,
                    hash_password(password),
                    1,
                    None,
                    None,
                    0,
                    None,
                    now,
                    now,
                ),
            )
        return self.get(operator_id)  # type: ignore[return-value]

    def authenticate(self, username: str, password: str, totp_code: str | None = None) -> dict[str, Any]:
        with get_connection() as conn:
            row = conn.execute('SELECT * FROM operators WHERE username = ?', (username.strip().lower(),)).fetchone()
            if not row or not bool(row['is_active']):
                raise ValueError('Invalid credentials.')
            if not verify_password(password, row['password_hash']):
                raise ValueError('Invalid credentials.')
            if bool(row['totp_enabled']):
                if not totp_code or not verify_totp(row['totp_secret'], totp_code):
                    raise ValueError('A valid TOTP code is required.')
            conn.execute('UPDATE operators SET last_login_at = ?, updated_at = ? WHERE id = ?', (utc_now(), utc_now(), row['id']))
        return self.get(str(row['id']))  # type: ignore[return-value]

    def change_password(self, operator_id: str, current_password: str, new_password: str, *, totp_code: str | None = None) -> dict[str, Any]:
        if not new_password.strip() or len(new_password) < 10:
            raise ValueError('New password must be at least 10 characters.')

        with get_connection() as conn:
            row = conn.execute('SELECT * FROM operators WHERE id = ?', (operator_id,)).fetchone()
            if not row:
                raise ValueError('Operator not found.')
            if not verify_password(current_password, row['password_hash']):
                raise ValueError('Current password is incorrect.')
            if bool(row['totp_enabled']):
                if not totp_code or not verify_totp(row['totp_secret'], totp_code):
                    raise ValueError('A valid TOTP code is required.')
            conn.execute(
                'UPDATE operators SET password_hash = ?, updated_at = ? WHERE id = ?',
                (hash_password(new_password), utc_now(), operator_id),
            )
        return self.get(operator_id)  # type: ignore[return-value]

    def start_totp_setup(self, operator_id: str) -> dict[str, Any]:
        with get_connection() as conn:
            row = conn.execute('SELECT * FROM operators WHERE id = ?', (operator_id,)).fetchone()
            if not row:
                raise ValueError('Operator not found.')
            secret = generate_totp_secret()
            conn.execute(
                'UPDATE operators SET pending_totp_secret = ?, updated_at = ? WHERE id = ?',
                (secret, utc_now(), operator_id),
            )
        return {
            'secret': secret,
            'otpauth_uri': build_otpauth_uri(secret=secret, username=str(row['username'])),
        }

    def enable_totp(self, operator_id: str, code: str) -> dict[str, Any]:
        with get_connection() as conn:
            row = conn.execute('SELECT * FROM operators WHERE id = ?', (operator_id,)).fetchone()
            if not row:
                raise ValueError('Operator not found.')
            pending_secret = row['pending_totp_secret']
            if not pending_secret:
                raise ValueError('Start TOTP setup before confirming it.')
            if not verify_totp(pending_secret, code):
                raise ValueError('The TOTP code did not verify.')
            conn.execute(
                '''
                UPDATE operators
                SET totp_secret = ?, pending_totp_secret = NULL, totp_enabled = 1, updated_at = ?
                WHERE id = ?
                ''',
                (pending_secret, utc_now(), operator_id),
            )
        return self.get(operator_id)  # type: ignore[return-value]

    def disable_totp(self, operator_id: str, password: str, totp_code: str) -> dict[str, Any]:
        with get_connection() as conn:
            row = conn.execute('SELECT * FROM operators WHERE id = ?', (operator_id,)).fetchone()
            if not row:
                raise ValueError('Operator not found.')
            if not verify_password(password, row['password_hash']):
                raise ValueError('Current password is incorrect.')
            if bool(row['totp_enabled']) and not verify_totp(row['totp_secret'], totp_code):
                raise ValueError('A valid TOTP code is required.')
            conn.execute(
                '''
                UPDATE operators
                SET totp_secret = NULL, pending_totp_secret = NULL, totp_enabled = 0, updated_at = ?
                WHERE id = ?
                ''',
                (utc_now(), operator_id),
            )
        return self.get(operator_id)  # type: ignore[return-value]

    def _row_to_public(self, row: Any) -> dict[str, Any]:
        return {
            'id': row['id'],
            'username': row['username'],
            'display_name': row['display_name'],
            'role': row['role'],
            'is_active': bool(row['is_active']),
            'totp_enabled': bool(row['totp_enabled']),
            'totp_pending': bool(row['pending_totp_secret']),
            'last_login_at': row['last_login_at'],
            'created_at': row['created_at'],
            'updated_at': row['updated_at'],
        }
