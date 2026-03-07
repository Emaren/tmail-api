from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path
    public_base_url: str
    default_smtp_host: str
    default_smtp_port: int


def get_settings() -> Settings:
    root = Path(__file__).resolve().parents[1]
    db_path = Path(os.getenv("TMAIL_DB_PATH", root / "tmail.db"))
    public_base_url = os.getenv("TMAIL_PUBLIC_BASE_URL", "http://127.0.0.1:8010").rstrip("/")
    default_smtp_host = os.getenv("TMAIL_DEFAULT_SMTP_HOST", "smtp.mail.me.com")
    default_smtp_port = int(os.getenv("TMAIL_DEFAULT_SMTP_PORT", 587))
    return Settings(
        db_path=db_path,
        public_base_url=public_base_url,
        default_smtp_host=default_smtp_host,
        default_smtp_port=default_smtp_port,
    )
