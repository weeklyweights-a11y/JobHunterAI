"""Database paths and default config values."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent
DATA_DIR = _ROOT / "data"
DB_PATH = DATA_DIR / "jobs.db"

DEFAULT_SOURCES: dict[str, bool] = {
    "linkedin": True,
    "indeed": True,
    "yc": True,
    "career_page": True,
}

DEFAULT_CONFIG: dict[str, Any] = {
    "id": 1,
    "roles": [],
    "locations": [],
    "experience": "any",
    "email_address": "",
    "email_app_password": "",
    "schedule_hours": 4,
    "sources": dict(DEFAULT_SOURCES),
    "career_pages": [],
    "custom_sites": [],
    "llm_provider": "gemini",
    "llm_api_key": "",
    "resume_path": "",
    "browser_cdp_url": "",
}


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
