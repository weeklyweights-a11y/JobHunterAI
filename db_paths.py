"""Database paths and default config values."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent

# LinkedIn Jobs URL param f_JT: I=Internship, F=Full-time, P=Part-time, C=Contract, V=Volunteer
LINKEDIN_EMPLOYMENT_TYPE_CODES: frozenset[str] = frozenset({"I", "F", "P", "C", "V"})


def normalize_linkedin_employment_types(raw: Any) -> list[str]:
    """Dedupe, filter to allowed codes, preserve order. Default full-time if empty."""
    if raw is None:
        return ["F"]
    if not isinstance(raw, list):
        return ["F"]
    seen: set[str] = set()
    out: list[str] = []
    for x in raw:
        u = str(x).strip().upper()
        if u in LINKEDIN_EMPLOYMENT_TYPE_CODES and u not in seen:
            seen.add(u)
            out.append(u)
    return out if out else ["F"]


DATA_DIR = _ROOT / "data"
DB_PATH = DATA_DIR / "jobs.db"

DEFAULT_SOURCES: dict[str, bool] = {
    "linkedin": True,
    "indeed": True,
    "yc": True,
    "career_page": True,
    "ats": False,
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
    "dedup_days": 7,
    # ATS: drop listings older than N days once posted_time is known; 0 = no age filter
    "ats_posted_within_days": 7,
    # ATS Google: max SERP pages (10 results each) per role×location×platform; early stop if page adds 0 new URLs
    "ats_google_max_serp_pages": 20,
    # When Google shows CAPTCHA, poll the page for this many seconds before stopping ATS Google discovery (partial results kept).
    "ats_captcha_wait_seconds": 180,
    "browser_cdp_url": "",
    "auto_run_enabled": True,
    "linkedin_email": "",
    "linkedin_password": "",
    # True = collect Easy Apply jobs too; False (default) = skip Easy Apply–badged cards only
    "linkedin_include_easy_apply": False,
    # False = LinkedIn search + freshness use last 24 hours; True = past week (7 days)
    "linkedin_posted_past_week": False,
    # True = keep listings LinkedIn marks as reposts; False (default) = skip them
    "linkedin_include_reposts": False,
    "linkedin_employment_types": ["F"],
    "filter_jobs_by_relevance_llm": True,
    "ats_platforms": {
        "greenhouse": True,
        "lever": True,
        "ashby": True,
        "smartrecruiters": True,
        "workable": True,
        "workable_apply": True,
        "workday": True,
        "jobvite": True,
        "bamboohr": True,
        "icims": True,
    },
}


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
