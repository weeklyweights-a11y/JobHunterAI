"""Single-row app configuration."""

from __future__ import annotations

from typing import Any

import aiosqlite

from db_json import dump_json, parse_dict, parse_list
from db_paths import (
    DB_PATH,
    DEFAULT_CONFIG,
    DEFAULT_SOURCES,
    normalize_linkedin_employment_types,
)


def _normalize_sources(raw: dict[str, Any]) -> dict[str, bool]:
    merged = {k: bool(v) for k, v in DEFAULT_SOURCES.items()}
    for key, val in raw.items():
        if key in merged:
            merged[key] = bool(val)
    return merged


def _row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
    keys = row.keys()
    cdp = ""
    if "browser_cdp_url" in keys and row["browser_cdp_url"] is not None:
        cdp = str(row["browser_cdp_url"])
    dedup = 7
    if "dedup_days" in keys and row["dedup_days"] is not None:
        try:
            dedup = int(row["dedup_days"])
        except (TypeError, ValueError):
            dedup = 7
    return {
        "id": row["id"],
        "roles": parse_list(row["roles"]),
        "locations": parse_list(row["locations"]),
        "experience": row["experience"],
        "email_address": row["email_address"],
        "email_app_password": row["email_app_password"],
        "schedule_hours": row["schedule_hours"],
        "sources": _normalize_sources(parse_dict(row["sources"])),
        "career_pages": parse_list(row["career_pages"]),
        "custom_sites": parse_list(row["custom_sites"]),
        "llm_provider": row["llm_provider"],
        "llm_api_key": row["llm_api_key"],
        "resume_path": row["resume_path"],
        "dedup_days": dedup,
        "browser_cdp_url": cdp,
        "auto_run_enabled": bool(row["auto_run_enabled"]) if "auto_run_enabled" in keys else True,
        "linkedin_email": str(row["linkedin_email"]) if "linkedin_email" in keys else "",
        "linkedin_password": str(row["linkedin_password"])
        if "linkedin_password" in keys
        else "",
        "linkedin_include_easy_apply": bool(row["linkedin_include_easy_apply"])
        if "linkedin_include_easy_apply" in keys
        else bool(row["linkedin_easy_apply_only"])
        if "linkedin_easy_apply_only" in keys
        else False,
        "linkedin_posted_past_week": bool(row["linkedin_posted_past_week"])
        if "linkedin_posted_past_week" in keys
        else False,
        "linkedin_include_reposts": bool(row["linkedin_include_reposts"])
        if "linkedin_include_reposts" in keys
        else False,
        "ats_platforms": parse_dict(row["ats_platforms"])
        if "ats_platforms" in keys
        else dict(DEFAULT_CONFIG.get("ats_platforms") or {}),
        "linkedin_employment_types": normalize_linkedin_employment_types(
            parse_list(row["linkedin_employment_types"])
            if "linkedin_employment_types" in keys
            else DEFAULT_CONFIG.get("linkedin_employment_types")
        ),
        "filter_jobs_by_relevance_llm": bool(row["filter_jobs_by_relevance_llm"])
        if "filter_jobs_by_relevance_llm" in keys
        else bool(DEFAULT_CONFIG.get("filter_jobs_by_relevance_llm", True)),
        "ats_posted_within_days": _int_cfg(
            row, keys, "ats_posted_within_days", DEFAULT_CONFIG.get("ats_posted_within_days", 7)
        ),
        "ats_google_max_serp_pages": max(
            1,
            min(
                _int_cfg(
                    row,
                    keys,
                    "ats_google_max_serp_pages",
                    DEFAULT_CONFIG.get("ats_google_max_serp_pages", 20),
                ),
                50,
            ),
        ),
        "ats_captcha_wait_seconds": max(
            30,
            min(
                _int_cfg(
                    row,
                    keys,
                    "ats_captcha_wait_seconds",
                    DEFAULT_CONFIG.get("ats_captcha_wait_seconds", 180),
                ),
                900,
            ),
        ),
    }


def _int_cfg(
    row: aiosqlite.Row,
    keys: list[str],
    col: str,
    default: int,
) -> int:
    if col not in keys or row[col] is None:
        return int(default)
    try:
        return int(row[col])
    except (TypeError, ValueError):
        return int(default)


async def get_config() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM config WHERE id = 1")
        row = await cur.fetchone()
        if not row:
            raise RuntimeError("Config row missing; init_db was not run")
        return _row_to_dict(row)


async def save_config(data: dict[str, Any]) -> dict[str, Any]:
    payload = {**DEFAULT_CONFIG, **data, "id": 1}
    payload["linkedin_employment_types"] = normalize_linkedin_employment_types(
        payload.get("linkedin_employment_types")
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE config SET
                roles = ?, locations = ?, experience = ?, email_address = ?,
                email_app_password = ?, schedule_hours = ?, sources = ?,
                career_pages = ?, custom_sites = ?, llm_provider = ?,
                llm_api_key = ?, resume_path = ?, dedup_days = ?, browser_cdp_url = ?,
                auto_run_enabled = ?, linkedin_email = ?, linkedin_password = ?,
                linkedin_include_easy_apply = ?, linkedin_posted_past_week = ?,
                linkedin_include_reposts = ?, ats_platforms = ?,
                linkedin_employment_types = ?, filter_jobs_by_relevance_llm = ?,
                ats_posted_within_days = ?, ats_google_max_serp_pages = ?,
                ats_captcha_wait_seconds = ?
            WHERE id = 1""",
            (
                dump_json(payload["roles"]),
                dump_json(payload["locations"]),
                payload["experience"],
                payload["email_address"],
                payload["email_app_password"],
                int(payload["schedule_hours"]),
                dump_json(payload["sources"]),
                dump_json(payload["career_pages"]),
                dump_json(payload["custom_sites"]),
                payload["llm_provider"],
                payload["llm_api_key"],
                payload["resume_path"],
                max(1, min(int(payload.get("dedup_days", 7)), 365)),
                str(payload.get("browser_cdp_url") or "").strip(),
                1 if payload.get("auto_run_enabled", True) else 0,
                str(payload.get("linkedin_email") or "").strip(),
                str(payload.get("linkedin_password") or "").strip(),
                1 if payload.get("linkedin_include_easy_apply") else 0,
                1 if payload.get("linkedin_posted_past_week") else 0,
                1 if payload.get("linkedin_include_reposts") else 0,
                dump_json(payload.get("ats_platforms") or {}),
                dump_json(payload["linkedin_employment_types"]),
                1 if payload.get("filter_jobs_by_relevance_llm", True) else 0,
                max(0, min(int(payload.get("ats_posted_within_days", 7)), 365)),
                max(1, min(int(payload.get("ats_google_max_serp_pages", 20)), 50)),
                max(
                    30,
                    min(int(payload.get("ats_captcha_wait_seconds", 180)), 900),
                ),
            ),
        )
        await db.commit()
    return await get_config()
