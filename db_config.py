"""Single-row app configuration."""

from __future__ import annotations

from typing import Any

import aiosqlite

from db_json import dump_json, parse_dict, parse_list
from db_paths import DB_PATH, DEFAULT_CONFIG, DEFAULT_SOURCES


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
        "browser_cdp_url": cdp,
    }


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
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE config SET
                roles = ?, locations = ?, experience = ?, email_address = ?,
                email_app_password = ?, schedule_hours = ?, sources = ?,
                career_pages = ?, custom_sites = ?, llm_provider = ?,
                llm_api_key = ?, resume_path = ?, browser_cdp_url = ?
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
                str(payload.get("browser_cdp_url") or "").strip(),
            ),
        )
        await db.commit()
    return await get_config()
