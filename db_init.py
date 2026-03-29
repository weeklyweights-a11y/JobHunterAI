"""Schema creation and connection health."""

from __future__ import annotations

import logging

import aiosqlite

from db_json import dump_json
from db_paths import DB_PATH, DEFAULT_CONFIG, ensure_data_dir

logger = logging.getLogger(__name__)


async def check_db_connection() -> bool:
    try:
        ensure_data_dir()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("SELECT 1")
            await db.commit()
        return True
    except Exception as exc:
        logger.exception("Database connection check failed: %s", exc)
        return False


async def init_db() -> None:
    ensure_data_dir()
    d = DEFAULT_CONFIG
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                company TEXT,
                url TEXT NOT NULL UNIQUE,
                source TEXT,
                found_at TEXT NOT NULL DEFAULT (datetime('now')),
                search_role TEXT,
                search_location TEXT
            );
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY,
                roles TEXT NOT NULL,
                locations TEXT NOT NULL,
                experience TEXT NOT NULL,
                email_address TEXT NOT NULL,
                email_app_password TEXT NOT NULL,
                schedule_hours INTEGER NOT NULL DEFAULT 4,
                sources TEXT NOT NULL,
                career_pages TEXT NOT NULL,
                custom_sites TEXT NOT NULL,
                llm_provider TEXT NOT NULL,
                llm_api_key TEXT NOT NULL,
                resume_path TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                jobs_found INTEGER NOT NULL DEFAULT 0,
                errors TEXT
            );
            """
        )
        cur = await db.execute("SELECT COUNT(*) FROM config WHERE id = 1")
        row = await cur.fetchone()
        if row and row[0] == 0:
            await db.execute(
                """INSERT INTO config (
                    id, roles, locations, experience, email_address, email_app_password,
                    schedule_hours, sources, career_pages, custom_sites,
                    llm_provider, llm_api_key, resume_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    1,
                    dump_json(d["roles"]),
                    dump_json(d["locations"]),
                    d["experience"],
                    d["email_address"],
                    d["email_app_password"],
                    d["schedule_hours"],
                    dump_json(d["sources"]),
                    dump_json(d["career_pages"]),
                    dump_json(d["custom_sites"]),
                    d["llm_provider"],
                    d["llm_api_key"],
                    d["resume_path"],
                ),
            )
        cur = await db.execute("PRAGMA table_info(runs)")
        run_cols = {row[1] for row in await cur.fetchall()}
        if "duration_sec" not in run_cols:
            await db.execute("ALTER TABLE runs ADD COLUMN duration_sec REAL")
        await db.commit()
