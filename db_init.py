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


async def _migrate_jobs_rolling_window(db: aiosqlite.Connection) -> None:
    """Replace legacy UNIQUE(url) jobs table with rolling-dedup schema."""
    cur = await db.execute("PRAGMA table_info(jobs)")
    cols = {row[1] for row in await cur.fetchall()}
    if "apply_type" in cols:
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_url_found_at ON jobs(url, found_at)"
        )
        return

    logger.info("Migrating jobs table to rolling-window dedup schema (non-unique url).")
    await db.executescript(
        """
        CREATE TABLE jobs_migrate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            company TEXT,
            url TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            apply_type TEXT NOT NULL DEFAULT 'unknown',
            found_at TEXT NOT NULL DEFAULT (datetime('now')),
            search_role TEXT,
            search_location TEXT
        );
        INSERT INTO jobs_migrate (id, title, company, url, source, apply_type, found_at, search_role, search_location)
        SELECT id, title, company, url, COALESCE(source, ''), 'unknown', found_at, search_role, search_location
        FROM jobs;
        DROP TABLE jobs;
        ALTER TABLE jobs_migrate RENAME TO jobs;
        CREATE INDEX IF NOT EXISTS idx_jobs_url_found_at ON jobs(url, found_at);
        """
    )


async def _migrate_jobs_location_job_id(db: aiosqlite.Connection) -> None:
    """Add posting location and LinkedIn job id columns when missing."""
    cur = await db.execute("PRAGMA table_info(jobs)")
    cols = {row[1] for row in await cur.fetchall()}
    if "location" not in cols:
        await db.execute("ALTER TABLE jobs ADD COLUMN location TEXT")
    if "job_id" not in cols:
        await db.execute("ALTER TABLE jobs ADD COLUMN job_id TEXT")


async def _migrate_jobs_linkedin_detail(db: aiosqlite.Connection) -> None:
    """LinkedIn right-panel fields: posted time, freshness label, applicant line."""
    cur = await db.execute("PRAGMA table_info(jobs)")
    cols = {row[1] for row in await cur.fetchall()}
    if "posted_time" not in cols:
        await db.execute("ALTER TABLE jobs ADD COLUMN posted_time TEXT")
    if "freshness" not in cols:
        await db.execute("ALTER TABLE jobs ADD COLUMN freshness TEXT")
    if "applicant_count" not in cols:
        await db.execute("ALTER TABLE jobs ADD COLUMN applicant_count TEXT")


async def _migrate_jobs_description_seniority(db: aiosqlite.Connection) -> None:
    cur = await db.execute("PRAGMA table_info(jobs)")
    cols = {row[1] for row in await cur.fetchall()}
    if "job_description" not in cols:
        await db.execute("ALTER TABLE jobs ADD COLUMN job_description TEXT")
    if "seniority" not in cols:
        await db.execute("ALTER TABLE jobs ADD COLUMN seniority TEXT")


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
                url TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                apply_type TEXT NOT NULL DEFAULT 'unknown',
                found_at TEXT NOT NULL DEFAULT (datetime('now')),
                search_role TEXT,
                search_location TEXT,
                location TEXT,
                job_id TEXT,
                posted_time TEXT,
                freshness TEXT,
                applicant_count TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_url_found_at ON jobs(url, found_at);
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
                resume_path TEXT NOT NULL,
                dedup_days INTEGER NOT NULL DEFAULT 7,
                auto_run_enabled INTEGER NOT NULL DEFAULT 1,
                ats_platforms TEXT NOT NULL DEFAULT '{}'
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
        await _migrate_jobs_rolling_window(db)
        await _migrate_jobs_location_job_id(db)
        await _migrate_jobs_linkedin_detail(db)
        await _migrate_jobs_description_seniority(db)

        cur = await db.execute("SELECT COUNT(*) FROM config WHERE id = 1")
        row = await cur.fetchone()
        if row and row[0] == 0:
            await db.execute(
                """INSERT INTO config (
                    id, roles, locations, experience, email_address, email_app_password,
                    schedule_hours, sources, career_pages, custom_sites,
                    llm_provider, llm_api_key, resume_path, dedup_days, auto_run_enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    int(d.get("dedup_days", 7)),
                    1 if d.get("auto_run_enabled", True) else 0,
                ),
            )
        cur = await db.execute("PRAGMA table_info(runs)")
        run_cols = {row[1] for row in await cur.fetchall()}
        if "duration_sec" not in run_cols:
            await db.execute("ALTER TABLE runs ADD COLUMN duration_sec REAL")
        cur = await db.execute("PRAGMA table_info(config)")
        cfg_cols = {row[1] for row in await cur.fetchall()}
        if "browser_cdp_url" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN browser_cdp_url TEXT NOT NULL DEFAULT ''"
            )
        if "auto_run_enabled" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN auto_run_enabled INTEGER NOT NULL DEFAULT 1"
            )
        if "linkedin_email" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN linkedin_email TEXT NOT NULL DEFAULT ''"
            )
        if "linkedin_password" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN linkedin_password TEXT NOT NULL DEFAULT ''"
            )
        if "dedup_days" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN dedup_days INTEGER NOT NULL DEFAULT 7"
            )
        if "linkedin_easy_apply_only" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN linkedin_easy_apply_only INTEGER NOT NULL DEFAULT 0"
            )
        if "linkedin_include_easy_apply" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN linkedin_include_easy_apply INTEGER NOT NULL DEFAULT 0"
            )
            if "linkedin_easy_apply_only" in cfg_cols:
                await db.execute(
                    "UPDATE config SET linkedin_include_easy_apply = 1 "
                    "WHERE id = 1 AND linkedin_easy_apply_only = 1"
                )
        if "ats_platforms" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN ats_platforms TEXT NOT NULL DEFAULT '{}'"
            )
            await db.execute(
                "UPDATE config SET ats_platforms = ? WHERE id = 1",
                (dump_json(d.get("ats_platforms") or {}),),
            )
        if "linkedin_employment_types" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN linkedin_employment_types TEXT NOT NULL "
                "DEFAULT '[\"F\"]'"
            )
            await db.execute(
                "UPDATE config SET linkedin_employment_types = ? WHERE id = 1",
                (dump_json(d.get("linkedin_employment_types") or ["F"]),),
            )
        if "filter_jobs_by_relevance_llm" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN filter_jobs_by_relevance_llm "
                "INTEGER NOT NULL DEFAULT 1"
            )
        if "ats_posted_within_days" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN ats_posted_within_days "
                "INTEGER NOT NULL DEFAULT 7"
            )
        if "ats_google_max_serp_pages" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN ats_google_max_serp_pages "
                "INTEGER NOT NULL DEFAULT 20"
            )
        if "ats_captcha_wait_seconds" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN ats_captcha_wait_seconds "
                "INTEGER NOT NULL DEFAULT 180"
            )
        if "linkedin_posted_past_week" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN linkedin_posted_past_week "
                "INTEGER NOT NULL DEFAULT 0"
            )
        if "linkedin_include_reposts" not in cfg_cols:
            await db.execute(
                "ALTER TABLE config ADD COLUMN linkedin_include_reposts "
                "INTEGER NOT NULL DEFAULT 0"
            )
        await db.commit()
