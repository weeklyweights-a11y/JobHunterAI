"""Job rows: insert, query, deduplication."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import aiosqlite

from db_paths import DB_PATH


def _today_start_iso() -> str:
    return datetime.combine(date.today(), datetime.min.time()).isoformat()


def _job_row(row: aiosqlite.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "title": row["title"],
        "company": row["company"],
        "url": row["url"],
        "source": row["source"],
        "found_at": row["found_at"],
        "search_role": row["search_role"],
        "search_location": row["search_location"],
    }


async def add_job(
    title: str,
    url: str,
    company: str | None = None,
    source: str | None = None,
    search_role: str | None = None,
    search_location: str | None = None,
) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT OR IGNORE INTO jobs
            (title, company, url, source, search_role, search_location)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (title, company, url, source, search_role, search_location),
        )
        await db.commit()
        return cur.rowcount > 0


async def add_jobs_bulk(
    jobs: list[dict[str, Any]],
    *,
    found_at_iso: str | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Insert jobs with URL dedup; return (count inserted, rows that were new)."""
    run_ts = found_at_iso or datetime.now().isoformat(timespec="seconds")
    added = 0
    new_rows: list[dict[str, Any]] = []
    async with aiosqlite.connect(DB_PATH) as db:
        for j in jobs:
            cur = await db.execute(
                """INSERT OR IGNORE INTO jobs
                (title, company, url, source, search_role, search_location, found_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    j["title"],
                    j.get("company"),
                    j["url"],
                    j.get("source"),
                    j.get("search_role"),
                    j.get("search_location"),
                    run_ts,
                ),
            )
            if cur.rowcount > 0:
                added += cur.rowcount
                new_rows.append(
                    {
                        "title": j["title"],
                        "company": (j.get("company") or "") or "",
                        "url": j["url"],
                        "source": j.get("source") or "",
                        "search_role": j.get("search_role"),
                        "search_location": j.get("search_location"),
                        "found_at": run_ts,
                    }
                )
        await db.commit()
    return added, new_rows


async def get_all_jobs(
    *,
    source: str | None = None,
    search_role: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if source:
        clauses.append("source = ?")
        params.append(source)
    if search_role:
        clauses.append("search_role = ?")
        params.append(search_role)
    if date_from:
        clauses.append("found_at >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("found_at <= ?")
        params.append(date_to)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"SELECT * FROM jobs{where} ORDER BY found_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
        return [_job_row(r) for r in rows]


async def get_jobs_today(
    *,
    source: str | None = None,
    search_role: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    return await get_all_jobs(
        source=source,
        search_role=search_role,
        date_from=_today_start_iso(),
        limit=limit,
        offset=offset,
    )


async def count_by_source() -> dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT source, COUNT(*) FROM jobs GROUP BY source ORDER BY source"
        )
        rows = await cur.fetchall()
        return {str(r[0]): int(r[1]) for r in rows if r[0]}


async def count_by_role() -> dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT search_role, COUNT(*) FROM jobs GROUP BY search_role ORDER BY search_role"
        )
        rows = await cur.fetchall()
        out: dict[str, int] = {}
        for r in rows:
            key = (r[0] or "Unknown") if r[0] else "Unknown"
            out[str(key)] = int(r[1])
        return out


async def get_job_count(
    *,
    source: str | None = None,
    search_role: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> int:
    clauses: list[str] = []
    params: list[Any] = []
    if source:
        clauses.append("source = ?")
        params.append(source)
    if search_role:
        clauses.append("search_role = ?")
        params.append(search_role)
    if date_from:
        clauses.append("found_at >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("found_at <= ?")
        params.append(date_to)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"SELECT COUNT(*) FROM jobs{where}"
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, params)
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def get_today_count() -> int:
    return await get_job_count(date_from=_today_start_iso())


async def url_exists(url: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM jobs WHERE url = ? LIMIT 1", (url,))
        row = await cur.fetchone()
        return row is not None
