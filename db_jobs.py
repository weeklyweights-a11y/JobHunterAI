"""Job rows: insert, query, deduplication (rolling window)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import aiosqlite

from db_paths import DB_PATH


def _today_start_iso() -> str:
    return datetime.combine(date.today(), datetime.min.time()).isoformat()


def _job_row(row: aiosqlite.Row) -> dict[str, Any]:
    keys = row.keys()
    apply_t = "unknown"
    if "apply_type" in keys and row["apply_type"] is not None:
        apply_t = str(row["apply_type"])
    loc = ""
    jid = ""
    posted = ""
    fresh = ""
    applicants = ""
    if "location" in keys and row["location"] is not None:
        loc = str(row["location"])
    if "job_id" in keys and row["job_id"] is not None:
        jid = str(row["job_id"])
    if "posted_time" in keys and row["posted_time"] is not None:
        posted = str(row["posted_time"])
    if "freshness" in keys and row["freshness"] is not None:
        fresh = str(row["freshness"])
    if "applicant_count" in keys and row["applicant_count"] is not None:
        applicants = str(row["applicant_count"])
    desc = ""
    sen = ""
    if "job_description" in keys and row["job_description"] is not None:
        desc = str(row["job_description"])
    if "seniority" in keys and row["seniority"] is not None:
        sen = str(row["seniority"])
    sal = ""
    emp = ""
    if "salary" in keys and row["salary"] is not None:
        sal = str(row["salary"])
    if "employment_type" in keys and row["employment_type"] is not None:
        emp = str(row["employment_type"])
    return {
        "id": row["id"],
        "title": row["title"],
        "company": row["company"],
        "url": row["url"],
        "source": row["source"],
        "apply_type": apply_t,
        "found_at": row["found_at"],
        "search_role": row["search_role"],
        "search_location": row["search_location"],
        "location": loc,
        "job_id": jid,
        "posted_time": posted,
        "freshness": fresh,
        "applicant_count": applicants,
        "job_description": desc,
        "seniority": sen,
        "salary": sal,
        "employment_type": emp,
    }


async def _url_in_dedup_window(
    db: aiosqlite.Connection, url: str, dedup_days: int
) -> bool:
    cur = await db.execute(
        """
        SELECT 1 FROM jobs
        WHERE url = ? AND found_at > datetime('now', '-' || ? || ' days')
        LIMIT 1
        """,
        (url, str(max(1, dedup_days))),
    )
    row = await cur.fetchone()
    return row is not None


async def add_job(
    title: str,
    url: str,
    company: str | None = None,
    source: str | None = None,
    *,
    apply_type: str = "unknown",
    search_role: str | None = None,
    search_location: str | None = None,
    location: str | None = None,
    job_id: str | None = None,
    posted_time: str | None = None,
    freshness: str | None = None,
    applicant_count: str | None = None,
    job_description: str | None = None,
    seniority: str | None = None,
    salary: str | None = None,
    employment_type: str | None = None,
    dedup_days: int = 7,
) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        if await _url_in_dedup_window(db, url, dedup_days):
            return False
        await db.execute(
            """INSERT INTO jobs
            (title, company, url, source, apply_type, search_role, search_location, location, job_id,
             posted_time, freshness, applicant_count, job_description, seniority, salary, employment_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                title,
                company,
                url,
                source or "",
                apply_type or "unknown",
                search_role,
                search_location,
                location,
                job_id,
                posted_time,
                freshness,
                applicant_count,
                job_description,
                seniority,
                salary,
                employment_type,
            ),
        )
        await db.commit()
        return True


async def add_jobs_bulk(
    jobs: list[dict[str, Any]],
    *,
    found_at_iso: str | None = None,
    dedup_days: int = 7,
) -> tuple[int, list[dict[str, Any]]]:
    """Insert jobs with rolling-window URL dedup; return (count inserted, new rows)."""
    run_ts = found_at_iso or datetime.now().isoformat(timespec="seconds")
    days = max(1, dedup_days)
    added = 0
    new_rows: list[dict[str, Any]] = []
    async with aiosqlite.connect(DB_PATH) as db:
        for j in jobs:
            url = str(j["url"]).strip()
            if not url:
                continue
            if await _url_in_dedup_window(db, url, days):
                continue
            apply_type = str(j.get("apply_type") or "unknown")
            posted_time = str(j.get("posted_time") or "").strip() or None
            freshness = str(j.get("freshness") or "").strip() or None
            applicant_count = str(j.get("applicant_count") or "").strip() or None
            job_description = str(j.get("job_description") or "").strip() or None
            seniority = str(j.get("seniority") or "").strip() or None
            salary = str(j.get("salary") or "").strip() or None
            employment_type = str(j.get("employment_type") or "").strip() or None
            await db.execute(
                """INSERT INTO jobs
                (title, company, url, source, apply_type, search_role, search_location, found_at, location, job_id,
                 posted_time, freshness, applicant_count, job_description, seniority, salary, employment_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    j["title"],
                    j.get("company"),
                    url,
                    j.get("source") or "",
                    apply_type,
                    j.get("search_role"),
                    j.get("search_location"),
                    run_ts,
                    str(j.get("location") or "").strip() or None,
                    str(j.get("job_id") or "").strip() or None,
                    posted_time,
                    freshness,
                    applicant_count,
                    job_description,
                    seniority,
                    salary,
                    employment_type,
                ),
            )
            added += 1
            new_rows.append(
                {
                    "title": j["title"],
                    "company": (j.get("company") or "") or "",
                    "url": url,
                    "source": j.get("source") or "",
                    "apply_type": apply_type,
                    "search_role": j.get("search_role"),
                    "search_location": j.get("search_location"),
                    "found_at": run_ts,
                    "location": str(j.get("location") or ""),
                    "job_id": str(j.get("job_id") or ""),
                    "posted_time": str(j.get("posted_time") or ""),
                    "freshness": str(j.get("freshness") or ""),
                    "applicant_count": str(j.get("applicant_count") or ""),
                    "job_description": str(j.get("job_description") or ""),
                    "seniority": str(j.get("seniority") or ""),
                    "salary": str(j.get("salary") or ""),
                    "employment_type": str(j.get("employment_type") or ""),
                }
            )
        await db.commit()
    return added, new_rows


async def cleanup_old_jobs(retention_days: int = 30) -> int:
    """Delete jobs older than retention_days. Returns rows deleted."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """DELETE FROM jobs WHERE found_at < datetime('now', '-' || ? || ' days')""",
            (str(max(1, retention_days)),),
        )
        await db.commit()
        return cur.rowcount


async def delete_all_jobs() -> int:
    """Remove every row from ``jobs``. Returns how many rows were deleted."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM jobs")
        row = await cur.fetchone()
        n = int(row[0]) if row and row[0] is not None else 0
        await db.execute("DELETE FROM jobs")
        await db.commit()
    return n


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


async def url_exists(url: str, *, dedup_days: int = 7) -> bool:
    """True if url appears within the rolling dedup window."""
    async with aiosqlite.connect(DB_PATH) as db:
        return await _url_in_dedup_window(db, url, max(1, dedup_days))


async def linkedin_url_for_title_company(
    title: str,
    company: str,
    *,
    dedup_days: int = 7,
    source: str = "linkedin",
) -> str | None:
    """Most recent stored URL for same title+company in the dedup window (repost detection)."""
    t = (title or "").strip().lower()
    c = (company or "").strip().lower()
    if not t or not c:
        return None
    days = max(1, int(dedup_days))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT url FROM jobs
            WHERE lower(title) = ? AND lower(company) = ? AND source = ?
              AND found_at > datetime('now', '-' || ? || ' days')
            ORDER BY found_at DESC
            LIMIT 1
            """,
            (t, c, source, str(days)),
        )
        row = await cur.fetchone()
        if not row or row[0] is None:
            return None
        u = str(row[0]).strip()
        return u or None
