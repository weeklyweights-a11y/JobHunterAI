"""Agent run history rows."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import aiosqlite

from db_paths import DB_PATH


async def create_run(status: str = "running") -> int:
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO runs (started_at, status, jobs_found) VALUES (?, ?, 0)",
            (now, status),
        )
        await db.commit()
        return int(cur.lastrowid)


async def update_run(
    run_id: int,
    *,
    finished_at: str | None = None,
    status: str | None = None,
    jobs_found: int | None = None,
    errors: str | None = None,
    duration_sec: float | None = None,
) -> None:
    fields: list[str] = []
    params: list[Any] = []
    if finished_at is not None:
        fields.append("finished_at = ?")
        params.append(finished_at)
    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if jobs_found is not None:
        fields.append("jobs_found = ?")
        params.append(jobs_found)
    if errors is not None:
        fields.append("errors = ?")
        params.append(errors)
    if duration_sec is not None:
        fields.append("duration_sec = ?")
        params.append(duration_sec)
    if not fields:
        return
    params.append(run_id)
    sql = "UPDATE runs SET " + ", ".join(fields) + " WHERE id = ?"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, params)
        await db.commit()


async def get_runs(limit: int = 50) -> list[dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            if "duration_sec" not in d:
                d["duration_sec"] = None
            out.append(
                {
                    "id": d["id"],
                    "started_at": d["started_at"],
                    "finished_at": d["finished_at"],
                    "status": d["status"],
                    "jobs_found": d["jobs_found"],
                    "errors": d["errors"],
                    "duration_sec": d.get("duration_sec"),
                }
            )
        return out
