"""Jobs, stats, and run history API (Phase 4)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Query

import db
from schemas import JobStatsResponse

router = APIRouter(tags=["jobs"])


def _parse_iso_duration(started_at: str | None, finished_at: str | None) -> float | None:
    if not started_at or not finished_at:
        return None
    try:
        a = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        b = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
        return round((b - a).total_seconds(), 2)
    except ValueError:
        return None


@router.get("/jobs")
async def list_jobs(
    source: str | None = None,
    role: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    jobs = await db.get_all_jobs(
        source=source,
        search_role=role,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )
    return {"data": jobs, "count": len(jobs)}


@router.get("/jobs/today")
async def jobs_today(
    source: str | None = None,
    role: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    jobs = await db.get_jobs_today(
        source=source,
        search_role=role,
        limit=limit,
        offset=offset,
    )
    return {"data": jobs, "count": len(jobs)}


@router.get("/jobs/stats")
async def jobs_stats() -> dict[str, Any]:
    total = await db.get_job_count()
    today = await db.get_today_count()
    by_source = await db.count_by_source()
    by_role = await db.count_by_role()
    return {
        "data": JobStatsResponse(
            total=total,
            today=today,
            by_source=by_source,
            by_role=by_role,
        ).model_dump(),
    }


@router.get("/runs")
async def list_runs(limit: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
    runs = await db.get_runs(limit=limit)
    out = []
    for r in runs:
        row = dict(r)
        stored = row.get("duration_sec")
        if stored is not None and isinstance(stored, (int, float)):
            row["duration_seconds"] = round(float(stored), 2)
        else:
            row["duration_seconds"] = _parse_iso_duration(
                row.get("started_at"), row.get("finished_at")
            )
        out.append(row)
    return {"data": out, "count": len(out)}
