"""APScheduler recurring hunts (Phase 6)."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import db
from agent_runtime import config_allows_schedule, try_start_hunt

logger = logging.getLogger(__name__)

HUNT_JOB_ID = "hunt_interval"
FIRST_DELAY_SEC = 30


def _bind_hunt_tick(app: Any):
    async def tick() -> None:
        await scheduled_hunt_tick(app)

    return tick


async def scheduled_hunt_tick(app: Any) -> None:
    result = await try_start_hunt(app, reason="scheduled")
    if result["started"]:
        return
    detail = result.get("detail")
    if detail == "already_running":
        logger.info("Skipping scheduled run — agent is already active.")
    else:
        logger.warning("Scheduled run skipped: %s", detail)


def _remove_job(scheduler: AsyncIOScheduler) -> None:
    try:
        scheduler.remove_job(HUNT_JOB_ID)
    except JobLookupError:
        pass


def _interval_hours(trigger: Any) -> float | None:
    if not isinstance(trigger, IntervalTrigger):
        return None
    return trigger.interval.total_seconds() / 3600.0


def _next_run_iso_utc(job: Any) -> str | None:
    nrt = job.next_run_time
    if nrt is None:
        return None
    if nrt.tzinfo is None:
        nrt = nrt.replace(tzinfo=timezone.utc)
    else:
        nrt = nrt.astimezone(timezone.utc)
    return nrt.strftime("%Y-%m-%dT%H:%M:%SZ")


async def sync_scheduler(
    app: Any,
    *,
    schedule_hours_changed: bool | None = None,
) -> None:
    """Add/update/remove hunt job from DB config. Caller passes schedule_hours_changed on POST /config."""
    scheduler: AsyncIOScheduler | None = getattr(app.state, "scheduler", None)
    if scheduler is None:
        logger.warning("sync_scheduler: no scheduler on app.state")
        return

    cfg = await db.get_config()
    if not config_allows_schedule(cfg):
        _remove_job(scheduler)
        logger.info("Auto-run disabled until config is complete (roles, locations, LLM, sources).")
        return

    hours = int(cfg["schedule_hours"])
    existing = scheduler.get_job(HUNT_JOB_ID)

    if existing is None:
        scheduler.add_job(
            _bind_hunt_tick(app),
            IntervalTrigger(hours=hours),
            id=HUNT_JOB_ID,
            replace_existing=True,
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=FIRST_DELAY_SEC),
        )
        logger.info(
            "Auto-run enabled: first hunt in %s seconds, then every %s hours.",
            FIRST_DELAY_SEC,
            hours,
        )
        return

    if schedule_hours_changed is True:
        _remove_job(scheduler)
        scheduler.add_job(
            _bind_hunt_tick(app),
            IntervalTrigger(hours=hours),
            id=HUNT_JOB_ID,
            replace_existing=True,
        )
        logger.info("Schedule interval updated to every %s hours.", hours)
        return

    ih = _interval_hours(existing.trigger)
    if ih is None or int(ih) != hours:
        _remove_job(scheduler)
        scheduler.add_job(
            _bind_hunt_tick(app),
            IntervalTrigger(hours=hours),
            id=HUNT_JOB_ID,
            replace_existing=True,
        )
        logger.info("Repaired hunt job trigger to every %s hours.", hours)


async def build_scheduler_status(app: Any) -> dict[str, Any]:
    """Payload for GET /api/scheduler (inner `data` object)."""
    cfg = await db.get_config()
    interval_hours = int(cfg["schedule_hours"])
    valid = config_allows_schedule(cfg)
    scheduler: AsyncIOScheduler | None = getattr(app.state, "scheduler", None)
    job = scheduler.get_job(HUNT_JOB_ID) if scheduler else None
    active = bool(valid and job is not None)
    next_run: str | None = None
    if job is not None:
        next_run = _next_run_iso_utc(job)
    return {
        "active": active,
        "next_run": next_run,
        "interval_hours": interval_hours,
    }


def create_scheduler(app: Any) -> AsyncIOScheduler:
    """Create AsyncIOScheduler, attach to app.state, start it."""
    scheduler = AsyncIOScheduler()
    app.state.scheduler = scheduler
    scheduler.start()
    return scheduler


def shutdown_scheduler(app: Any) -> None:
    sched: AsyncIOScheduler | None = getattr(app.state, "scheduler", None)
    if sched is not None and sched.running:
        sched.shutdown(wait=False)
