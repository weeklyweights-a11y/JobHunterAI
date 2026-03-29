"""Run all enabled source agents and persist jobs (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import db
from agents.career_pages import search_career_pages
from agents.indeed import search_indeed
from agents.linkedin import search_linkedin
from agents.llm_setup import build_chat_model
from agents.yc import search_yc

logger = logging.getLogger(__name__)

SOURCE_ORDER = ["linkedin", "indeed", "yc", "career_page"]


def _make_emit(
    status_queue: asyncio.Queue[dict[str, Any]] | None,
    status_callback: Callable[[str], Awaitable[None] | None]
    | Callable[[str], None]
    | None,
    progress_sink: Callable[[dict[str, Any]], Awaitable[None]] | None,
) -> tuple[
    Callable[[str], Awaitable[None]],
    Callable[[dict[str, Any]], Awaitable[None]],
]:
    async def put_event(ev: dict[str, Any]) -> None:
        msg = ev.get("message", "")
        if isinstance(msg, str):
            logger.info("%s", msg)
        if status_queue is not None:
            await status_queue.put(ev)
        if progress_sink is not None:
            await progress_sink(ev)
        if status_callback is not None:
            text = msg if isinstance(msg, str) else str(ev)
            result = status_callback(text)
            if asyncio.iscoroutine(result):
                await result

    async def emit(msg: str) -> None:
        await put_event({"type": "status", "message": msg, "data": {}})

    return emit, put_event


def _enrich(jobs: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for j in jobs:
        out.append(
            {
                "title": j["title"],
                "company": j.get("company") or "",
                "url": j["url"],
                "source": source,
                "search_role": j.get("search_role"),
                "search_location": j.get("search_location"),
            }
        )
    return out


async def run_hunt(
    status_queue: asyncio.Queue[dict[str, Any]] | None = None,
    status_callback: Callable[[str], Awaitable[None] | None]
    | Callable[[str], None]
    | None = None,
    progress_sink: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    """Load config, run enabled agents sequentially, insert jobs, return summary."""
    emit, put_event = _make_emit(status_queue, status_callback, progress_sink)
    cfg = await db.get_config()
    llm = build_chat_model(cfg["llm_provider"], cfg["llm_api_key"])
    sources = cfg["sources"]
    roles = [r for r in cfg["roles"] if isinstance(r, str) and r.strip()]
    locs = [l for l in cfg["locations"] if isinstance(l, str) and l.strip()]
    career_urls = list(
        dict.fromkeys(
            [u for u in cfg.get("career_pages", []) if u]
            + [u for u in cfg.get("custom_sites", []) if u]
        )
    )

    all_jobs: list[dict[str, Any]] = []
    timings: dict[str, float] = {}
    by_source: dict[str, int] = {}
    errors: list[str] = []

    for key in SOURCE_ORDER:
        if not sources.get(key, False):
            continue
        if key != "career_page" and (not roles or not locs):
            await emit(f"Skipping {key}: add at least one role and location in config.")
            continue
        if key == "career_page" and not career_urls:
            await emit("Skipping career pages: no career or custom URLs configured.")
            continue

        await emit(f"Starting {key}...")
        t0 = time.perf_counter()
        try:
            if key == "linkedin":
                raw = await search_linkedin(llm, roles, locs, emit=emit)
            elif key == "indeed":
                raw = await search_indeed(llm, roles, locs, emit=emit)
            elif key == "yc":
                raw = await search_yc(llm, roles, locs, emit=emit)
            else:
                raw = await search_career_pages(llm, career_urls, emit=emit)

            enriched = _enrich(raw, key)
            all_jobs.extend(enriched)
            by_source[key] = len(enriched)
            for job in enriched:
                title = job["title"]
                company = (job.get("company") or "").strip() or "Unknown"
                await put_event(
                    {
                        "type": "job_found",
                        "message": f"Found: {title} at {company}",
                        "data": {
                            "title": title,
                            "company": company,
                            "url": job["url"],
                            "source": key,
                        },
                    }
                )
            label = key.replace("_", " ").title()
            await put_event(
                {
                    "type": "source_complete",
                    "message": f"{label} complete: {len(enriched)} jobs found",
                    "data": {"source": key, "count": len(enriched)},
                }
            )
        except Exception as e:
            msg = f"Source {key} failed: {e}"
            logger.exception(msg)
            errors.append(msg)
            await put_event({"type": "error", "message": msg, "data": {"source": key}})
        timings[key] = time.perf_counter() - t0

    total_found = len(all_jobs)
    total_new = await db.add_jobs_bulk(all_jobs)
    ns = len(by_source)
    done_msg = (
        f"Done! {total_new} new jobs found across {ns} source{'s' if ns != 1 else ''}."
        if ns
        else f"Done! {total_new} new jobs found."
    )
    await put_event(
        {
            "type": "complete",
            "message": done_msg,
            "data": {
                "total_found": total_found,
                "total_new": total_new,
                "by_source": by_source,
                "errors": errors,
            },
        }
    )

    return {
        "total_found": total_found,
        "total_new": total_new,
        "by_source": by_source,
        "timings_sec": timings,
        "errors": errors,
    }
