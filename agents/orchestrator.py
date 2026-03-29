"""Run all enabled source agents and persist jobs (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from collections.abc import Awaitable, Callable
from typing import Any

import db
from emailer import send_jobs_report_sync
from excel_report import write_jobs_xlsx
from output_util import OUTPUT_DIR
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


def _by_source_counts_new(new_jobs: list[dict[str, Any]]) -> dict[str, int]:
    keys = ("linkedin", "indeed", "yc", "career_page")
    out = {k: 0 for k in keys}
    for j in new_jobs:
        s = str(j.get("source") or "")
        if s in out:
            out[s] += 1
    return out


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
                raw = await search_linkedin(llm, roles, locs, emit=emit, app_cfg=cfg)
            elif key == "indeed":
                raw = await search_indeed(llm, roles, locs, emit=emit, app_cfg=cfg)
            elif key == "yc":
                raw = await search_yc(llm, roles, locs, emit=emit, app_cfg=cfg)
            else:
                raw = await search_career_pages(
                    llm, career_urls, emit=emit, app_cfg=cfg
                )

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
    run_ts = datetime.now().isoformat(timespec="seconds")
    total_new, new_jobs = await db.add_jobs_bulk(
        all_jobs, found_at_iso=run_ts
    )
    ns = len(by_source)

    if total_new == 0:
        await emit("No new jobs found this cycle. Skipping email.")
    else:
        xlsx_path = write_jobs_xlsx(new_jobs, output_dir=OUTPUT_DIR)
        await emit(f"Excel report saved to output/{xlsx_path.name}")
        email_addr = (cfg.get("email_address") or "").strip()
        email_pw = (cfg.get("email_app_password") or "").strip()
        if email_addr and email_pw:
            try:
                await asyncio.to_thread(
                    send_jobs_report_sync,
                    email_addr,
                    email_pw,
                    xlsx_path,
                    {
                        "total": total_new,
                        "by_source": _by_source_counts_new(new_jobs),
                    },
                )
                await emit(f"Email sent to {email_addr}")
            except Exception as e:
                logger.exception("Email sending failed")
                await emit(f"Email sending failed: {str(e)[:400]}")
        else:
            await emit("Email not configured; report saved locally only.")

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
