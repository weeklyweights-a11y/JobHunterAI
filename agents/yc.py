"""YC Work at a Startup agent (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from agents.base import (
    EmitFn,
    MAX_STEPS_YC,
    TASK_TIMEOUT_SEC_YC,
    run_agent_task,
)

logger = logging.getLogger(__name__)


def _yc_task(role: str, location: str) -> str:
    return f"""Open https://www.workatastartup.com/jobs — search for work matching the user's criteria.

CRITICAL — use THIS role and location (do not swap in unrelated categories like "full stack" or "remote software engineer" unless they are the same as the user's words):
- Role keywords to match: {role!r}
- Location / remote preference: {location!r}

Steps:
1) If there is a search or filter box, enter the role text above (or the most important words from it). Do not pick a generic category that changes the intent (e.g. do not click a broad "Software" bucket if the user asked for something specific).
2) Apply location / remote filters that match {location!r} when the site offers them.
3) Prefer listings posted recently (today / 24h / new). Skip obviously stale posts (months old) when the date is visible.
4) Scroll down repeatedly and use "load more" / pagination if present until you have collected a **large set** of relevant listings (aim for dozens), not just the first screen.
5) Only include jobs whose title (or obvious role line) relates to the user's role {role!r} — skip unrelated roles.
6) Done action only: JSON array of {{"title","company","url"}}. No markdown. If none, [].

Role: {role}
Location: {location}
"""


def _soft_filter_by_role(jobs: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    """Keep jobs whose title/company mention role tokens; if none match, return original."""
    r = (role or "").strip().lower()
    if not r:
        return jobs
    tokens = [t for t in re.split(r"[\s,/+._-]+", r) if len(t) >= 3]
    if not tokens:
        return jobs
    matched: list[dict[str, Any]] = []
    for j in jobs:
        blob = f"{j.get('title', '')} {j.get('company', '')}".lower()
        if any(t in blob for t in tokens):
            matched.append(j)
    if matched:
        return matched
    logger.info("YC role filter: no token match for %r; keeping all %s jobs", role, len(jobs))
    return jobs


async def search_yc(
    llm: BaseChatModel,
    roles: list[str],
    locations: list[str],
    emit: EmitFn | None = None,
    app_cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    async def _emit(msg: str) -> None:
        if emit:
            await emit(msg)

    combos = [(r, l) for r in roles for l in locations]
    collected: list[dict[str, Any]] = []
    for i, (role, loc) in enumerate(combos):
        await _emit(f"Searching YC Work at a Startup for {role!r} in {loc!r}...")
        try:
            jobs = await run_agent_task(
                llm,
                _yc_task(role, loc),
                app_cfg,
                task_timeout_sec=TASK_TIMEOUT_SEC_YC,
                max_steps=MAX_STEPS_YC,
            )
            jobs = _soft_filter_by_role(jobs, role)
            for item in jobs:
                item["search_role"] = role
                item["search_location"] = loc
            collected.extend(jobs)
            logger.info("YC %s / %s: %s jobs", role, loc, len(jobs))
        except Exception:
            logger.exception("YC failed for %s / %s", role, loc)
        if i < len(combos) - 1:
            await asyncio.sleep(random.uniform(1.0, 3.0))
    return collected
