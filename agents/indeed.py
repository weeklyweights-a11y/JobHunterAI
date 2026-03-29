"""Indeed job search agent (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from agents.base import EmitFn, run_agent_task

logger = logging.getLogger(__name__)


def _indeed_task(role: str, location: str) -> str:
    return f"""On https://www.indeed.com search for job {role!r} in {location!r}. Submit search and wait for listings.
Optionally narrow to recent postings if a date filter is obvious. Use the first results page only.
Extract title, company, and full job URL for each listing.
Done action only: JSON array of {{"title","company","url"}}. No markdown. If none, [].

Role: {role}
Location: {location}
"""


async def search_indeed(
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
        await _emit(f"Searching Indeed for {role} in {loc}...")
        try:
            jobs = await run_agent_task(llm, _indeed_task(role, loc), app_cfg)
            for item in jobs:
                item["search_role"] = role
                item["search_location"] = loc
            collected.extend(jobs)
            logger.info("Indeed %s / %s: %s jobs", role, loc, len(jobs))
        except Exception:
            logger.exception("Indeed failed for %s / %s", role, loc)
        if i < len(combos) - 1:
            await asyncio.sleep(random.uniform(3.0, 8.0))
    return collected
