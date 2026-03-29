"""LinkedIn Jobs agent (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from agents.base import EmitFn, run_agent_task

logger = logging.getLogger(__name__)


def _linkedin_task(role: str, location: str) -> str:
    return f"""Open https://www.linkedin.com/jobs/search/ — set keyword {role!r} and location {location!r}, run search, wait for results.
If you see a date filter, prefer "Past week". Stay on the first results page only.
Collect every visible job card: title, company, job URL.
Finish with the done action only. In done text output a JSON array of objects with keys "title", "company", "url". No markdown. If none, [].

Role: {role}
Location: {location}
"""


async def search_linkedin(
    llm: BaseChatModel,
    roles: list[str],
    locations: list[str],
    emit: EmitFn | None = None,
) -> list[dict[str, Any]]:
    async def _emit(msg: str) -> None:
        if emit:
            await emit(msg)

    combos = [(r, l) for r in roles for l in locations]
    collected: list[dict[str, Any]] = []
    for i, (role, loc) in enumerate(combos):
        await _emit(f"Searching LinkedIn for {role} in {loc}...")
        try:
            jobs = await run_agent_task(llm, _linkedin_task(role, loc))
            for item in jobs:
                item["search_role"] = role
                item["search_location"] = loc
            collected.extend(jobs)
            logger.info("LinkedIn %s / %s: %s jobs", role, loc, len(jobs))
        except Exception:
            logger.exception("LinkedIn failed for %s / %s", role, loc)
        if i < len(combos) - 1:
            await asyncio.sleep(random.uniform(3.0, 8.0))
    return collected
