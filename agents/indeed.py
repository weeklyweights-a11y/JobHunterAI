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
    return f"""Complete this task in the browser:

1. Go to https://www.indeed.com
2. In the "what" / job title field, type: {role!r}
3. In the "where" / location field, clear it and type: {location!r}
4. Click the search button
5. Open the "Date posted" filter and select "Last 24 hours"
6. Wait for results to load
7. For each job listing, extract job title, company name, and the full URL to the job posting
8. If there is a next page, go to it — up to 2 pages total
9. When finished, use only the "done" action. In the done text, output a JSON array of objects
   with keys "title", "company", "url". No markdown. If none, output [].

Role: {role}
Location: {location}
"""


async def search_indeed(
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
        await _emit(f"Searching Indeed for {role} in {loc}...")
        try:
            jobs = await run_agent_task(llm, _indeed_task(role, loc))
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
