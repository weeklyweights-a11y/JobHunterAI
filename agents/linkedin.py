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
    return f"""Complete this task in the browser:

1. Go to https://www.linkedin.com/jobs/search/
2. If there is a search box for keywords, clear it and type: {role!r}
3. Set the location field to: {location!r}
4. Click search or press Enter
5. Open the "Date posted" or time filter
6. Prefer "Past 24 hours"; if unavailable use "Past week"
7. Wait for results to load
8. For each visible job card, read job title, company name, and the URL to the job posting
9. If there is a next page or "see more jobs", go to the next page. Repeat up to 3 pages total
10. When finished, respond using ONLY the "done" action. In the done text, output a JSON array
    of objects with keys "title", "company", "url" for every job you found. No markdown fences.
    If none found, output [].

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
