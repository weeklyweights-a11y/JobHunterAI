"""YC Work at a Startup agent (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from agents.base import EmitFn, run_agent_task

logger = logging.getLogger(__name__)


def _yc_task(role: str, location: str) -> str:
    return f"""Complete this task in the browser:

1. Go to https://www.workatastartup.com/jobs
2. If there is a search or filter, enter the role: {role!r}
3. If there is a location filter, set it to: {location!r}
4. For each job listing, read any "posted" or date indicator
5. Only include jobs posted "today", "1 day ago", "new", or similar meaning within ~24 hours
6. For each qualifying job, extract title, company name, and job URL
7. If there is no role search field, scroll listings and prefer jobs whose titles match keywords from {role!r}
8. When finished, use only the "done" action. In the done text, output a JSON array of objects
   with keys "title", "company", "url". No markdown. If none, output [].

Role: {role}
Location: {location}
"""


async def search_yc(
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
        await _emit(f"Searching YC Work at a Startup for {role} in {loc}...")
        try:
            jobs = await run_agent_task(llm, _yc_task(role, loc))
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
