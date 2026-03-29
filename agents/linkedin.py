"""LinkedIn Jobs agent (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from agents.base import (
    EmitFn,
    MAX_STEPS_LINKEDIN,
    TASK_TIMEOUT_SEC_LINKEDIN,
    run_agent_task,
)

logger = logging.getLogger(__name__)


def _linkedin_task(role: str, location: str) -> str:
    return f"""LinkedIn Jobs only — stay on linkedin.com. Do not use Google search or any other site to bypass LinkedIn.

1) Open https://www.linkedin.com/jobs/search/
2) Sign-in wall: If you see "Sign in to view more jobs", an auth wall, or any overlay that blocks keyword/location fields or the results list, do NOT dismiss it with random clicks, do NOT leave LinkedIn. Use the wait action with 60 seconds so the user can sign in in the browser; repeat wait (60s) up to 10 times until the wall is gone and you can use search or see job cards.
3) Set keyword {role!r} and location {location!r}, run search. Stay on the search results page (job list on the left), not stuck on a single job detail.
4) Date posted: prefer "Past 24 hours" (freshest). If you cannot set it (e.g. not indexed), try "Past week". If still impossible, keep the active filter and continue.
5) First results page only. Collect every visible job card: title, company, job URL.
6) Finish with the done action only: JSON array of objects with keys "title", "company", "url". No markdown. If none, [].

Role: {role}
Location: {location}
"""


async def search_linkedin(
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
        await _emit(f"Searching LinkedIn for {role} in {loc}...")
        await _emit(
            "LinkedIn: if a sign-in prompt appears, sign in in the browser window — "
            "the agent will wait in 60s chunks (up to ~10 min) before continuing."
        )
        try:
            jobs = await run_agent_task(
                llm,
                _linkedin_task(role, loc),
                app_cfg,
                exclude_actions=["search_google"],
                task_timeout_sec=TASK_TIMEOUT_SEC_LINKEDIN,
                max_steps=MAX_STEPS_LINKEDIN,
            )
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
