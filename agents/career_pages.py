"""Career page / custom URL agent (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel

from agents.base import EmitFn, run_agent_task

logger = logging.getLogger(__name__)


def _career_task(url: str) -> str:
    return f"""Complete this task in the browser:

1. Navigate to {url}
2. Read the page and find all job listings or open positions
3. For each listing, extract job title, company name (from page header or nearby context),
   and the URL to that specific job posting
4. Do not filter by posting date
5. When finished, use only the "done" action. In the done text, output a JSON array of objects
   with keys "title", "company", "url". No markdown. If none, output [].
"""


async def search_career_pages(
    llm: BaseChatModel,
    urls: list[str],
    emit: EmitFn | None = None,
) -> list[dict[str, Any]]:
    async def _emit(msg: str) -> None:
        if emit:
            await emit(msg)

    collected: list[dict[str, Any]] = []
    for i, url in enumerate(urls):
        await _emit(f"Scanning career page: {url}")
        try:
            jobs = await run_agent_task(llm, _career_task(url))
            collected.extend(jobs)
            logger.info("Career URL %s: %s jobs", url, len(jobs))
        except Exception:
            logger.exception("Career page failed for %s", url)
        if i < len(urls) - 1:
            await asyncio.sleep(random.uniform(2.0, 5.0))
    return collected
