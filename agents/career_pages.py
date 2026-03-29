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
    return f"""Go to {url}. Find open roles on this page (and one level of obvious "see all" if present).
For each role: title, company (from page if visible), direct link.
Done action only: JSON array of {{"title","company","url"}}. No markdown. If none, [].
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
