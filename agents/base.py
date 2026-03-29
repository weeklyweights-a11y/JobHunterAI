"""browser-use Agent factory, Chrome profile, and task runner (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TypeAlias

from browser_use import Agent
from browser_use.browser.browser import Browser, BrowserConfig
from langchain_core.language_models.chat_models import BaseChatModel

from agents.job_parse import format_jobs_with_llm, parse_jobs_json_from_text

# Spec name alias (Step 2)
parse_jobs_from_agent_output = parse_jobs_json_from_text

logger = logging.getLogger(__name__)

TASK_TIMEOUT_SEC = 300.0
MAX_STEPS = 100

EmitFn: TypeAlias = Callable[[str], Awaitable[None]]


def default_chrome_user_data_dir() -> str | None:
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        if local:
            p = Path(local) / "Google" / "Chrome" / "User Data"
            return str(p) if p.is_dir() else None
    if sys.platform == "darwin":
        p = Path.home() / "Library/Application Support/Google/Chrome"
        return str(p) if p.is_dir() else None
    p = Path.home() / ".config" / "google-chrome"
    return str(p) if p.is_dir() else None


def build_browser_config() -> BrowserConfig:
    headless = os.getenv("JOBHUNTER_HEADLESS", "").lower() in ("1", "true", "yes")
    user_data = os.getenv("CHROME_USER_DATA_DIR") or default_chrome_user_data_dir()
    profile = os.getenv("CHROME_PROFILE_DIRECTORY", "Default")
    extra: list[str] = []
    if user_data and Path(user_data).is_dir():
        extra.append(f"--user-data-dir={user_data}")
        extra.append(f"--profile-directory={profile}")
        logger.info("Using Chrome user data dir for session persistence")
    else:
        logger.warning(
            "Chrome user data directory not set or not found; "
            "set CHROME_USER_DATA_DIR for logged-in sessions."
        )
    return BrowserConfig(headless=headless, extra_chromium_args=extra)


def create_browser_agent(llm: BaseChatModel, task: str) -> Agent:
    """Return a browser-use Agent ready to run (spec Step 2)."""
    browser = Browser(build_browser_config())
    return Agent(task=task, llm=llm, browser=browser, use_vision=True)


async def run_agent_task(llm: BaseChatModel, task: str) -> list[dict[str, Any]]:
    """Run one agent task with timeout; parse jobs from output."""
    agent = create_browser_agent(llm, task)
    try:
        history = await asyncio.wait_for(
            agent.run(max_steps=MAX_STEPS),
            timeout=TASK_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        logger.error("Browser agent timed out after %s seconds", TASK_TIMEOUT_SEC)
        return []
    except Exception:
        logger.exception("Browser agent run failed")
        return []

    text = history.final_result()
    if not text:
        chunks = history.extracted_content()
        text = "\n".join(c for c in chunks if c) if chunks else ""

    jobs = parse_jobs_json_from_text(text)
    if jobs:
        return jobs
    if text.strip():
        try:
            llm_jobs = await format_jobs_with_llm(llm, text)
            if llm_jobs:
                return llm_jobs
        except Exception:
            logger.exception("LLM job reformat failed")
        logger.warning(
            "Browser agent finished but no jobs parsed (%d chars of output)",
            len(text),
        )
    else:
        logger.warning(
            "Browser agent finished with empty result; check Playwright install "
            "and backend logs (try JOBHUNTER_RELOAD=0 on Windows if you see NotImplementedError)."
        )
    return []
