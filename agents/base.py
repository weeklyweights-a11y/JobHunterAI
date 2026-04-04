"""browser-use Agent factory, browser connection (CDP or launch), and task runner (Phase 3)."""

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
from browser_use.controller.service import Controller
from langchain_core.language_models.chat_models import BaseChatModel

from agents.job_parse import format_jobs_with_llm, parse_jobs_json_from_text
from agents.llm_setup import maybe_preflight_vertex_llm

# Spec name alias (Step 2)
parse_jobs_from_agent_output = parse_jobs_json_from_text

logger = logging.getLogger(__name__)

TASK_TIMEOUT_SEC = 300.0
# LinkedIn: sign-in, scroll, click each job for detail, paginate — very long runs.
TASK_TIMEOUT_SEC_LINKEDIN = 2400.0
# YC: scroll + stricter role matching needs more steps than default.
TASK_TIMEOUT_SEC_YC = 900.0
MAX_STEPS = 60
MAX_STEPS_LINKEDIN = 320
MAX_STEPS_YC = 90

EmitFn: TypeAlias = Callable[[str], Awaitable[None]]


def _vertex_preflight_is_known_blocker(exc: BaseException) -> bool:
    s = str(exc).lower()
    return any(
        needle in s
        for needle in (
            "billing_disabled",
            "service_disabled",
            "requires billing",
            "vertex ai api has not been used",
            "api has not been used in project",
            "invalid_grant",
            "permissiondenied",
            "getting metadata from plugin failed",
            "publisher model",
            "was not found or your project does not have access",
            "notfound: 404",
        )
    )


def _vertex_preflight_is_model_not_found(exc: BaseException) -> bool:
    s = str(exc).lower()
    return "publisher model" in s or "was not found or your project does not have access" in s


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


def build_browser_config(app_cfg: dict[str, Any] | None = None) -> BrowserConfig:
    """Prefer CDP to your real Chrome (same app; agent opens tabs there). Else Playwright Chromium."""
    headless = os.getenv("JOBHUNTER_HEADLESS", "").lower() in ("1", "true", "yes")
    dash = ""
    if app_cfg:
        raw = app_cfg.get("browser_cdp_url")
        if isinstance(raw, str):
            dash = raw.strip()
    cdp_url = dash or (
        os.getenv("JOBHUNTER_CDP_URL") or os.getenv("CHROME_CDP_URL") or ""
    ).strip()
    chrome_path = (
        os.getenv("JOBHUNTER_CHROME_PATH") or os.getenv("CHROME_INSTANCE_PATH") or ""
    ).strip()

    if cdp_url:
        logger.info("Browser: connect via CDP (%s)", cdp_url)
        return BrowserConfig(
            cdp_url=cdp_url,
            headless=False,
            disable_security=True,
            extra_chromium_args=[],
        )

    if chrome_path:
        logger.info(
            "Browser: chrome_instance_path=%s (remote debugging on port 9222)",
            chrome_path,
        )
        return BrowserConfig(
            chrome_instance_path=chrome_path,
            headless=False,
            disable_security=True,
            extra_chromium_args=[],
        )

    use_profile = os.getenv("JOBHUNTER_USE_CHROME_PROFILE", "").lower() in (
        "1",
        "true",
        "yes",
    )
    extra: list[str] = []
    if use_profile:
        user_data = os.getenv("CHROME_USER_DATA_DIR") or default_chrome_user_data_dir()
        profile = os.getenv("CHROME_PROFILE_DIRECTORY", "Default")
        if user_data and Path(user_data).is_dir():
            extra.append(f"--user-data-dir={user_data}")
            extra.append(f"--profile-directory={profile}")
            logger.warning(
                "JOBHUNTER_USE_CHROME_PROFILE: sharing a real Chrome profile with "
                "Playwright Chromium is unreliable (locks, ignored flags). Prefer JOBHUNTER_CDP_URL."
            )
        else:
            logger.warning(
                "JOBHUNTER_USE_CHROME_PROFILE set but Chrome user data dir not found"
            )
    else:
        logger.warning(
            "Browser: launching separate Playwright Chromium (extra window). "
            "To use YOUR Chrome and only open new tabs: quit all Chrome, start "
            "`chrome.exe --remote-debugging-port=9222`, open this dashboard in that "
            "Chrome, set Chrome CDP URL to http://127.0.0.1:9222 on the form, Save, "
            "then Start Hunting — or set JOBHUNTER_CDP_URL in .env. "
            "If the window stays blank, check the server log for LLM errors first "
            "(e.g. Vertex invalid_grant, 403 API disabled, or BILLING_DISABLED)."
        )

    return BrowserConfig(
        headless=headless,
        disable_security=True,
        extra_chromium_args=extra,
    )


def create_browser_agent(
    llm: BaseChatModel,
    task: str,
    app_cfg: dict[str, Any] | None = None,
    *,
    exclude_actions: list[str] | None = None,
) -> Agent:
    """Return a browser-use Agent ready to run (spec Step 2)."""
    browser = Browser(build_browser_config(app_cfg))
    controller = Controller(exclude_actions=list(exclude_actions or []))
    return Agent(
        task=task,
        llm=llm,
        browser=browser,
        controller=controller,
        use_vision=True,
    )


async def run_agent_task(
    llm: BaseChatModel,
    task: str,
    app_cfg: dict[str, Any] | None = None,
    *,
    exclude_actions: list[str] | None = None,
    task_timeout_sec: float | None = None,
    max_steps: int | None = None,
) -> list[dict[str, Any]]:
    """Run one agent task with timeout; parse jobs from output."""
    try:
        await maybe_preflight_vertex_llm(llm)
    except asyncio.TimeoutError:
        logger.error(
            "Vertex preflight timed out; skipping browser (no about:blank window for this task)."
        )
        return []
    except Exception as e:
        if _vertex_preflight_is_known_blocker(e):
            proj = (
                os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "YOUR_PROJECT"
            )
            if _vertex_preflight_is_model_not_found(e):
                logger.error(
                    "Vertex preflight failed — model not found or not enabled for this project/region. "
                    "Run `python scripts/list_vertex_models.py` and set JOBHUNTER_VERTEX_MODEL to the "
                    "printed short id (e.g. gemini-2.5-flash). "
                    "Or use AI Studio: comment JOBHUNTER_GEMINI_BACKEND=vertex. Error: %s",
                    str(e)[:900],
                )
            else:
                logger.error(
                    "Vertex preflight failed — browser not opened (fixes about:blank from dead LLM). "
                    "Enable billing: https://console.developers.google.com/billing/enable?project=%s "
                    "and ensure Vertex AI API is enabled. "
                    "Or use AI Studio: remove JOBHUNTER_GEMINI_BACKEND=vertex and use GOOGLE_API_KEY. "
                    "Error: %s",
                    proj,
                    str(e)[:900],
                )
            return []
        logger.warning(
            "Vertex preflight failed with unexpected error (continuing): %s",
            str(e)[:500],
        )

    timeout = TASK_TIMEOUT_SEC if task_timeout_sec is None else task_timeout_sec
    steps = MAX_STEPS if max_steps is None else max_steps
    agent = create_browser_agent(
        llm, task, app_cfg, exclude_actions=exclude_actions
    )
    try:
        history = await asyncio.wait_for(
            agent.run(max_steps=steps),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.error("Browser agent timed out after %s seconds", timeout)
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
            "Browser agent finished with empty result; on Windows use `python run.py` "
            "(Proactor in run.py only), JOBHUNTER_RELOAD=0 if needed, Playwright install, "
            "and JOBHUNTER_CDP_URL for a real Chrome session."
        )
    return []
