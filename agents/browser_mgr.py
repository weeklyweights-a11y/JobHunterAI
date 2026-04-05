"""Playwright browser lifecycle: CDP to user Chrome, optional launch, else Chromium."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from urllib.parse import urlparse
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import re
import time

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

logger = logging.getLogger(__name__)

DEFAULT_CDP_URL = "http://127.0.0.1:9222"
CDP_PROBE_PATH = "/json/version"
_LAUNCH_POLL_SEC = 0.5
_LAUNCH_POLL_TIMEOUT_SEC = 45.0

_WARN_FALLBACK = (
    "Could not connect to your Chrome. Using a fresh browser — you may need to log into LinkedIn."
)


def _normalize_cdp_base(url: str) -> str:
    u = (url or "").strip().rstrip("/")
    if not u:
        return DEFAULT_CDP_URL
    if "://" not in u:
        u = "http://" + u
    return u.rstrip("/")


def _cdp_port(base: str) -> int:
    parsed = urlparse(base)
    if parsed.port is not None:
        return int(parsed.port)
    return 9222


def _launch_chrome_enabled() -> bool:
    """Opt-in: spawn Google Chrome with --remote-debugging-port when CDP is down."""
    return os.getenv("JOBHUNTER_LAUNCH_CHROME", "").lower() in ("1", "true", "yes")


def _cdp_url_from_config(app_cfg: dict[str, Any] | None) -> str:
    if app_cfg:
        u = str(app_cfg.get("browser_cdp_url") or "").strip()
        if u:
            return u.rstrip("/")
    env = (os.getenv("JOBHUNTER_CDP_URL") or os.getenv("CHROME_CDP_URL") or "").strip()
    if env:
        return env.rstrip("/")
    return ""


def _find_chrome_executable() -> str | None:
    if sys.platform == "win32":
        paths = [
            Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
            Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
        ]
        for p in paths:
            if p.is_file():
                return str(p)
        return None
    if sys.platform == "darwin":
        p = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
        if p.is_file():
            return str(p)
        return None
    for name in ("google-chrome", "google-chrome-stable", "chromium-browser", "chromium"):
        found = shutil.which(name)
        if found:
            return found
    return None


async def _cdp_reachable(base_url: str) -> bool:
    url = f"{base_url.rstrip('/')}{CDP_PROBE_PATH}"

    def _probe() -> bool:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                return 200 <= getattr(r, "status", 200) < 300
        except (urllib.error.URLError, OSError, TimeoutError):
            return False

    return await asyncio.to_thread(_probe)


def _launch_chrome_for_cdp(chrome_exe: str, port: int = 9222) -> subprocess.Popen[Any] | None:
    args = [
        chrome_exe,
        f"--remote-debugging-port={port}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    try:
        if sys.platform == "win32":
            detach = getattr(subprocess, "DETACHED_PROCESS", 0)
            newgrp = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            return subprocess.Popen(
                args,
                close_fds=True,
                creationflags=detach | newgrp,
            )
        return subprocess.Popen(args, start_new_session=True)
    except OSError as e:
        logger.warning("Could not launch Chrome for CDP: %s", e)
        return None


async def _wait_for_cdp(base_url: str, timeout_sec: float) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout_sec
    while asyncio.get_event_loop().time() < deadline:
        if await _cdp_reachable(base_url):
            return True
        await asyncio.sleep(_LAUNCH_POLL_SEC)
    return False


async def resolve_cdp_url(app_cfg: dict[str, Any] | None) -> tuple[str, bool, bool]:
    """
    Return (cdp_base_url, used_fallback_chromium, we_launched_chrome_process).
    - If config/env sets a CDP URL: connect when reachable; otherwise Playwright Chromium
      (unless JOBHUNTER_LAUNCH_CHROME=1 tries to start Chrome).
    - If unset: probe http://127.0.0.1:9222, then same fallback behavior.
    Default is the previous behavior: **no** auto-spawn of Google Chrome — bundled Chromium opens
    so hunts still run without a manual Chrome debug session. Set JOBHUNTER_LAUNCH_CHROME=1 to
    auto-launch system Chrome when the port is free.
    """
    explicit = _cdp_url_from_config(app_cfg)
    if explicit:
        base = _normalize_cdp_base(explicit)
        if await _cdp_reachable(base):
            return base, False, False
        if _launch_chrome_enabled():
            port = _cdp_port(base)
            exe = _find_chrome_executable()
            if exe:
                logger.info(
                    "CDP not reachable at %s; launching Google Chrome (%s) with port %s",
                    base,
                    exe,
                    port,
                )
                proc = _launch_chrome_for_cdp(exe, port=port)
                if proc and await _wait_for_cdp(base, _LAUNCH_POLL_TIMEOUT_SEC):
                    logger.info("Chrome is listening for CDP at %s", base)
                    return base, False, True
                logger.warning("Chrome launch did not expose CDP in time.")
        return base, True, False

    base = DEFAULT_CDP_URL
    if await _cdp_reachable(base):
        return base, False, False

    if _launch_chrome_enabled():
        exe = _find_chrome_executable()
        if exe:
            logger.info(
                "CDP not reachable at %s; launching Google Chrome (%s) with port %s",
                base,
                exe,
                9222,
            )
            proc = _launch_chrome_for_cdp(exe, port=9222)
            if proc and await _wait_for_cdp(base, _LAUNCH_POLL_TIMEOUT_SEC):
                return base, False, True
            logger.warning("Chrome launch did not expose CDP in time.")

    return base, True, False


async def dismiss_cookie_popup(page: Page, *, budget_sec: float = 3.0) -> None:
    """
    Best-effort click on common cookie-consent controls. Returns immediately if nothing matches
    within ``budget_sec`` (non-blocking for automation).
    """
    deadline = time.monotonic() + max(0.5, budget_sec)
    selectors = (
        "#onetrust-accept-btn-handler",
        "button[id*='accept']",
        "button[id*='cookie']",
        "button[class*='cookie']",
        "button[id*='consent']",
        "[class*='cookie-banner'] button",
        "[class*='CookieConsent'] button",
        ".cc-btn",
        "[data-testid*='cookie']",
    )
    for sel in selectors:
        if time.monotonic() >= deadline:
            return
        try:
            loc = page.locator(sel).first
            if await loc.count() == 0:
                continue
            ms = max(300, int((deadline - time.monotonic()) * 1000))
            if ms < 400:
                return
            await loc.click(timeout=min(2000, ms))
            await asyncio.sleep(1.2)
            return
        except Exception:
            continue
    for label in (
        "Accept All",
        "Accept Cookies",
        "Accept",
        "I Agree",
        "OK",
        "Got it",
        "Allow All",
    ):
        if time.monotonic() >= deadline:
            return
        try:
            btn = page.get_by_role("button", name=re.compile(re.escape(label), re.I))
            if await btn.count() == 0:
                continue
            ms = max(300, int((deadline - time.monotonic()) * 1000))
            if ms < 400:
                return
            await btn.first.click(timeout=min(2000, ms))
            await asyncio.sleep(1.2)
            return
        except Exception:
            continue


@dataclass
class BrowserSession:
    """Holds Playwright + browser; creates fresh tabs and tracks pages we open."""

    playwright: Playwright
    browser: Browser
    context: BrowserContext
    _pages: list[Page] = field(default_factory=list)
    launched_local_chromium: bool = False

    async def new_page(self) -> Page:
        page = await self.context.new_page()
        self._pages.append(page)
        return page

    async def close_own_pages(self) -> None:
        for p in reversed(self._pages):
            try:
                await p.close()
            except Exception as e:
                logger.debug("page.close: %s", e)
        self._pages.clear()

    async def disconnect(self) -> None:
        await self.close_own_pages()
        try:
            await self.browser.close()
        except Exception as e:
            logger.debug("browser.close: %s", e)
        try:
            await self.playwright.stop()
        except Exception as e:
            logger.debug("playwright.stop: %s", e)


@asynccontextmanager
async def playwright_browser_session(
    app_cfg: dict[str, Any] | None,
) -> AsyncIterator[BrowserSession]:
    """
    One Playwright connect (or Chromium launch). Caller uses session.new_page() for each task.
    CDP: does not close the user's Chrome — only disconnects Playwright and closes tabs we opened.
    """
    pw = await async_playwright().start()
    session: BrowserSession | None = None
    try:
        cdp_url, use_fallback, _launched = await resolve_cdp_url(app_cfg)
        if not use_fallback:
            logger.info("Playwright: connecting over CDP %s", cdp_url)
            browser = await pw.chromium.connect_over_cdp(cdp_url)
            contexts = browser.contexts
            context = contexts[0] if contexts else await browser.new_context()
            session = BrowserSession(
                playwright=pw,
                browser=browser,
                context=context,
                launched_local_chromium=False,
            )
        else:
            logger.warning(_WARN_FALLBACK)
            browser = await pw.chromium.launch(headless=False)
            context = await browser.new_context()
            session = BrowserSession(
                playwright=pw,
                browser=browser,
                context=context,
                launched_local_chromium=True,
            )
        try:
            yield session
        finally:
            await session.disconnect()
    except BaseException:
        if session is None:
            try:
                await pw.stop()
            except Exception as e:
                logger.debug("playwright.stop (error path): %s", e)
        raise


@asynccontextmanager
async def playwright_page(
    app_cfg: dict[str, Any] | None,
) -> AsyncIterator[Any]:
    """Yield a single fresh Page; close that tab and disconnect Playwright on exit."""
    async with playwright_browser_session(app_cfg) as session:
        page = await session.new_page()
        try:
            yield page
        finally:
            await session.close_own_pages()
