"""Jobright.ai — Tier 1 Playwright + BeautifulSoup: login, scroll AI feed, open each card, parse detail JSON."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from agents.base import EmitFn
from agents.browser_mgr import dismiss_cookie_popup, playwright_browser_session
from parsers.ats_posted_time import title_matches_search_roles
from parsers.jobright_parser import parse_jobright_detail_html

logger = logging.getLogger(__name__)

JOBRIGHT_HOME = "https://jobright.ai"
JOBRIGHT_FEED = "https://jobright.ai/jobs/recommend"

# CSS-module hashes change; match stable fragments from class names.
_FEED_SCROLL_SEL = '[class*="jobs-list-scrollable"]'
_DETAIL_SCRIPT = "#jobright-helper-job-detail-info"
_DETAIL_CONTAINER = '[class*="job-info-page-background-container"]'
_CLOSE_SEL = '[class*="closeButton"], [id*="job-detail-close"], [id*="job_detail_close"]'

MAX_JOBS_COLLECT = 100
SCROLL_NO_GROWTH_LIMIT = 5
SCROLL_PAUSE_SEC = 2.0
CAPTCHA_MARKERS = re.compile(
    r"captcha|recaptcha|hcaptcha|turnstile|challenge.*robot",
    re.I,
)


def _jobright_credentials(app_cfg: dict[str, Any] | None) -> tuple[str, str] | None:
    if not app_cfg:
        return None
    email = str(app_cfg.get("jobright_email") or "").strip()
    password = str(app_cfg.get("jobright_password") or "").strip()
    if not email or not password:
        return None
    return email, password


def _page_looks_like_captcha(html: str) -> bool:
    h = (html or "")[:80000]
    return bool(CAPTCHA_MARKERS.search(h))


async def _emit(emit: EmitFn | None, msg: str) -> None:
    if emit:
        await emit(msg)


async def _login_jobright(
    page: Any,
    email: str,
    password: str,
    emit: EmitFn | None,
) -> bool:
    await _emit(emit, "Jobright: opening jobright.ai…")
    try:
        await page.goto(JOBRIGHT_HOME, wait_until="domcontentloaded", timeout=90_000)
    except PlaywrightTimeoutError:
        logger.warning("Jobright: initial navigation timed out")
        return False

    await dismiss_cookie_popup(page)
    await asyncio.sleep(1.0)

    if re.search(r"/jobs/recommend", page.url or ""):
        await _emit(emit, "Jobright: already signed in (session).")
        return True

    try:
        email_box = page.locator('input[type="email"]').first
        await email_box.wait_for(state="visible", timeout=25_000)
    except PlaywrightTimeoutError:
        if _page_looks_like_captcha(await page.content()):
            await _emit(emit, "Jobright: CAPTCHA or bot challenge detected on login — cannot continue.")
        else:
            await _emit(emit, "Jobright: login form not found (check site or session).")
        return False

    if _page_looks_like_captcha(await page.content()):
        await _emit(emit, "Jobright: CAPTCHA on login page — cannot continue.")
        return False

    await email_box.fill(email)
    pw = page.locator('input[type="password"]').first
    await pw.fill(password)

    submit = page.get_by_role("button", name=re.compile(r"sign\s*in|log\s*in|continue", re.I))
    if await submit.count():
        await submit.first.click()
    else:
        await page.locator('button[type="submit"]').first.click()

    try:
        await page.wait_for_url(re.compile(r"/jobs/recommend|/jobs/"), timeout=120_000)
    except PlaywrightTimeoutError:
        html = await page.content()
        if _page_looks_like_captcha(html):
            await _emit(emit, "Jobright: login blocked by CAPTCHA or verification.")
        else:
            await _emit(emit, "Jobright: login did not reach the jobs feed in time.")
        return False

    await _emit(emit, "Jobright: signed in.")
    return True


async def _goto_feed(page: Any, emit: EmitFn | None) -> bool:
    try:
        await page.goto(JOBRIGHT_FEED, wait_until="domcontentloaded", timeout=90_000)
    except PlaywrightTimeoutError:
        await _emit(emit, "Jobright: timed out loading /jobs/recommend.")
        return False
    await dismiss_cookie_popup(page)
    return True


async def _wait_feed_populated(page: Any, emit: EmitFn | None) -> Any | None:
    feed = page.locator(_FEED_SCROLL_SEL).first
    try:
        await feed.wait_for(state="visible", timeout=60_000)
    except PlaywrightTimeoutError:
        await _emit(emit, "Jobright: feed scroll container not found (selectors may have changed).")
        return None

    try:
        await page.wait_for_function(
            """(sel) => {
                const el = document.querySelector(sel);
                if (!el) return false;
                return el.children && el.children.length > 0;
            }""",
            arg=_FEED_SCROLL_SEL,
            timeout=90_000,
        )
    except PlaywrightTimeoutError:
        await _emit(emit, "Jobright: no job cards appeared in the feed (empty or UI changed).")
        return None

    return feed


async def _scroll_feed_load_cards(feed: Any, emit: EmitFn | None) -> int:
    """Scroll until we have enough cards or several stalls. Returns final child count."""
    stall = 0
    prev = -1
    while stall < SCROLL_NO_GROWTH_LIMIT:
        try:
            n = await feed.evaluate("el => el.children.length")
        except Exception:
            n = 0
        if n >= MAX_JOBS_COLLECT:
            break
        if n <= prev:
            stall += 1
        else:
            stall = 0
        prev = n
        try:
            await feed.evaluate("el => { el.scrollTop = el.scrollHeight; }")
        except Exception:
            break
        await asyncio.sleep(SCROLL_PAUSE_SEC)

    try:
        final_n = int(await feed.evaluate("el => el.children.length"))
    except Exception:
        final_n = max(prev, 0)
    await _emit(
        emit,
        f"Jobright: feed scroll done — about {final_n} card slot(s) in list (cap {MAX_JOBS_COLLECT}).",
    )
    return final_n


async def _wait_detail_ready(page: Any) -> bool:
    try:
        loc = page.locator(f"{_DETAIL_SCRIPT}, {_DETAIL_CONTAINER}")
        await loc.first.wait_for(state="attached", timeout=30_000)
        return True
    except PlaywrightTimeoutError:
        return False


async def _try_close_detail(page: Any) -> None:
    try:
        c = page.locator(_CLOSE_SEL).first
        if await c.count():
            await c.click(timeout=5000)
            await asyncio.sleep(0.4)
    except Exception:
        pass


async def scrape_jobright_jobs(
    roles: list[str],
    locations: list[str],
    *,
    emit: EmitFn | None = None,
    app_cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Log into Jobright, load the recommend feed, scroll, open each visible card,
    parse ``#jobright-helper-job-detail-info`` JSON. ``url`` in each row is the real ATS link.
    """
    creds = _jobright_credentials(app_cfg)
    if not creds:
        await _emit(
            emit,
            "Jobright: skipped — set Jobright email and password in settings.",
        )
        return []

    email, password = creds
    search_role = next((r for r in roles if isinstance(r, str) and r.strip()), "") or ""
    search_location = next((x for x in locations if isinstance(x, str) and x.strip()), "") or ""

    out: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    async with playwright_browser_session(app_cfg) as session:
        page = await session.new_page()
        try:
            if not await _login_jobright(page, email, password, emit):
                return []
            if not await _goto_feed(page, emit):
                return []

            feed = await _wait_feed_populated(page, emit)
            if feed is None:
                return []

            await _scroll_feed_load_cards(feed, emit)

            try:
                n_cards = await feed.evaluate("el => el.children.length")
            except Exception:
                n_cards = 0

            n_cards = min(max(n_cards, 0), MAX_JOBS_COLLECT)
            await _emit(emit, f"Jobright: opening up to {n_cards} job card(s) for detail JSON…")

            for i in range(n_cards):
                try:
                    clicked = await feed.evaluate(
                        """(el, idx) => {
                            const c = el.children[idx];
                            if (!c) return false;
                            c.scrollIntoView({ block: 'center', behavior: 'instant' });
                            c.click();
                            return true;
                        }""",
                        i,
                    )
                except Exception:
                    logger.debug("Jobright: card %s click failed", i, exc_info=True)
                    await _try_close_detail(page)
                    continue
                if not clicked:
                    await _try_close_detail(page)
                    continue

                if not await _wait_detail_ready(page):
                    await _emit(emit, f"Jobright: detail did not load for card {i + 1}/{n_cards}.")
                    await _try_close_detail(page)
                    continue

                html = await page.content()
                if _page_looks_like_captcha(html):
                    await _emit(emit, "Jobright: CAPTCHA mid-session — stopping.")
                    break

                row = parse_jobright_detail_html(
                    html,
                    search_role=search_role,
                    search_location=search_location,
                )
                if not row:
                    await _try_close_detail(page)
                    continue

                title = str(row.get("title") or "")
                if not title_matches_search_roles(title, roles):
                    await _try_close_detail(page)
                    continue

                u = str(row.get("url") or "").strip()
                if u in seen_urls:
                    await _try_close_detail(page)
                    continue
                seen_urls.add(u)
                out.append(dict(row))
                await _emit(emit, f"Jobright: +1 — {title[:60]}")

                await _try_close_detail(page)
                await asyncio.sleep(0.25)

            await _emit(emit, f"Jobright: finished — {len(out)} job(s) after title filter.")
            return out
        finally:
            await session.close_own_pages()
