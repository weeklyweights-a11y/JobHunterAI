"""Indeed — Tier 1 Playwright + BeautifulSoup (no LLM)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any
from urllib.parse import quote_plus

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from agents.browser_mgr import playwright_browser_session
from agents.base import EmitFn
from parsers.indeed_parser import parse_indeed_jobs_html

logger = logging.getLogger(__name__)

INDEED_PAGE_SIZE = 10
INDEED_MAX_PAGES = 10


class IndeedBrowserClosedError(RuntimeError):
    """Raised when Playwright shuts down mid-fetch (e.g. server stop / Ctrl+C)."""


def _is_browser_closed_error(err: BaseException) -> bool:
    s = str(err).lower()
    return "closed" in s and ("browser" in s or "page" in s or "context" in s or "target" in s)


def _indeed_url(query: str, location: str) -> str:
    q = quote_plus(query)
    loc = quote_plus(location)
    return f"https://www.indeed.com/jobs?q={q}&l={loc}&fromage=1"


def _indeed_url_paged(query: str, location: str, start: int) -> str:
    base = _indeed_url(query, location)
    s = max(0, int(start))
    return f"{base}&start={s}" if s else base


async def _indeed_scroll_results(page: Any) -> None:
    for _ in range(4):
        try:
            await page.evaluate(
                "() => window.scrollTo(0, document.body.scrollHeight)"
            )
        except Exception:
            pass
        await asyncio.sleep(0.6)


async def _indeed_load_serp(page: Any, url: str) -> str:
    """Navigate, wait for results, scroll — return HTML."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=25_000)
        except PlaywrightTimeoutError:
            pass
        try:
            await page.wait_for_selector(
                "div.job_seen_beacon, a[data-jk], a.jcs-JobTitle, a[href*='/viewjob'], "
                "a[href*='/rc/clk'], #mosaic-provider-jobcards",
                timeout=25_000,
            )
        except Exception:
            pass
        await asyncio.sleep(1.2)
        await _indeed_scroll_results(page)
        await asyncio.sleep(0.8)
        return await page.content()
    except PlaywrightError as e:
        if _is_browser_closed_error(e):
            raise IndeedBrowserClosedError(str(e)) from e
        raise


async def search_indeed(
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

    # One browser; fresh tab per role×location so Indeed SPA/state cannot bleed between searches.
    async with playwright_browser_session(app_cfg) as session:
        for i, (role, loc) in enumerate(combos):
            await _emit(f"Indeed (Tier 1): loading {role} in {loc}…")
            page = await session.new_page()
            try:
                merged: dict[str, dict[str, Any]] = {}
                for page_idx in range(INDEED_MAX_PAGES):
                    start = page_idx * INDEED_PAGE_SIZE
                    url = _indeed_url_paged(role, loc, start)
                    html = await _indeed_load_serp(page, url)
                    batch = parse_indeed_jobs_html(html)
                    if not batch and page_idx == 0:
                        await asyncio.sleep(2.0)
                        await _indeed_scroll_results(page)
                        html = await page.content()
                        batch = parse_indeed_jobs_html(html)
                    before = len(merged)
                    for item in batch:
                        u = str(item.get("url") or "").strip()
                        if u and u not in merged:
                            merged[u] = dict(item)
                    new_cards = len(merged) - before
                    await _emit(
                        f"Indeed: page {page_idx + 1}/{INDEED_MAX_PAGES}, +{new_cards} new (total {len(merged)})."
                    )
                    if page_idx == 0 and len(merged) == 0:
                        logger.warning(
                            "Indeed: 0 jobs parsed for first page — URL=%s (CAPTCHA or DOM change?)",
                            url,
                        )
                        await _emit("Indeed: no jobs on page 1; stopping pagination for this search.")
                        break
                    if page_idx > 0 and new_cards == 0:
                        await _emit("Indeed: no new cards; stopping early.")
                        break
                    await asyncio.sleep(random.uniform(3.0, 5.0))

                jobs = list(merged.values())
                for item in jobs:
                    item["search_role"] = role
                    item["search_location"] = loc
                collected.extend(jobs)
                logger.info("Indeed %s / %s: %s jobs (Tier 1)", role, loc, len(jobs))
                await _emit(f"Indeed: {len(jobs)} jobs parsed across pages.")
            except IndeedBrowserClosedError:
                logger.warning(
                    "Indeed: browser closed during fetch (server shutdown?). "
                    "Stopping Indeed with %s jobs collected so far.",
                    len(collected),
                )
                await _emit("Indeed: stopped (browser closed).")
                return collected
            except Exception:
                logger.exception("Indeed Tier 1 failed for %s / %s", role, loc)
            finally:
                try:
                    await page.close()
                except Exception:
                    pass
            if i < len(combos) - 1:
                await asyncio.sleep(random.uniform(3.0, 8.0))
    return collected
