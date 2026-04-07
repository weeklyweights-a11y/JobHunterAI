"""Lever job detail pages: HTTP GET + JSON-LD and shared ATS extractors (before Playwright)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from parsers.ashby_http import job_patch_from_ld_json
from parsers.ats_posted_time import (
    accept_normalized_posted_string,
    extract_posted_time_from_html,
    fetch_job_page_html_cached,
)
from parsers.job_description import extract_job_description_from_html

logger = logging.getLogger(__name__)


def is_lever_job_detail_url(url: str) -> bool:
    """``jobs.lever.co/{company}/{posting}`` (at least two path segments), not the board index."""
    if "lever.co" not in urlparse((url or "").strip()).netloc.lower():
        return False
    parts = [x for x in urlparse(url).path.split("/") if x]
    return len(parts) >= 2


async def enrich_lever_jobs_via_http(
    jobs: list[dict[str, Any]],
    *,
    emit: Any | None = None,
    html_cache: dict[str, str] | None = None,
) -> int:
    """
    HTTP GET each Lever **job** URL and merge JSON-LD + ``extract_posted_time_from_html`` /
    ``extract_job_description_from_html``. Skips single-segment board index URLs.
    """
    async def _one(j: dict[str, Any]) -> int:
        url = str(j.get("url") or "").strip()
        if not url or "lever.co" not in urlparse(url).netloc.lower():
            return 0
        if not is_lever_job_detail_url(url):
            return 0
        if str(j.get("posted_time") or "").strip():
            return 0
        need_desc = not str(j.get("job_description") or "").strip()
        try:
            html = await asyncio.to_thread(fetch_job_page_html_cached, url, html_cache)
        except (HTTPError, URLError, OSError) as e:
            logger.debug("Lever HTTP fetch failed url=%s err=%s", url[:120], e)
            return 0
        except Exception:
            logger.debug("Lever HTTP fetch failed url=%s", url[:120], exc_info=True)
            return 0

        had_desc = bool(str(j.get("job_description") or "").strip())

        patch = job_patch_from_ld_json(html)
        for k, v in patch.items():
            if v is None or (isinstance(v, str) and not v.strip()):
                continue
            if not str(j.get(k) or "").strip():
                j[k] = v

        if not str(j.get("posted_time") or "").strip():
            dt = extract_posted_time_from_html(html, url)
            if dt and accept_normalized_posted_string(dt):
                j["posted_time"] = dt

        if need_desc and not str(j.get("job_description") or "").strip():
            desc = extract_job_description_from_html(html, url)
            if desc:
                j["job_description"] = desc

        if str(j.get("posted_time") or "").strip() or (
            not had_desc and str(j.get("job_description") or "").strip()
        ):
            return 1
        return 0

    eligible = [
        j
        for j in jobs
        if str(j.get("url") or "").strip()
        and "lever.co" in urlparse(str(j.get("url") or "")).netloc.lower()
        and is_lever_job_detail_url(str(j.get("url") or ""))
        and not str(j.get("posted_time") or "").strip()
    ]
    n_ok = 0
    batch = 5
    for i in range(0, len(eligible), batch):
        chunk = eligible[i : i + batch]
        got = await asyncio.gather(*(_one(j) for j in chunk))
        n_ok += sum(got)
        if i + batch < len(eligible):
            await asyncio.sleep(random.uniform(0.5, 1.0))

    if n_ok and emit is not None:
        await emit(
            f"ATS: enriched {n_ok} Lever job page(s) via HTTP (JSON-LD + datePosted / embedded extractors)."
        )
    return n_ok
