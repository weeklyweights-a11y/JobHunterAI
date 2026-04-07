"""Ashby public job-board API: ``api.ashbyhq.com/posting-api/job-board/{slug}`` (no auth)."""

from __future__ import annotations

import asyncio
import json
import logging
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from parsers.ats_posted_time import (
    accept_normalized_posted_string,
    location_matches_search_locations,
    title_matches_search_roles,
)
from parsers.job_description import html_fragment_to_plain_text

logger = logging.getLogger(__name__)

_USER_AGENT = "Mozilla/5.0 (compatible; JobAgent/1.0)"
_TIMEOUT_SEC = 25


def _fetch_ashby_board_sync(slug: str) -> list[dict]:
    q = "includeCompensation=true"
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?{q}"
    req = Request(url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"})
    with urlopen(req, timeout=_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    if not isinstance(data, dict):
        return []
    jobs = data.get("jobs")
    return [x for x in jobs if isinstance(x, dict)] if isinstance(jobs, list) else []


def _job_location(j: dict) -> str:
    loc = j.get("location")
    if isinstance(loc, str) and loc.strip():
        return loc.strip()
    return ""


async def enrich_ashby(
    slugs: list[str],
    *,
    roles: list[str],
    locations: list[str],
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for slug in sorted({s.strip().lower() for s in slugs if s and s.strip()}):
        try:
            jobs = await asyncio.to_thread(_fetch_ashby_board_sync, slug)
        except (HTTPError, URLError, OSError, ValueError, json.JSONDecodeError):
            logger.debug("Ashby API fetch failed slug=%s", slug, exc_info=True)
            continue
        for job in jobs:
            if job.get("isListed") is False:
                continue
            title = str(job.get("title") or "").strip()
            loc = _job_location(job)
            if not title_matches_search_roles(title, roles) or not location_matches_search_locations(
                loc, locations
            ):
                continue
            pd = str(job.get("publishedAt") or "").strip()
            posted = accept_normalized_posted_string(pd) or ""
            job_url = str(job.get("jobUrl") or job.get("applyUrl") or "").strip()
            if not job_url:
                continue
            desc_raw = str(job.get("descriptionHtml") or job.get("descriptionPlain") or "")
            desc = (
                html_fragment_to_plain_text(desc_raw, max_len=8000) if desc_raw else ""
            )
            out.append(
                {
                    "title": title,
                    "company": slug.replace("-", " ").title(),
                    "location": loc or "Unknown",
                    "url": job_url,
                    "source": "ashby",
                    "apply_type": "external",
                    "job_id": str(job.get("id") or ""),
                    "posted_time": posted,
                    "job_description": desc,
                }
            )
    return out
