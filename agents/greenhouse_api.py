"""Greenhouse public API enrichment."""

from __future__ import annotations

import asyncio
import json
import logging
from urllib.request import Request, urlopen

from parsers.ats_posted_time import (
    accept_normalized_posted_string,
    title_matches_search_roles,
)
from parsers.job_description import html_fragment_to_plain_text

logger = logging.getLogger(__name__)


def _title_match(title: str, roles: list[str]) -> bool:
    """Delegates to shared logic so API filtering matches Ashby/Greenhouse HTTP listing passes."""
    return title_matches_search_roles(title, roles)


def _loc_match(loc: str, locations: list[str]) -> bool:
    l = (loc or "").lower()
    if not l:
        return False
    for q in locations:
        qq = (q or "").lower().strip()
        if not qq:
            continue
        if qq in l:
            return True
        if qq in ("us", "usa", "united states") and "united states" in l:
            return True
    return False


def _fetch_board_jobs_sync(token: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    data = json.loads(raw)
    jobs = data.get("jobs") if isinstance(data, dict) else []
    return jobs if isinstance(jobs, list) else []


async def enrich_greenhouse(
    tokens: list[str],
    *,
    roles: list[str],
    locations: list[str],
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for tok in sorted({t.strip().lower() for t in tokens if t and t.strip()}):
        try:
            jobs = await asyncio.to_thread(_fetch_board_jobs_sync, tok)
        except Exception:
            logger.debug("Greenhouse API fetch failed for token=%s", tok, exc_info=True)
            continue
        for j in jobs:
            title = str(j.get("title") or "").strip()
            loc = str((j.get("location") or {}).get("name") or "").strip()
            if not _title_match(title, roles) or not _loc_match(loc, locations):
                continue
            updated = str(j.get("updated_at") or "").strip()
            posted = accept_normalized_posted_string(updated) or ""
            raw_content = str(j.get("content") or "")
            desc = html_fragment_to_plain_text(raw_content, max_len=8000) if raw_content else ""
            out.append(
                {
                    "title": title,
                    "company": tok.replace("-", " ").title(),
                    "location": loc or "Unknown",
                    "url": str(j.get("absolute_url") or "").strip(),
                    "source": "greenhouse",
                    "apply_type": "external",
                    "job_id": "",
                    "posted_time": posted,
                    "job_description": desc,
                }
            )
    return out
