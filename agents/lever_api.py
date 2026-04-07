"""Lever public postings API (no auth): ``api.lever.co/v0/postings/{company}?mode=json``."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
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


def _fetch_lever_postings_sync(company_slug: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    req = Request(url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"})
    with urlopen(req, timeout=_TIMEOUT_SEC) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def _posting_location(p: dict) -> str:
    cat = p.get("categories")
    if isinstance(cat, dict):
        loc = cat.get("location")
        if isinstance(loc, str) and loc.strip():
            return loc.strip()
    return ""


def _posting_posted_time(p: dict) -> str:
    raw = p.get("createdAt")
    if raw is None:
        return ""
    try:
        ms = float(raw)
    except (TypeError, ValueError):
        return ""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    s = dt.date().isoformat()
    return accept_normalized_posted_string(s) or ""


async def enrich_lever(
    slugs: list[str],
    *,
    roles: list[str],
    locations: list[str],
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for slug in sorted({s.strip().lower() for s in slugs if s and s.strip()}):
        try:
            postings = await asyncio.to_thread(_fetch_lever_postings_sync, slug)
        except (HTTPError, URLError, OSError, ValueError, json.JSONDecodeError):
            logger.debug("Lever API fetch failed slug=%s", slug, exc_info=True)
            continue
        for p in postings:
            title = str(p.get("text") or "").strip()
            loc = _posting_location(p)
            if not title_matches_search_roles(title, roles) or not location_matches_search_locations(
                loc, locations
            ):
                continue
            posted = _posting_posted_time(p)
            hosted = str(p.get("hostedUrl") or p.get("applyUrl") or "").strip()
            if not hosted:
                continue
            desc_raw = str(p.get("descriptionPlain") or p.get("description") or "")
            desc = (
                html_fragment_to_plain_text(desc_raw, max_len=8000) if desc_raw else ""
            )
            out.append(
                {
                    "title": title,
                    "company": slug.replace("-", " ").title(),
                    "location": loc or "Unknown",
                    "url": hosted,
                    "source": "lever",
                    "apply_type": "external",
                    "job_id": str(p.get("id") or ""),
                    "posted_time": posted,
                    "job_description": desc,
                }
            )
    return out
